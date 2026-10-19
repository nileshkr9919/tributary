"""A worker that pulls from a subscription and runs a handler per job."""

import hashlib
import logging
import os
import socket
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from .errors import Permanent, QuotaExhausted, Throttled, TributaryError

log = logging.getLogger("tributary")

_ENDED = ("superseded", "cancelled", "lost")


class Job:
    """One delivery. Call save_progress after each expensive step so a retry resumes."""

    def __init__(self, client, delivery, lease_s):
        self._client = client
        self._lease_s = lease_s
        self.id = delivery["id"]
        self.token = delivery["token"]
        self.key = delivery.get("key")
        self.payload = delivery.get("payload")
        self.headers = delivery.get("headers") or {}
        self.attempt = delivery.get("attempt", 1)
        self.operation_id = delivery.get("operation_id") or self.id
        self.priority = delivery.get("priority")
        self.checkpoint = delivery.get("checkpoint")
        self.previous_holder = delivery.get("previous_holder")
        self.usage = {}
        self.cancelled = False  # set when a newer message superseded this one, or it was cancelled
        self.lost = False       # set when the lease was lost; stop and do not commit side effects

    def effect_key(self, step):
        """A stable idempotency key for an external call: the same across every attempt."""
        return hashlib.sha256(f"{self.operation_id}/{step}".encode()).hexdigest()[:32]

    def save_progress(self, checkpoint):
        """Store a progress checkpoint (JSON-serialisable, up to 64 KiB) with the lease."""
        status = self._client.extend(self.token, duration=self._lease_s, checkpoint=checkpoint)
        self.checkpoint = checkpoint
        self._observe(status)
        return status

    def report_usage(self, **numbers):
        """Report usage (for example tokens=..., cost=...) to settle quota pools on ack."""
        for k, v in numbers.items():
            self.usage[k] = self.usage.get(k, 0) + v

    def _observe(self, status):
        if status in ("superseded", "cancelled"):
            self.cancelled = True
        elif status == "lost":
            self.lost = True
            self.cancelled = True


class Worker:
    """Pulls jobs from a subscription and runs handler(job) with bounded concurrency.

    Returning normally acks the job (with job.usage). Raising Throttled, QuotaExhausted or
    Permanent sends that nack; any other exception retries with back-off.
    """

    def __init__(self, client, subscription, handler, concurrency=10, member=None):
        self.client = client
        self.subscription = subscription
        self.handler = handler
        self.concurrency = concurrency
        self.member = member or f"{socket.gethostname()}-{os.getpid()}-{id(self) & 0xffff:x}"
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._in_flight = 0

    def stop(self):
        self._stop.set()

    def run_in_thread(self):
        t = threading.Thread(target=self.run, daemon=True)
        t.start()
        return t

    def run(self, stop_event=None):
        if stop_event is not None:
            self._stop = stop_event
        lease_ns = self.client.stats(self.subscription)["settings"]["lease"]["initial"]
        lease_s = lease_ns / 1e9
        interval = lease_s / 3
        if lease_s >= 300:
            interval = min(interval, 1.0)
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            while not self._stop.is_set():
                with self._lock:
                    free = self.concurrency - self._in_flight
                if free <= 0:
                    time.sleep(0.01)
                    continue
                try:
                    deliveries = self.client.receive(self.subscription, self.member, max=free, wait_ms=1000)
                except (TributaryError, OSError) as e:
                    log.warning("receive failed: %s", e)
                    time.sleep(0.5)
                    continue
                for d in deliveries:
                    with self._lock:
                        self._in_flight += 1
                    pool.submit(self._process, d, lease_s, interval)

    def _process(self, delivery, lease_s, interval):
        job = Job(self.client, delivery, lease_s)
        done = threading.Event()

        def keep_alive():
            while not done.wait(interval):
                try:
                    job._observe(self.client.extend(job.token, duration=lease_s))
                except (TributaryError, OSError) as e:
                    log.debug("extend failed: %s", e)
                if job.lost:
                    return

        ka = threading.Thread(target=keep_alive, daemon=True)
        ka.start()
        try:
            self.handler(job)
        except Throttled as e:
            self._safe(self.client.nack, job.token, reason="throttled", retry_after=e.retry_after, scope=e.scope,
                       usage=job.usage or None)
        except QuotaExhausted as e:
            self._safe(self.client.nack, job.token, reason="quota_exhausted", retry_after=e.retry_after,
                       scope=e.scope, usage=job.usage or None)
        except Permanent as e:
            self._safe(self.client.nack, job.token, reason="permanent", error=e.reason, usage=job.usage or None)
        except Exception as e:  # noqa: BLE001 - any handler failure is a retry
            log.debug("handler failed: %s", traceback.format_exc())
            self._safe(self.client.nack, job.token, reason="retry", error=f"{type(e).__name__}: {e}",
                       usage=job.usage or None)
        else:
            self._safe(self.client.ack, job.token, usage=job.usage or None)
        finally:
            done.set()
            with self._lock:
                self._in_flight -= 1

    @staticmethod
    def _safe(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except (TributaryError, OSError) as e:
            log.warning("%s failed: %s", fn.__name__, e)
