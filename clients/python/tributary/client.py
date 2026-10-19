"""HTTP client for a Tributary server. Standard library only."""

import json
import urllib.error
import urllib.request

from .errors import TributaryError


class Client:
    def __init__(self, url="http://127.0.0.1:7070", timeout=40):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _req(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                msg = json.loads(raw).get("error", raw.decode())
            except ValueError:
                msg = raw.decode(errors="replace")
            raise TributaryError(e.code, msg) from None
        return json.loads(raw) if raw else {}

    # --- admin -------------------------------------------------------------
    def create_topic(self, name, shards=4, exist_ok=False):
        try:
            return self._req("POST", "/v1/topics", {"name": name, "shards": shards})
        except TributaryError as e:
            if exist_ok and e.status == 409:
                return {"name": name}
            raise

    def create_pool(self, name, per_minute, burst=None, unit="tokens", mode=None, headroom=None, pace=None,
                    exist_ok=False):
        """mode "window" counts usage over a rolling 60 s, like provider per-minute limits.
        headroom is the fraction of capacity only the top priority band may use.
        pace (window mode, e.g. "10s") spreads lower bands evenly across the minute."""
        body = {"name": name, "unit": unit, "per_minute": per_minute}
        if burst is not None:
            body["burst"] = burst
        if mode:
            body["mode"] = mode
        if headroom is not None:
            body["headroom"] = headroom
        if pace:
            body["pace"] = pace
        try:
            return self._req("POST", "/v1/pools", body)
        except TributaryError as e:
            if exist_ok and e.status == 409:
                return {"name": name}
            raise

    def create_subscription(self, name, topic, profile="work-queue", params=None, overrides=None, exist_ok=False):
        body = {"name": name, "topic": topic, "profile": profile}
        if params:
            body["params"] = params
        if overrides:
            body["overrides"] = overrides
        try:
            return self._req("POST", "/v1/subscriptions", body)
        except TributaryError as e:
            if exist_ok and e.status == 409:
                return self.stats(name)
            raise

    def stats(self, subscription):
        return self._req("GET", f"/v1/subscriptions/{subscription}")

    def pools(self):
        return self._req("GET", "/v1/pools")["pools"]

    def resume_pool(self, name):
        return self._req("POST", f"/v1/pools/{name}/resume", {})

    def cancel(self, subscription, id=None, key=None):
        body = {"id": id} if id else {"key": key}
        return self._req("POST", f"/v1/subscriptions/{subscription}/cancel", body)["cancelled"]

    def pause(self, subscription, value=None):
        return self._req("POST", f"/v1/subscriptions/{subscription}/pause", {"fairness_value": value or ""})

    def resume(self, subscription, value=None):
        return self._req("POST", f"/v1/subscriptions/{subscription}/resume", {"fairness_value": value or ""})

    def dead_letters(self, subscription):
        return self._req("GET", f"/v1/subscriptions/{subscription}/dlq")["dead_letters"]

    def requeue(self, subscription, ids):
        return self._req("POST", f"/v1/subscriptions/{subscription}/dlq/requeue", {"ids": list(ids)})["requeued"]

    # --- produce -----------------------------------------------------------
    @staticmethod
    def _message(payload, key=None, headers=None, priority=None, idempotency_key=None, deliver_at=None):
        m = {"payload": payload}
        if key is not None:
            m["key"] = str(key)
        if headers:
            m["headers"] = {k: str(v) for k, v in headers.items()}
        if priority:
            m["priority"] = priority
        if idempotency_key:
            m["idempotency_key"] = idempotency_key
        if deliver_at is not None:
            m["deliver_at_ms"] = int(deliver_at * 1000)  # unix seconds
        return m

    def publish(self, topic, payload, key=None, headers=None, priority=None, idempotency_key=None, deliver_at=None):
        msg = self._message(payload, key, headers, priority, idempotency_key, deliver_at)
        return self._req("POST", f"/v1/topics/{topic}/messages", {"messages": [msg]})["ids"][0]

    def publish_many(self, topic, items):
        """items: dicts with payload and optional key, headers, priority, idempotency_key, deliver_at."""
        msgs = [self._message(**it) for it in items]
        return self._req("POST", f"/v1/topics/{topic}/messages", {"messages": msgs})["ids"]

    # --- consume -----------------------------------------------------------
    def receive(self, subscription, member, max=1, wait_ms=0):
        body = {"member": member, "max": max, "wait_ms": wait_ms}
        return self._req("POST", f"/v1/subscriptions/{subscription}/receive", body)["deliveries"]

    def ack(self, token, usage=None):
        a = {"token": token}
        if usage:
            a["usage"] = usage
        return self._req("POST", "/v1/ack", {"acks": [a]})["results"][0]["status"]

    def nack(self, token, reason="retry", retry_after=None, scope=None, checkpoint=None, error=None, usage=None):
        body = {"token": token, "reason": reason}
        if retry_after is not None:
            body["retry_after_ms"] = int(retry_after * 1000)
        if scope:
            body["scope"] = scope
        if checkpoint is not None:
            body["checkpoint"] = checkpoint
        if error:
            body["error"] = error[:2000]
        if usage:
            body["usage"] = usage
        return self._req("POST", "/v1/nack", body)["status"]

    def extend(self, token, duration=None, checkpoint=None):
        body = {"token": token}
        if duration is not None:
            body["duration_ms"] = int(duration * 1000)
        if checkpoint is not None:
            body["checkpoint"] = checkpoint
        return self._req("POST", "/v1/extend", body)["status"]
