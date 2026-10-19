"""The baseline: a typical Celery setup on Redis.

One queue; acks_late so crashed tasks are redelivered; autoretry on 429 and on crashes with
exponential back-off (Celery's retry_backoff), up to 10 retries. A retry re-runs the task from
the start, which is how a plain task queue behaves.
"""

import time

import redis
from celery import Celery

from workload import call_llm

app = Celery("demo", broker="redis://127.0.0.1:6379/0")
app.conf.update(task_acks_late=True, task_acks_on_failure_or_timeout=False, task_reject_on_worker_lost=True,
                worker_prefetch_multiplier=1, broker_connection_retry_on_startup=True)
r = redis.Redis(host="127.0.0.1", port=6379, db=1)


class RateLimited(Exception):
    pass


class Crash(Exception):
    pass


@app.task(bind=True, autoretry_for=(RateLimited, Crash), retry_backoff=True, retry_backoff_max=60,
          retry_jitter=True, max_retries=10)
def run_job(self, job):
    for step in range(job["steps"]):
        status, _ = call_llm(job["id"], step, job["tenant"], job["tokens"])
        if status == 429:
            raise RateLimited()
        if step == 1 and job["crash"] and r.setnx(f"crashed:{job['id']}", 1):
            raise Crash()
    r.hset("done", job["id"], time.time())
