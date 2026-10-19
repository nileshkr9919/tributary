"""Runs the workload on the Celery baseline and writes out/celery.json."""

import json
import os
import subprocess
import sys
import time

import redis

from workload import CONCURRENCY, jobs, mock_stats, reset_mock

HERE = os.path.dirname(os.path.abspath(__file__))
TIMEOUT = float(os.environ.get("DEMO_TIMEOUT", "900"))


def main():
    r = redis.Redis(host="127.0.0.1", port=6379, db=1)
    r.flushall()
    reset_mock()
    worker = subprocess.Popen([sys.executable, "-m", "celery", "-A", "celery_app", "worker", "--pool", "threads",
                               "--concurrency", str(CONCURRENCY), "--loglevel", "WARNING"], cwd=HERE)
    time.sleep(4)
    from celery_app import run_job

    bulk, inter = jobs()
    start = time.time()
    enq = {}
    for j in bulk:
        run_job.delay(j)
        enq[j["id"]] = time.time()
    for j in inter:
        delay = start + j["at"] - time.time()
        if delay > 0:
            time.sleep(delay)
        run_job.delay(j)
        enq[j["id"]] = time.time()
    total = len(bulk) + len(inter)
    while time.time() - start < TIMEOUT and r.hlen("done") < total:
        time.sleep(0.5)
    end = time.time()
    done = {k.decode(): float(v) for k, v in r.hgetall("done").items()}
    worker.terminate()
    worker.wait(30)
    out = {"system": "Celery 5 + Redis", "start": start, "end": end, "enqueued": enq, "done": done,
           "jobs": bulk + inter, "calls": mock_stats()["calls"]}
    os.makedirs(os.path.join(HERE, "out"), exist_ok=True)
    with open(os.path.join(HERE, "out", "celery.json"), "w") as f:
        json.dump(out, f)
    print(f"celery: {len(done)}/{total} jobs in {end - start:.1f}s")


if __name__ == "__main__":
    main()
