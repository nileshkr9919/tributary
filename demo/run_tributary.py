"""Runs the workload on Tributary and writes out/tributary.json.

The llm-requests profile with a token pool sized to the provider's limit; interactive
requests carry priority=interactive; fairness by tenant; a 429 raises Throttled with the
provider's Retry-After; a checkpoint after each step lets a retry resume instead of restart.
"""

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "clients", "python"))

from tributary import Client, Throttled, Worker  # noqa: E402
from workload import CONCURRENCY, TPM, call_llm, jobs, mock_stats, reset_mock  # noqa: E402

TIMEOUT = float(os.environ.get("DEMO_TIMEOUT", "900"))


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def main():
    reset_mock()
    addr = f"127.0.0.1:{free_port()}"
    trib = subprocess.Popen([os.path.join(HERE, "..", "bin", "trib"), "dev", "--listen", addr],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = "http://" + addr
    for _ in range(100):
        try:
            urllib.request.urlopen(url + "/healthz").read()
            break
        except OSError:
            time.sleep(0.05)
    c = Client(url)
    c.create_topic("llm", shards=4)
    # The pool mirrors the provider's rule (a rolling 60 s token limit) and keeps 20% of it
# for the top priority band, and paces batch work evenly across the minute (10 s slices), so
# batch jobs cannot spend the budget interactive users need.
    c.create_pool("provider", per_minute=TPM, mode=os.environ.get("POOL_MODE", "window"),
                  headroom=float(os.environ.get("POOL_HEADROOM", "0.2")),
                  pace=os.environ.get("POOL_PACE", "10s") or None,
                  burst=float(os.environ["POOL_BURST"]) if os.environ.get("POOL_BURST") else None)
    c.create_subscription("agents", "llm", profile="llm-requests", params={"pool": "provider"})

    done, crashed, lock = {}, set(), threading.Lock()

    def handler(job):
        p = job.payload
        start = (job.checkpoint or {}).get("step", 0)
        for step in range(start, p["steps"]):
            status, retry_after = call_llm(p["id"], step, p["tenant"], p["tokens"])
            if status == 429:
                raise Throttled(retry_after=retry_after, scope="subscription")
            job.report_usage(tokens=p["tokens"])
            job.save_progress({"step": step + 1})
            if step == 1 and p["crash"]:
                with lock:
                    first = p["id"] not in crashed
                    crashed.add(p["id"])
                if first:
                    raise RuntimeError("worker crashed after step 2")
        with lock:
            done[p["id"]] = time.time()

    worker = Worker(c, "agents", handler, concurrency=CONCURRENCY)
    wt = worker.run_in_thread()

    def msg(j):
        prio = "interactive" if j["kind"] == "interactive" else "background"
        return {"payload": j, "key": j["id"], "priority": prio,
                "headers": {"tenant": j["tenant"], "x-est-tokens": str(j["steps"] * j["tokens"])}}

    bulk, inter = jobs()
    start = time.time()
    enq = {}
    for i in range(0, len(bulk), 100):
        chunk = bulk[i:i + 100]
        c.publish_many("llm", [msg(j) for j in chunk])
        for j in chunk:
            enq[j["id"]] = time.time()
    for j in inter:
        delay = start + j["at"] - time.time()
        if delay > 0:
            time.sleep(delay)
        c.publish_many("llm", [msg(j)])
        enq[j["id"]] = time.time()
    total = len(bulk) + len(inter)
    while time.time() - start < TIMEOUT:
        with lock:
            if len(done) >= total:
                break
        time.sleep(0.5)
    end = time.time()
    worker.stop()
    wt.join(10)
    trib.terminate()
    trib.wait(10)
    out = {"system": "Tributary v0", "start": start, "end": end, "enqueued": enq, "done": dict(done),
           "jobs": bulk + inter, "calls": mock_stats()["calls"]}
    os.makedirs(os.path.join(HERE, "out"), exist_ok=True)
    with open(os.path.join(HERE, "out", "tributary.json"), "w") as f:
        json.dump(out, f)
    print(f"tributary: {len(done)}/{total} jobs in {end - start:.1f}s")


if __name__ == "__main__":
    main()
