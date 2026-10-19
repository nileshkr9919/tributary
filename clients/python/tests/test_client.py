import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
import unittest
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from tributary import Client, Permanent, Throttled, Worker  # noqa: E402

FAST = {"lease": {"initial": "1s"}, "failure": {"backoff_min": "100ms", "backoff_max": "200ms", "jitter": False}}


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.addr = f"127.0.0.1:{free_port()}"
        cls.proc = subprocess.Popen([os.path.join(ROOT, "bin", "trib"), "dev", "--listen", cls.addr],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cls.url = "http://" + cls.addr
        for _ in range(100):
            try:
                urllib.request.urlopen(cls.url + "/healthz").read()
                break
            except OSError:
                time.sleep(0.05)
        cls.c = Client(cls.url)

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait()

    def topic(self, name, profile="work-queue", overrides=None):
        self.c.create_topic(name, shards=1)
        self.c.create_subscription(name + "-sub", name, profile=profile, overrides=overrides)
        return name + "-sub"

    def run_until(self, worker, cond, timeout=10):
        t = worker.run_in_thread()
        deadline = time.time() + timeout
        while time.time() < deadline and not cond():
            time.sleep(0.05)
        worker.stop()
        t.join(5)
        self.assertTrue(cond(), "condition not reached")

    def test_publish_and_work(self):
        sub = self.topic("basic")
        ids = [self.c.publish("basic", {"n": i}) for i in range(3)]
        self.assertEqual(len(set(ids)), 3)
        seen = []
        self.run_until(Worker(self.c, sub, lambda job: seen.append(job.payload["n"]), concurrency=2),
                       lambda: len(seen) == 3)
        self.assertEqual(sorted(seen), [0, 1, 2])
        self.assertEqual(self.c.stats(sub)["counts"]["pending"], 0)

    def test_throttled_does_not_count_attempt(self):
        sub = self.topic("throttle", overrides=FAST)
        self.c.publish("throttle", "x")
        attempts = []

        def handler(job):
            attempts.append(job.attempt)
            if len(attempts) <= 3:
                raise Throttled(retry_after=0.1)

        self.run_until(Worker(self.c, sub, handler), lambda: len(attempts) == 4)
        self.assertEqual(attempts, [1, 1, 1, 1])

    def test_permanent_dead_letters(self):
        sub = self.topic("perm")
        self.c.publish("perm", "x")
        calls = []

        def handler(job):
            calls.append(1)
            raise Permanent("bad input")

        self.run_until(Worker(self.c, sub, handler), lambda: len(self.c.dead_letters(sub)) == 1)
        self.assertEqual(self.c.dead_letters(sub)[0]["reason"], "bad input")

    def test_checkpoint_resume(self):
        sub = self.topic("resume", overrides=FAST)
        self.c.publish("resume", "job")
        child = textwrap.dedent(f"""
            import os, sys
            sys.path.insert(0, {os.path.join(ROOT, "clients", "python")!r})
            from tributary import Client, Worker
            def handler(job):
                job.save_progress({{"step": 1}})
                os._exit(1)  # crash hard after step 1
            Worker(Client({self.url!r}), {sub!r}, handler, concurrency=1).run()
        """)
        subprocess.run([sys.executable, "-c", child], timeout=20)
        seen = []
        self.run_until(Worker(self.c, sub, lambda job: seen.append((job.attempt, job.checkpoint))),
                       lambda: len(seen) == 1, timeout=15)
        self.assertEqual(seen[0], (2, {"step": 1}))

    def test_supersede_sets_cancelled(self):
        self.c.create_topic("chat", shards=1)
        self.c.create_subscription("chat-sub", "chat", profile="agent-session")
        self.c.publish("chat", "first", key="user-1")
        started = threading.Event()
        outcome = {}

        def handler(job):
            if job.payload == "first":
                started.set()
                deadline = time.time() + 8
                while not job.cancelled and time.time() < deadline:
                    time.sleep(0.05)
                outcome["first_cancelled"] = job.cancelled
            else:
                outcome["second"] = True

        w = Worker(self.c, "chat-sub", handler, concurrency=2)
        t = w.run_in_thread()
        self.assertTrue(started.wait(5))
        self.c.publish("chat", "second", key="user-1")
        deadline = time.time() + 10
        while time.time() < deadline and "second" not in outcome:
            time.sleep(0.05)
        w.stop()
        t.join(5)
        self.assertTrue(outcome.get("first_cancelled"))
        self.assertTrue(outcome.get("second"))

    def test_effect_key_stable_across_attempts(self):
        sub = self.topic("effects", overrides=FAST)
        self.c.publish("effects", "x")
        keys = []

        def handler(job):
            keys.append(job.effect_key("llm-call"))
            if len(keys) == 1:
                raise RuntimeError("transient")

        self.run_until(Worker(self.c, sub, handler), lambda: len(keys) == 2)
        self.assertEqual(keys[0], keys[1])
        self.assertEqual(len(keys[0]), 32)

    def test_usage_reported_on_ack(self):
        self.c.create_topic("llm", shards=1)
        self.c.create_pool("prov", unit="tokens", per_minute=600000, burst=10000)
        self.c.create_subscription("llm-sub", "llm", profile="llm-requests", params={"pool": "prov"})
        self.c.publish("llm", "hi", headers={"tenant": "acme", "x-est-tokens": "100"}, priority="interactive")
        done = []

        def handler(job):
            job.report_usage(tokens=250)
            done.append(1)

        self.run_until(Worker(self.c, "llm-sub", handler), lambda: done)
        time.sleep(0.2)
        metrics = urllib.request.urlopen(self.url + "/metrics").read().decode()
        self.assertIn('trib_usage_total{subscription="llm-sub",field="tokens",label="acme"} 250', metrics)


if __name__ == "__main__":
    unittest.main()
