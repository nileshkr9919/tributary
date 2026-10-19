"""The shared workload and the one function both runners use to call the model."""

import json
import os
import random
import urllib.error
import urllib.request

SCALE = float(os.environ.get("DEMO_SCALE", "1"))
MOCK = os.environ.get("MOCK_URL", "http://127.0.0.1:9100")
CONCURRENCY = 20
TPM = 120000
RPM = 600


def jobs():
    """200 background agent jobs (3 steps x 500 tokens, 10% crash once after step 2) and
    120 interactive requests (1 step x 300 tokens, one every 0.5 s starting at 5 s)."""
    rng = random.Random(42)
    n_bulk = max(1, int(200 * SCALE))
    n_inter = max(1, int(120 * SCALE))
    crashers = set(rng.sample(range(n_bulk), max(1, n_bulk // 10)))
    bulk = [{"id": f"bulk-{i}", "tenant": "bulk", "steps": 3, "tokens": 500, "crash": i in crashers,
             "at": 0.0, "kind": "bulk"} for i in range(n_bulk)]
    inter = [{"id": f"user-{i}", "tenant": "users", "steps": 1, "tokens": 300, "crash": False,
              "at": 5.0 + i * 0.5, "kind": "interactive"} for i in range(n_inter)]
    return bulk, inter


def call_llm(job_id, step, tenant, tokens):
    """Returns (status, retry_after_seconds)."""
    req = urllib.request.Request(MOCK + "/v1/complete", data=json.dumps({"tokens": tokens}).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "X-Job": job_id, "X-Step": str(step),
                                          "X-Tenant": tenant})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            r.read()
            return 200, 0
    except urllib.error.HTTPError as e:
        if e.code == 429:
            ms = e.headers.get("retry-after-ms")
            return 429, (int(ms) / 1000 if ms else float(e.headers.get("Retry-After", "1")))
        raise


def reset_mock():
    req = urllib.request.Request(MOCK + "/reset", data=json.dumps({"tpm": TPM, "rpm": RPM}).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req).read()


def mock_stats():
    with urllib.request.urlopen(MOCK + "/stats") as r:
        return json.loads(r.read())
