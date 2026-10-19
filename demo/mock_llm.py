"""A mock LLM API with provider-style limits.

POST /v1/complete  headers X-Job, X-Step, X-Tenant; body {"tokens": N}
  - enforces tokens-per-minute and requests-per-minute over a sliding 60 s window
  - over the limit: 429 with Retry-After (seconds) and retry-after-ms
  - otherwise sleeps 0.2 + tokens/4000 seconds, bills the tokens, returns 200
GET /stats   every call: time, job, step, tenant, tokens, status, latency
POST /reset  clears state
"""

import argparse
import json
import math
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
window = []  # (time, tokens) of accepted calls in the last 60 s
calls = []
cfg = {"tpm": 120000, "rpm": 600}


def admit(tokens, now):
    """Returns 0 if admitted, else seconds until enough capacity frees."""
    global window
    window = [(t, n) for t, n in window if t > now - 60]
    used = sum(n for _, n in window)
    if used + tokens <= cfg["tpm"] and len(window) + 1 <= cfg["rpm"]:
        window.append((now, tokens))
        return 0
    need_tokens = used + tokens - cfg["tpm"]
    need_reqs = len(window) + 1 - cfg["rpm"]
    freed_t, freed_r = 0, 0
    for t, n in window:  # oldest first
        freed_t += n
        freed_r += 1
        if freed_t >= need_tokens and freed_r >= need_reqs:
            return max(0.05, t + 60 - now)
    return 60.0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, code, body, headers=None):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/stats":
            with lock:
                self._json(200, {"calls": list(calls), "config": dict(cfg)})
        else:
            self._json(404, {})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/reset":
            with lock:
                window.clear()
                calls.clear()
                cfg.update({k: body[k] for k in ("tpm", "rpm") if k in body})
            return self._json(200, {"ok": True})
        if self.path != "/v1/complete":
            return self._json(404, {})
        tokens = int(body.get("tokens", 100))
        rec = {"t": time.time(), "job": self.headers.get("X-Job"), "step": int(self.headers.get("X-Step") or 0),
               "tenant": self.headers.get("X-Tenant"), "tokens": tokens}
        with lock:
            wait = admit(tokens, rec["t"])
        if wait:
            rec.update(status=429, latency=0)
            with lock:
                calls.append(rec)
            return self._json(429, {"error": "rate_limited"},
                              {"Retry-After": str(math.ceil(wait)), "retry-after-ms": str(int(wait * 1000))})
        latency = 0.2 + tokens / 4000
        time.sleep(latency)
        rec.update(status=200, latency=latency)
        with lock:
            calls.append(rec)
        self._json(200, {"text": "ok", "usage": {"tokens": tokens}})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--tpm", type=int, default=120000)
    ap.add_argument("--rpm", type=int, default=600)
    a = ap.parse_args()
    cfg.update(tpm=a.tpm, rpm=a.rpm)
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
