#!/usr/bin/env bash
# Reruns the whole comparison: Celery + Redis vs Tributary, same workload, same mock provider.
# DEMO_SCALE=0.1 runs a tenth of the workload.
set -euo pipefail
cd "$(dirname "$0")"
GO=${GO:-$(command -v go || echo /opt/homebrew/bin/go)}
(cd .. && "$GO" build -o bin/trib ./cmd/trib)
if [ ! -d .venv ]; then python3 -m venv .venv; fi
.venv/bin/pip install -q -r requirements.txt
mkdir -p out
docker compose up -d redis
.venv/bin/python mock_llm.py --port 9100 &
MOCK_PID=$!
trap 'kill $MOCK_PID 2>/dev/null; docker compose down >/dev/null 2>&1' EXIT
sleep 1
if [ "${ONLY:-}" != "tributary" ]; then
  echo "== Celery baseline"
  .venv/bin/python run_celery.py
fi
echo "== Tributary"
.venv/bin/python run_tributary.py
echo "== Report"
.venv/bin/python report.py
