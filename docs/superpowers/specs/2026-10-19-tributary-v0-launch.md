# Tributary v0: the job queue that understands LLMs

Launch spec, 2026-10-19. Approved by the owner in conversation ("yes, go ahead and build it").
v0 is a single-node, demo-first release of the AI work queue described in sections 7 and 8 of
[the platform design](2026-10-19-unified-messaging-platform-design.md). Where this document is
silent, the platform design applies; where they differ, this document wins for v0.

## Goal

Go viral with one sharp, reproducible claim: **agent and LLM jobs on Tributary stop melting under
rate limits, stop paying twice for retries, and stop starving interactive users**, shown by a
side-by-side demo against Celery on Redis that anyone can rerun.

## What v0 is

- One Go binary, `trib`, running one node. `trib server` and `trib dev`.
- Durable: every acknowledged produce and every ack, nack, extend and lease grant is fsynced before
  the response. v0's on-disk format is **not stable**; the README says so.
- An HTTP/JSON API and a Python client. No gRPC, no Go client in v0.
- Message subscriptions only (no cursor subscriptions), `pull` delivery, `shared` distribution,
  at-least-once.

## Features in v0

| Feature | Design section | v0 detail |
| --- | --- | --- |
| Topics with fixed shards | 5.2 | default 4 shards; key hash routing; keyless round-robin |
| Idempotency keys | 6.2 | window 15 minutes |
| Leases, tokens, ack, nack, extend | 7.7 | every lease grant committed before sending |
| Back-off, attempts, crash-loop quarantine | 7.3 | defaults from design 7.2 |
| Dead letters, list, requeue | 7.10 | dead-letter topic `<topic>.dlq.<subscription>` |
| Per-key ordering, supersede, cancel, pause | 7.9, 8.5 | pause by subscription or by fairness value |
| Priority bands, ageing, overdue lane | 8.2 | bands from the profile |
| Fairness by header | 8.2 | round-robin, per-value in-flight cap |
| Quota pools | 8.3 | single node, no coordinator; estimate times learned ratio; settle on ack; debt |
| Provider-aware nacks | 8.4 | `retry`, `throttled`, `quota_exhausted`, `permanent` |
| Progress checkpoints, operation ID, effect keys | 8.6 | checkpoint up to 64 KiB |
| Cost caps and usage metrics | 8.7 | `failure.max_cost`; Prometheus text at `/metrics` |
| Built-in profiles | 7.4 | `work-queue@1`, `llm-requests@1`, `agent-session@1` with inline overrides; no custom profiles |
| Health | 12.1 | `/healthz` |

## Not in v0

Replication, cursor subscriptions, push, webhooks, exactly-once, custom profiles and pinning,
schemas, auth (v0 listens on `127.0.0.1` unless `--listen` is given), and everything in the
platform design's annex. v0 keeps per-subscription dispatch structures in memory and rebuilds them
from disk at start; the platform design's memory bounds do not apply to v0.

## HTTP API

All bodies are JSON. Payloads are any JSON value.

| Method and path | Body | Response |
| --- | --- | --- |
| `POST /v1/topics` | `name`, `shards` | topic |
| `POST /v1/topics/{topic}/messages` | `messages`: list of `key`, `headers`, `payload`, `priority`, `deliver_at_ms`, `idempotency_key` | `ids` |
| `POST /v1/pools` | `name`, `unit`, `per_minute`, `burst` | pool |
| `POST /v1/subscriptions` | `name`, `topic`, `profile`, `params`, `overrides` | effective settings |
| `GET /v1/subscriptions/{sub}` | | effective settings and counts by state |
| `POST /v1/subscriptions/{sub}/receive` | `member`, `max`, `wait_ms` | `deliveries` |
| `POST /v1/ack` | `acks`: list of `token`, `usage` | per-token status |
| `POST /v1/nack` | `token`, `reason`, `retry_after_ms`, `scope`, `checkpoint`, `error` | status |
| `POST /v1/extend` | `token`, `duration_ms`, `checkpoint` | status: `ok`, `superseded`, `cancelled`, `paused`, `lost` |
| `POST /v1/subscriptions/{sub}/cancel` | `id` or `key` | count |
| `POST /v1/subscriptions/{sub}/pause` and `/resume` | optional `fairness_value` | ok |
| `GET /v1/subscriptions/{sub}/dlq` | | dead letters |
| `POST /v1/subscriptions/{sub}/dlq/requeue` | `ids` | count |
| `POST /v1/subscriptions/{sub}/release` | `ids` of held messages | count |
| `GET /metrics`, `GET /healthz` | | |

A delivery carries `id`, `token`, `key`, `headers`, `payload`, `attempt`, `operation_id`,
`priority`, `checkpoint` and `previous_holder`.

## The demo

- **Mock LLM API** (Python): enforces a tokens-per-minute and requests-per-minute limit, returns
  429 with `Retry-After`, bills every accepted call, and records each call's job ID, step and
  latency.
- **Workload**: 200 background agent jobs from tenant `bulk`, each 3 model-call steps, plus a
  steady stream of interactive one-call requests from tenant `users`. A fixed fraction of
  background jobs crash once after their second step.
- **Baseline**: Celery 5 on Redis with `acks_late`, autoretry on 429 with exponential back-off,
  one queue, the same worker concurrency.
- **Tributary**: the `llm-requests` profile with a token pool sized to the mock's limit,
  `throttled` nacks using `Retry-After`, priority for interactive requests, fairness by tenant,
  and a checkpoint after each step.
- **Report**: total 429s, duplicate paid calls (the same job step billed more than once), tokens
  wasted, interactive p50 and p95 latency, and total completion time, as a table and charts.
- One command reruns everything: `demo/run.sh`.

## Launch materials

A README that opens with the claim, the demo chart, a 30-second install, and an honest "v0,
single node" status section.
