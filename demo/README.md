# The demo: Celery vs Tributary under LLM rate limits

One command reruns everything: `./run.sh` (about 20 minutes; `DEMO_SCALE=0.1 ./run.sh` for a quick
version). It needs Docker (for Redis), Go and Python 3.

## The setup

- **Mock provider** (`mock_llm.py`): 120,000 tokens per minute and 600 requests per minute, counted
  over a rolling 60 seconds, like real provider limits. Over the limit it returns 429 with
  `Retry-After`. Each accepted call takes `0.2 + tokens/4000` seconds and is billed.
- **Workload** (`workload.py`, fixed seed):
  - 200 background agent jobs from tenant `bulk`, each 3 model calls of 500 tokens, all
    enqueued at once. 20 of them crash once after their second call.
  - 120 interactive requests from tenant `users`, 1 call of 300 tokens each, one every 0.5
    seconds starting 5 seconds in.
- **Both systems** run 20 concurrent workers and call the same mock.

**Celery 5 + Redis** (`celery_app.py`) uses a common production setup:

- one queue
- `acks_late`
- `autoretry_for` 429s and crashes with `retry_backoff=True`, `retry_backoff_max=60`,
  `retry_jitter=True` and `max_retries=10`

A retry re-runs the task from its first call.

**Tributary v0** (`run_tributary.py`) uses the `llm-requests` profile:

- **Requests:** interactive requests carry `priority=interactive`, background jobs `background`, and
  each carries a `tenant` header and its token estimate.
- **Quota pool:** window mode at 120,000 tokens per minute, with 20% headroom for the top band and
  10-second pacing.
- **Handler:** on a 429 it raises `Throttled(retry_after, scope="subscription")`, and it saves a
  checkpoint after each call.

## Results

Final run, same workload and same provider limits:

| | Celery 5 + Redis | Tributary v0 |
|---|---|---|
| 429 responses | 1,807 | 0 |
| Duplicate paid calls (same job step billed twice) | 47 | 0 |
| Wasted tokens | 23,500 | 0 |
| Interactive latency p50 | 38.3 s | 0.3 s |
| Interactive latency p95 | 134.3 s | 0.3 s |
| Jobs completed | 308 of 320 | 320 of 320 |
| Time to last completed job | 177.7 s | 221.9 s |

- **Celery gave up on 12 jobs**: 9 agent jobs and 3 interactive requests, after 10 rate-limited
  retries each.
- **Tributary finished all 320 jobs, but later.** It deliberately keeps 20% of the provider's
  capacity free for interactive users, so batch work runs at about 80% of the provider's limit.
  The busiest minute peaked at 92,500 of 120,000 tokens.
- **The trade-off is a setting.** A token bucket with a full minute of burst (run 1 below) used the
  provider's whole limit, but interactive users waited behind batch work. Window mode with no
  headroom has not been benchmarked yet.

![Interactive latency](out/latency.png)

## Every configuration we ran

We are publishing all three Tributary runs, not just the best one. The Celery baseline was run
once and not changed.

| Tributary pool | 429s | Duplicate paid calls | Interactive p50 / p95 | Jobs | Last job done |
|---|---|---|---|---|---|
| Token bucket, burst 120,000 | 93 | 0 | 27.0 s / 52.3 s | 320 of 320 | 125.9 s |
| Window, 20% headroom | 0 | 0 | 0.3 s / 12.8 s | 320 of 320 | 183.3 s |
| Window, 20% headroom, 10 s pace | 0 | 0 | 0.3 s / 0.8 s | 320 of 320 | 211.2 s |
| Same, after the code-review fixes (the final run above) | 0 | 0 | 0.3 s / 0.3 s | 320 of 320 | 221.9 s |

What each run taught us, and what changed in Tributary as a result:

1. **The token bucket over-admits.** A bucket with a full minute of burst let batch jobs take the
   whole minute's budget at the start. The provider then returned 429s and interactive requests
   waited for the window to roll over. This led to **window mode**, which applies the provider's
   own rule, and to **headroom** for the top priority band.
2. **Headroom alone is not enough.** Batch work still spent its 80% share in the first seconds.
   Interactive traffic then used up the reserved 20% by about 45 seconds and waited about 12
   seconds for the window. This led to **pacing**, which spreads lower bands across the minute.

3. **A code review found scheduler bugs** that the demo workload did not trigger. With
   standard-priority traffic, or a backlog older than ten minutes, interactive messages could
   be starved. The fixes went in before the final run.

Raw data for every run is in `out/*.json`. The final run is `out/tributary.json`, and the
earlier runs are `out/tributary-run1-*.json` to `out/tributary-run3-*.json`.

## Limits of this benchmark

- **The provider is a mock.** Real providers have variable latency, separate input and output
  token limits, and occasional overloads. The mock enforces only a rolling token and request
  limit.
- **Each configuration was run once.** There are no error bars.
- **Celery can do better with custom code.** It can honour `Retry-After`, use priority queues, or
  checkpoint inside tasks; the baseline uses none of these, which is typical but not the best
  possible. Tributary's checkpoints also require the handler to save progress, which this demo's
  handler does.
- **Tributary v0 runs on a single node**, and its on-disk format is not stable yet.
