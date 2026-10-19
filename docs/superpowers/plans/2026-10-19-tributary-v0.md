# Tributary v0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single-node Go binary and Python client that make LLM and agent jobs rate-limit-aware, resumable and fair, plus a reproducible demo against Celery on Redis.

**Architecture:** One process: an HTTP/JSON API over a Pebble store. Every state change is a synced Pebble batch. Each subscription has an in-memory dispatcher, rebuilt from Pebble at start, that picks the next message by overdue lane, priority band, fairness value and key availability, gated by quota pools. A Python client wraps the API with a worker loop.

**Tech Stack:** Go 1.27 (`/opt/homebrew/bin/go`), `github.com/cockroachdb/pebble` v1.1.5, Go standard library `net/http`; Python 3.14 standard library for the client; Celery 5, Redis (Docker) and matplotlib for the demo only.

**Spec:** `docs/superpowers/specs/2026-10-19-tributary-v0-launch.md`, which refers to sections 7 and 8 of `docs/superpowers/specs/2026-10-19-unified-messaging-platform-design.md`.

## Global Constraints

- Go module path `github.com/nileshkr9919/tributary`; binary name `trib`; header prefix `trib-`.
- Default listen address `127.0.0.1:7070`; anything else only with `--listen`.
- Every produce, lease grant, ack, nack, extend, cancel and pause is committed with `pebble.Sync` before the HTTP response.
- Defaults: 4 shards; idempotency window 15 minutes; lease 30 s initial, extendable, 12 h maximum; `max_attempts` 10; back-off exponential 1 s to 15 min with jitter; `quarantine_after` 3; `on_exhausted` dead-letter; checkpoint limit 64 KiB; `throttled` `retry_after` minimum 100 ms, maximum 15 minutes; `failure.max_throttled` 24 h.
- Dead-letter topic name `<topic>.dlq.<subscription>`.
- Built-in profiles exactly: `work-queue@1`, `llm-requests@1`, `agent-session@1` (Task 4 values).
- Time comes from `internal/clock.Clock`; no package except `clock` calls `time.Now` directly, so tests use a fake clock.
- Python client uses only the standard library.
- The README and `trib --help` state "v0: single node, on-disk format not stable".
- **Commits are deferred**: the owner has not asked for commits, so each task's "Commit" step is skipped and changes stay in the working tree until the owner asks. When commits are made, messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Server restart mid-lease**: a message leased before restart is redelivered after its deadline with attempt plus one and its checkpoint; a token issued before restart still acks if the lease is unexpired. Test in Task 5 (`TestRestartPreservesLeases`).
2. **Ack after lease loss**: a late ack from the old holder is rejected and does not ack the redelivered copy. Test in Task 5 (`TestStaleTokenRejected`).
3. **Throttled storm**: 100 `throttled` nacks leave `attempt` unchanged and nothing is dead-lettered; dispatch for the scope resumes after `retry_after`. Test in Task 8 (`TestThrottledUsesNoAttempt`).
4. **Supersede while in back-off**: a newer record for a key supersedes an older one that is pending, delayed, leased or held, and the older one is never redelivered. Test in Task 6 (`TestSupersedeCoversDelayed`).
5. **Malformed requests**: unknown topic, unknown subscription, bad token, oversized checkpoint and invalid JSON return 4xx with a JSON `error`, never 500 or a crash. Test in Task 9 (`TestAPIErrors`).

---

## File Structure

```
go.mod, go.sum, Makefile
cmd/trib/main.go                CLI: server, dev, version
internal/clock/clock.go         Clock interface, real and fake clocks
internal/store/store.go         Pebble wrapper, batches, key encoding
internal/store/keys.go          key builders and parsers
internal/topic/topic.go         topics, produce, read, idempotency
internal/settings/settings.go   Settings, profiles, validation
internal/sub/state.go           per-message state records
internal/sub/dispatcher.go      in-memory scheduling structures and Receive
internal/sub/sub.go             Subscription: ack, nack, extend, cancel, pause, recovery
internal/sub/ordering.go        per-key busy set, supersede
internal/sub/sched.go           priority bands, ageing, overdue lane, fairness
internal/quota/pool.go          quota pools, estimates, settlement
internal/broker/broker.go       wires topics, subscriptions, pools, ticker
internal/api/http.go            HTTP handlers
internal/api/metrics.go         Prometheus text metrics
test/crash/crash_test.go        kill -9 durability test
clients/python/tributary/       client.py, worker.py, errors.py, __init__.py
clients/python/tests/           test_client.py
demo/                           mock_llm.py, workload.py, run_celery.py, run_tributary.py, report.py, run.sh, docker-compose.yml
README.md                       rewritten for launch
```

---

### Task 1: Repository layout and Go skeleton

**Files:**
- Delete: the TypeScript prototype (`src/`, `package.json`, `package-lock.json`, `tsconfig.json`)
- Create: `go.mod`, `Makefile`, `cmd/trib/main.go`, `internal/clock/clock.go`, `internal/clock/clock_test.go`
- Modify: `.gitignore` (add `/bin/`, `/data/`, `demo/out/`, `__pycache__/`, `.venv/`)

**Interfaces:**
- Produces: `clock.Clock` with `Now() time.Time`; `clock.Real()`; `clock.NewFake(t time.Time) *Fake` with `Advance(d time.Duration)` and `Set(t time.Time)`.
- Produces: `make build` writes `bin/trib`; `make test` runs `go test ./...`.

- [ ] **Step 1: Delete the prototype**

Run: `git rm -r -q src package.json package-lock.json tsconfig.json` and `rm -rf node_modules`.
Expected: `git status` shows deletions only.

- [ ] **Step 2: Write the failing clock test** in `internal/clock/clock_test.go`

```go
func TestFakeClockAdvances(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	c := NewFake(start)
	c.Advance(1500 * time.Millisecond)
	if got := c.Now(); !got.Equal(start.Add(1500 * time.Millisecond)) {
		t.Fatalf("got %v", got)
	}
}
```

- [ ] **Step 3: Run it** — `go mod init github.com/nileshkr9919/tributary` first, then `go test ./internal/clock/`. Expected: FAIL, `NewFake` undefined.

- [ ] **Step 4: Implement `internal/clock/clock.go`** (`Fake` guarded by a mutex) and `cmd/trib/main.go` with subcommands `server`, `dev`, `version`. `version` prints `trib v0.1.0 (v0: single node, on-disk format not stable)`. `server` and `dev` print "not implemented" for now. `Makefile` targets `build`, `test`, `lint` (`go vet ./...`).

- [ ] **Step 5: Verify** — `make test && make build && bin/trib version`. Expected: tests pass; prints the version line.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "chore: remove TypeScript prototype and add Go skeleton"`

---

### Task 2: Store layer

**Files:**
- Create: `internal/store/store.go`, `internal/store/keys.go`, `internal/store/store_test.go`

**Interfaces:**
- Produces: `store.Open(dir string) (*Store, error)`; `(*Store).Close() error`; `(*Store).Get(key []byte) ([]byte, bool, error)`; `(*Store).NewBatch() *Batch`; `(*Batch).Set(k, v []byte)`; `(*Batch).Delete(k []byte)`; `(*Batch).Commit() error` (always `pebble.Sync`); `(*Store).Scan(prefix []byte, fn func(k, v []byte) error) error` in key order; `(*Store).ScanFrom(start, prefix []byte, fn ...)`.
- Produces in `keys.go`: `Key(parts ...any) []byte` building order-preserving keys, where strings are length-prefixed and `uint64` values are big-endian 8 bytes, and `Prefix(parts ...any) []byte`. Key families, each a one-byte tag:
  - `'T'` topic
  - `'O'` next offset per shard
  - `'M'` message (topic, shard, offset)
  - `'I'` idempotency
  - `'S'` subscription config
  - `'X'` message state (sub, shard, offset)
  - `'F'` ack floor (sub, shard)
  - `'P'` pool
  - `'D'` dead letter (sub, seq)

- [ ] **Step 1: Write failing tests**

```go
func TestKeysOrderNumerically(t *testing.T) {
	a := Key('M', "t", uint64(0), uint64(9))
	b := Key('M', "t", uint64(0), uint64(10))
	if bytes.Compare(a, b) >= 0 { t.Fatal("9 must sort before 10") }
}
func TestBatchPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir); b := s.NewBatch(); b.Set([]byte("k"), []byte("v"))
	if err := b.Commit(); err != nil { t.Fatal(err) }
	s.Close(); s2, _ := Open(dir); defer s2.Close()
	v, ok, _ := s2.Get([]byte("k")); if !ok || string(v) != "v" { t.Fatal("lost write") }
}
func TestScanPrefixInOrder(t *testing.T) { /* set M/t/0/{3,1,2}; Scan(Prefix('M',"t",uint64(0))) yields offsets 1,2,3 */ }
```

- [ ] **Step 2: Run** — `go get github.com/cockroachdb/pebble@v1.1.5 && go test ./internal/store/`. Expected: FAIL, undefined symbols.

- [ ] **Step 3: Implement** with Pebble defaults, `pebble.Sync` on every commit. Concurrent `Commit` calls are safe; Pebble groups their syncs.

- [ ] **Step 4: Verify** — `go test ./internal/store/ -race`. Expected: PASS.

- [ ] **Step 5: Commit** — `git commit -am "feat(store): Pebble store with synced batches and ordered keys"`

---

### Task 3: Topics, produce and idempotency

**Files:**
- Create: `internal/topic/topic.go`, `internal/topic/topic_test.go`

**Interfaces:**
- Consumes: `store.*`, `clock.Clock`.
- Produces:
  - `type Message struct { ID string; Topic string; Shard int; Offset uint64; Key string; Headers map[string]string; Payload json.RawMessage; Priority string; DeliverAt time.Time; ProducedAt time.Time; Delete bool }`
  - `type Input struct { Key string; Headers map[string]string; Payload json.RawMessage; Priority string; DeliverAt time.Time; IdempotencyKey string }`
  - `topic.NewManager(s *store.Store, c clock.Clock) (*Manager, error)` loads existing topics.
  - `(*Manager).Create(name string, shards int) error`: shards 1 to 256; default 4 when 0; error `ErrExists`.
  - `(*Manager).Produce(topic string, in []Input) ([]Message, error)`: atomic per shard, one synced batch for the whole call.
  - `(*Manager).Get(topic string, shard int, offset uint64) (Message, bool, error)`
  - `(*Manager).Scan(topic string, shard int, from uint64, fn func(Message) error) error`
  - `(*Manager).Shards(topic string) (int, error)`
  - `(*Manager).OnAppend(fn func([]Message))`: listeners are called after commit, in order.
  - `topic.FormatID(topic string, shard int, offset uint64) string` returns `topic~shard~offset`; `topic.ParseID(id string) (string, int, uint64, error)`.
- Routing: FNV-1a 64-bit of the key, else of the idempotency key, modulo the shard count. Keyless records go round-robin per manager.
- Idempotency: key `'I'`/topic/idempotency key stores the message ID and an expiry 15 minutes ahead. Within the window a duplicate returns the original `Message` and appends nothing. Expired entries are ignored and overwritten.

- [ ] **Step 1: Write failing tests**
  - `TestProduceAssignsSequentialOffsetsPerShard`: same key three times, so offsets 0, 1, 2 on one shard.
  - `TestKeyRoutingIsStable`: the same key always maps to the same shard across a reopen.
  - `TestIdempotencyWithinWindow`: a second produce with key `"req-1"` returns the same ID, and the shard holds 1 message.
  - `TestIdempotencyExpires`: advance the fake clock 16 minutes; the produce creates a new message.
  - `TestProduceSurvivesReopen`: produce, reopen the manager, `Get` returns the payload, and the next offset continues from 3.
  - `TestOnAppendCalledAfterCommit`
- [ ] **Step 2: Run** — `go test ./internal/topic/`. Expected: FAIL.
- [ ] **Step 3: Implement** as specified. The next offset per shard is kept in memory, loaded at open from key `'O'`, and written in the same batch.
- [ ] **Step 4: Verify** — `go test ./internal/topic/ -race`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(topic): topics, routed produce, idempotency window"`

---

### Task 4: Settings and built-in profiles

**Files:**
- Create: `internal/settings/settings.go`, `internal/settings/settings_test.go`

**Interfaces:**
- Produces:
  - `type Band struct { Name string; Weight int; MaxWait time.Duration }`
  - `type Settings struct { Profile string; Ordering string /* "none"|"per-key" */; Supersede bool; Lease Lease; Failure Failure; Priority []Band; MaxTotalWait time.Duration; FairnessHeader string; MaxInFlightPerValue int; Pool string; EstimateHeader string; DefaultEstimate float64; MaxInFlight int }`
  - `type Lease struct { Initial, MaxTotal time.Duration; Extendable bool; CheckpointLimit int }`
  - `type Failure struct { MaxAttempts int; BackoffMin, BackoffMax time.Duration; Jitter bool; QuarantineAfter int; OnExhausted string /* dead-letter|drop|hold */; MaxCost float64; MaxThrottled time.Duration }`
  - `settings.Resolve(profile string, params map[string]string, overrides json.RawMessage) (Settings, error)`: starts from the built-in, applies the JSON overrides (snake_case field names, durations as strings like `"30s"`), then validates.
  - `settings.ErrUnknownProfile`, `settings.ErrMissingParam`, `settings.ErrInvalid`.
- Built-in values (exact):
  - `work-queue@1`: ordering `none`; lease 30 s / 12 h / extendable / 64 KiB; failure 10 attempts, back-off 1 s to 15 min, jitter, quarantine 3, `dead-letter`, max cost 0 (off), max throttled 24 h; no bands; `MaxInFlight` 10,000.
  - `llm-requests@1`: as `work-queue@1` plus:
    - 5 attempts
    - bands `interactive` (weight 8, max wait 2 s), `standard` (2, 30 s), `background` (1, 5 min); `MaxTotalWait` 10 min
    - `FairnessHeader` `tenant`, `MaxInFlightPerValue` 0 (off)
    - `EstimateHeader` `x-est-tokens`, `DefaultEstimate` 1,000
    - required param `pool`
  - `agent-session@1`: as `work-queue@1` with ordering `per-key`, `Supersede` true, lease 5 min initial and 6 h maximum, 3 attempts, `dead-letter`.
  - A profile name without a version means `@1`.
- Validation (`ErrInvalid` with the field name):
  - `Supersede` requires `per-key`.
  - Every band needs a weight of at least 1.
  - `Lease.Initial` must not exceed `Lease.MaxTotal`.
  - `CheckpointLimit` is at most 65,536.
  - `OnExhausted` must be one of its three values.

- [ ] **Step 1: Write failing tests**
  - `TestLLMRequestsRequiresPool`
  - `TestOverridesApply`: overrides `{"failure":{"max_attempts":2},"lease":{"initial":"10s"}}` yield 2 and 10 s.
  - `TestSupersedeNeedsPerKey`
  - `TestUnversionedNameMeansV1`
  - `TestAgentSessionValues`: assert the exact values above.
- [ ] **Step 2: Run** — `go test ./internal/settings/`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify** — PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(settings): built-in profiles, overrides, validation"`

---

### Task 5: Subscription core: leases, ack, nack, extend, failure handling, recovery

**Files:**
- Create: `internal/sub/state.go`, `internal/sub/dispatcher.go`, `internal/sub/sub.go`, `internal/sub/sub_test.go`

**Interfaces:**
- Consumes: `store`, `topic.Manager`, `settings.Settings`, `clock.Clock`, `quota.Gate` (interface defined here, implemented in Task 8; tests pass `sub.Unlimited{}`, a no-op `Gate` defined in this package).
- Produces:
  - `type Gate interface { Reserve(sub string, m topic.Message) (charge float64, ok bool, retryAt time.Time); Settle(sub string, charged float64, usage map[string]float64) }`
  - `type Delivery struct { ID, Token, Key string; Headers map[string]string; Payload json.RawMessage; Attempt int; OperationID, Priority string; Checkpoint json.RawMessage; PreviousHolder string }`
  - `sub.New(name, topic string, st settings.Settings, deps Deps) (*Subscription, error)` with `type Deps struct { Store *store.Store; Topics *topic.Manager; Clock clock.Clock; Gate Gate }`. It persists the config under `'S'`, creates the dead-letter topic, and loads state.
  - `sub.Load(name string, deps Deps) (*Subscription, error)` rebuilds from disk at start.
  - `(*Subscription).Receive(member string, max int) []Delivery` (non-blocking; long-poll lives in the broker)
  - `(*Subscription).Ack(token string, usage map[string]float64) (Status, error)`
  - `(*Subscription).Nack(token string, n NackRequest) (Status, error)` with `type NackRequest struct { Reason string; RetryAfter time.Duration; Scope string; Checkpoint json.RawMessage; Error string }`
  - `(*Subscription).Extend(token string, d time.Duration, checkpoint json.RawMessage) (Status, error)`
  - `(*Subscription).Tick()` expires leases and releases due delays
  - `(*Subscription).OnAppend([]topic.Message)`
  - `(*Subscription).Counts() map[string]int`
  - `(*Subscription).DeadLetters() []DeadLetter`
  - `(*Subscription).Requeue(ids []string) int`
  - `type Status string` with values `ok`, `superseded`, `cancelled`, `paused`, `lost`.
- Message states: `pending`, `delayed`, `leased`, `held`, `acked`, `dead-lettered`, `dropped`, `superseded`, `cancelled`.
- State record `'X'`/sub/shard/offset (JSON): state, attempt, lease ID, holder, deadline, not-before, checkpoint, consecutive expiries, cost used, throttled time.
- The **ack floor** per shard (`'F'`) advances over contiguous terminal states, and their `'X'` records are deleted in the same batch.
- **Token**: base64url of `sub|shard|offset|attempt|leaseID`, where `leaseID` is a random `uint64`. A token is valid only if the record is `leased` with the same attempt and lease ID; otherwise `Status` is `lost`.
- **Every lease grant is committed** (one batch per `Receive` call) before deliveries are returned.
- **Back-off**: `min(BackoffMax, BackoffMin * 2^(attempt-1))`, with jitter uniform in [50%, 100%] when on.
- **Lease expiry**: attempt plus one and consecutive expiries plus one. If consecutive expiries reach `QuarantineAfter`, apply `on_exhausted` with reason `consumer crash loop`. Otherwise go to `delayed` with back-off.
- **Exhaustion**: `dead-letter` appends a copy to the dead-letter topic with headers `trib-failure-reason`, `trib-attempts`, `trib-source` and `trib-operation-id`, and records an entry under `'D'`. `hold` parks the message. `drop` ends it.
- **Requeue**: puts a dead-lettered message back to `pending` with attempt 0 and the same operation ID; requeueing the same ID twice has no effect.
- **Operation ID**: the message ID, or the `trib-operation-id` header if present.
- **Checkpoint** above `CheckpointLimit` returns `ErrCheckpointTooLarge`.

- [ ] **Step 1: Write failing tests** (all with a fake clock)
  - `TestReceiveLeasesAndAckFloorAdvances`: produce 3, receive 3, ack all; `Counts()["acked"]` is 0 because the floor advanced, and no `'X'` keys remain.
  - `TestLeaseExpiryRedeliversWithAttempt`: receive, advance 31 s, `Tick`, wait out the back-off of 1 s, receive again; attempt is 2.
  - `TestStaleTokenRejected`: old token after redelivery gives `lost`, and the new delivery is still `leased`.
  - `TestQuarantineAfterThreeExpiries`: three expiries without ack or nack; the message is dead-lettered with reason `consumer crash loop`.
  - `TestMaxAttemptsDeadLetters`: nack `retry` up to `MaxAttempts`; the dead-letter topic holds 1 copy with `trib-attempts` set.
  - `TestRequeueOnce`
  - `TestCheckpointReturnedOnRedelivery`: extend with checkpoint `{"step":2}`, expire; the next delivery's `Checkpoint` is `{"step":2}` and `PreviousHolder` is the first member.
  - `TestRestartPreservesLeases`: lease, `Load` a new `Subscription` from the same store, and ack with the old token gives `ok`.
  - `TestDelayedMessageNotDispatchedEarly`: `DeliverAt` 10 s ahead.
- [ ] **Step 2: Run** — `go test ./internal/sub/`. Expected: FAIL.
- [ ] **Step 3: Implement.** The dispatcher holds a ready queue per (band, fairness value), a delayed min-heap by not-before, and a lease min-heap by deadline. Task 7 fills in the scheduling policy; for now there is one FIFO by (shard, offset). Each subscription has its own mutex.
- [ ] **Step 4: Verify** — `go test ./internal/sub/ -race -count=3`. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(sub): leases, tokens, ack/nack/extend, back-off, dead letters, recovery"`

---

### Task 6: Per-key ordering, supersede, cancel and pause

**Files:**
- Create: `internal/sub/ordering.go`, `internal/sub/ordering_test.go`
- Modify: `internal/sub/sub.go`, `internal/sub/dispatcher.go`

**Interfaces:**
- Produces:
  - `(*Subscription).Cancel(id string) int`
  - `(*Subscription).CancelKey(key string) int`
  - `(*Subscription).Pause(fairnessValue string)` and `(*Subscription).Resume(fairnessValue string)`; an empty value means the whole subscription.
- **Per-key**: at most one message per key is `leased`; later ones wait in a per-key FIFO. Keyless messages are unconstrained.
- **Supersede** (on `OnAppend`, after commit): for a new keyed record, every earlier record of that key in state pending, delayed, leased or held becomes `superseded` in one batch. A leased holder's next `Extend` or `Ack` returns `superseded`; a superseded record is never redelivered.
- **Cancel**: pending, delayed, leased or held become `cancelled`; holders see `cancelled`.
- **Pause**: no new leases for the scope; holders' `Extend` returns `paused`. Pauses persist under `'S'`.

- [ ] **Step 1: Write failing tests**
  - `TestPerKeyOneInFlight`: two messages for key `u1`; `Receive(max=10)` returns 1; after ack, the next receive returns the second.
  - `TestSupersedeCoversDelayed`: message A for key `u1` is nacked into back-off; produce B for `u1`; A is `superseded`, and after the back-off only B is ever delivered.
  - `TestSupersedeNotifiesHolder`: A is leased; produce B; `Extend(A)` gives `superseded`; once A's lease ends, B is delivered.
  - `TestCancelByID`: cancel a pending message; it is never delivered.
  - `TestPauseFairnessValue`: pause `tenant=bulk`; receive returns only `tenant=users` messages; after resume both flow.
  - `TestPauseSurvivesRestart`
- [ ] **Step 2–4: Run (FAIL), implement, run with `-race` (PASS).**
- [ ] **Step 5: Commit** — `git commit -am "feat(sub): per-key ordering, supersede, cancel, pause"`

---

### Task 7: Priority bands, ageing, overdue lane and fairness

**Files:**
- Create: `internal/sub/sched.go`, `internal/sub/sched_test.go`
- Modify: `internal/sub/dispatcher.go`

**Interfaces:**
- Produces: `type scheduler struct{...}` used by the dispatcher.
  - `push(e entry)` where `entry` holds band index, fairness value, eligible-since time, shard and offset.
  - `next(now time.Time, available func(entry) bool) (entry, bool)`.
- **Order of choice**:
  1. The **overdue lane**: entries whose wait since eligible exceeds `MaxTotalWait`, oldest first.
  2. Bands by smooth weighted round-robin. An entry waiting longer than its band's `MaxWait` is treated as one band higher. The top band ages only into the overdue lane.
  3. Within a band, round-robin across fairness values that have an available entry. Within a value, FIFO.
- `available` lets the caller skip entries whose key is busy, whose fairness value is paused or at `MaxInFlightPerValue`, or whose quota is short.
- Messages with no `trib-priority` (or an unknown band) go to the middle band, or the only band. A subscription without bands is a single band.
- Eligible-since is the produce time, the due time, or the end of back-off.

- [ ] **Step 1: Write failing tests**
  - `TestWeightedBands`: 900 interactive and 900 background available; the first 90 picks contain 80 ± 1 interactive (weights 8 and 1, no standard band).
  - `TestAgeingPromotes`: a background entry older than its 5-minute max wait is served before fresh `standard` entries.
  - `TestOverdueLaneFirst`: an entry older than `MaxTotalWait` is served before fresh `interactive` entries.
  - `TestFairnessRoundRobin`: tenant `bulk` has 1,000 entries and `users` has 1; the `users` entry is among the first 2 picks.
  - `TestPerValueInFlightCap`
- [ ] **Step 2–4: Run (FAIL), implement, run (PASS).**
- [ ] **Step 5: Commit** — `git commit -am "feat(sub): priority bands with ageing, overdue lane, fairness"`

---

### Task 8: Quota pools and provider-aware nacks

**Files:**
- Create: `internal/quota/pool.go`, `internal/quota/pool_test.go`
- Modify: `internal/sub/sub.go` (nack reasons, `max_cost`, `max_throttled`), `internal/sub/sub_test.go`

**Interfaces:**
- Produces:
  - `quota.NewRegistry(s *store.Store, c clock.Clock) (*Registry, error)`
  - `(*Registry).Create(name, unit string, perMinute, burst float64) error`
  - `(*Registry).Gate() sub.Gate`
  - `(*Registry).Pause(pool string, until time.Time)`
  - `(*Registry).Stats(pool string) PoolStats` with `type PoolStats struct { Tokens, Debt, Granted, Settled, Overshoot float64 }`
- **Bucket**: capacity `burst`, refill `perMinute/60` per second, starting full.
- **Reserve**:
  - estimate = the value of `EstimateHeader`, else `DefaultEstimate`, times the subscription's ratio (an exponential moving average of actual over estimate, alpha 0.2, starting at 1.0, clamped to [0.25, 4]);
  - grant when tokens ≥ estimate or when tokens equal the full burst (a single huge request is never starved forever);
  - otherwise return `retryAt` when enough tokens will exist.
- **Settle**: usage key `tokens` (or the pool's unit name) is compared with the charge. Tokens may go negative (debt), and overshoot accumulates the positive differences. Pool state is persisted under `'P'` at most once per second and at shutdown.
- **Nack reasons in `sub`**:

  | Reason | Effect |
  | --- | --- |
  | `retry` | Counts an attempt; follows back-off. |
  | `throttled` | No attempt. Pauses `scope` (`message` default, `subscription`, `pool`) for `retry_after`, clamped to [100 ms, 15 min]. |
  | `quota_exhausted` | No attempt. Pauses `scope` (default `pool`) for `retry_after` if given, else until resumed. |
  | `permanent` | Applies `on_exhausted` at once. |

  - Any unknown reason is `retry`.
  - Throttled time accumulates per message. Past `MaxThrottled`, further throttles count attempts.
  - `max_cost`: after settling, if the message's summed reported `cost` (or `tokens` when the pool unit is tokens) exceeds `MaxCost`, apply `on_exhausted` with reason `max cost exceeded`.

- [ ] **Step 1: Write failing tests**
  - `TestTokenWeightedDispatch`: pool 10,000 per minute with burst 1,000; mixed estimates of 100 and 900 over 5 simulated minutes, where each received message is acked with actual equal to estimate. Granted tokens are within 10,000×5 + 1,000 + 10%.
  - `TestSettleCreatesDebt`: estimate 100, actual 600; the next reserve waits until debt is repaid.
  - `TestLearnedRatio`: after 20 settles with actual 2× estimate, the charge for estimate 100 is between 180 and 220.
  - `TestThrottledUsesNoAttempt`: 100 throttled nacks with `retry_after` 100 ms; attempt stays 1 and nothing is dead-lettered.
  - `TestThrottledSubscriptionScopePausesAll`
  - `TestQuotaExhaustedPausesPoolUntilResume`
  - `TestPermanentDeadLettersImmediately`
  - `TestMaxCostDeadLetters`
- [ ] **Step 2–4: Run (FAIL), implement, run (PASS).**
- [ ] **Step 5: Commit** — `git commit -am "feat(quota): token pools with learned estimates; provider-aware nacks"`

---

### Task 9: Broker, HTTP API, metrics and CLI server

**Files:**
- Create: `internal/broker/broker.go`, `internal/api/http.go`, `internal/api/metrics.go`, `internal/api/http_test.go`
- Modify: `cmd/trib/main.go`

**Interfaces:**
- Produces:
  - `broker.Open(dir string, c clock.Clock) (*Broker, error)` loads topics, pools and subscriptions, and starts a 50 ms ticker calling `Tick` on each subscription.
  - `(*Broker).Close() error`
  - Accessors used by the API.
  - **Long-poll** `(*Broker).Receive(ctx, sub, member string, max int, wait time.Duration) ([]sub.Delivery, error)`: a per-subscription notify channel is signalled on append, ack, tick release and resume. It returns when at least one delivery exists or `wait` passes (maximum 30 s).
  - `api.Handler(b *broker.Broker) http.Handler` implementing every route in the spec's HTTP API table.
  - **Errors**: JSON `{"error": "..."}` with 400 (bad JSON or validation), 404 (unknown topic or subscription), 409 (exists), 413 (checkpoint or payload too large; payload limit 8 MiB). Never 500 for client mistakes.
  - `/metrics` as Prometheus text, with these series labelled by `subscription`, `state` and `band`:
    - `trib_messages{subscription,state}`
    - `trib_deliveries_total`
    - `trib_nacks_total{reason}`
    - `trib_pool_tokens{pool}`, `trib_pool_debt{pool}`, `trib_pool_overshoot_total{pool}`
    - `trib_wait_seconds` histogram by `band`
    - `trib_usage_total{subscription,field,label}`, where `label` is the fairness value
  - `trib server --data DIR --listen ADDR` (default `127.0.0.1:7070`); `trib dev` uses a temp dir and prints the URL.
  - Graceful shutdown on SIGINT and SIGTERM.

- [ ] **Step 1: Write failing tests** with `httptest`
  - `TestEndToEndWorkQueue`: create the topic, the `work-queue` subscription and produce 2; receive, ack; counts show 0 pending.
  - `TestLongPollWakesOnProduce`: a receive with `wait_ms` 2000 returns within 200 ms after a produce lands.
  - `TestLLMRequestsFlow`: a pool plus the `llm-requests` subscription with param `pool`; an ack with usage updates `trib_pool_debt`.
  - `TestAPIErrors`: unknown topic gives 404; bad JSON gives 400; garbage token gives 200 with status `lost`; a 70 KiB checkpoint gives 413; duplicate topic gives 409.
- [ ] **Step 2–4: Run (FAIL), implement, run (PASS).**
- [ ] **Step 5: Manual check** — `make build && bin/trib dev`, then `curl -s localhost:7070/healthz`. Expected: `ok`.
- [ ] **Step 6: Commit** — `git commit -am "feat(api): HTTP API, long-poll receive, metrics, trib server and dev"`

---

### Task 10: Crash durability test

**Files:**
- Create: `test/crash/crash_test.go`

**Interfaces:**
- Consumes: the built `bin/trib` binary and the HTTP API.

- [ ] **Step 1: Write the test** `TestKill9NoAckedLoss`, skipped unless `TRIB_CRASH=1`. Loop 50 times:
  1. Start `trib server` on a fixed data directory and a free port.
  2. Run a producer and a consumer concurrently for a random 200 to 800 ms. The producer records every acknowledged ID. The consumer records every ID whose ack returned `ok`.
  3. Kill the server with SIGKILL.

  After the loop, restart and drain. Assertions:
  - every acknowledged produce ID is present (delivered or acked);
  - no ID whose ack returned `ok` is ever delivered again.
- [ ] **Step 2: Run** — `make build && TRIB_CRASH=1 go test ./test/crash/ -v -timeout 10m`. Expected: PASS. A failure is a real bug: fix it in the owning package with a regression unit test first.
- [ ] **Step 3: Commit** — `git commit -am "test: kill -9 durability test"`

---

### Task 11: Python client

**Files:**
- Create: `clients/python/tributary/__init__.py`, `client.py`, `worker.py`, `errors.py`, `clients/python/pyproject.toml`, `clients/python/tests/test_client.py`

**Interfaces:**
- Consumes: the HTTP API.
- Produces:
  - `Client(url="http://127.0.0.1:7070")`, with methods:
    - `.create_topic(name, shards=4)`
    - `.create_pool(name, unit, per_minute, burst)`
    - `.create_subscription(name, topic, profile="work-queue", params=None, overrides=None)`
    - `.publish(topic, payload, key=None, headers=None, priority=None, idempotency_key=None, deliver_at=None) -> str`
    - `.publish_many(topic, items) -> list[str]`
    - `.cancel(subscription, id=None, key=None)`, `.pause(subscription, value=None)`, `.resume(subscription, value=None)`, `.stats(subscription)`
  - `Worker(client, subscription, handler, concurrency=10, member=None)` with `.run(stop_event=None)` and `.run_in_thread() -> threading.Thread`.
  - `handler(job)`: returning acks with `job.usage`; raising `Throttled(retry_after: float, scope="message")`, `QuotaExhausted(retry_after=None, scope="pool")` or `Permanent(reason)` sends that nack; any other exception nacks `retry` with the exception text.
  - `Job` attributes:
    - `.id`, `.key`, `.payload`, `.headers`, `.attempt`, `.operation_id`, `.priority`
    - `.checkpoint` (dict or None) and `.save_progress(dict)` (sends an extend with the checkpoint)
    - `.effect_key(step: str) -> str` (SHA-256 hex of `operation_id + "/" + step`, first 32 characters)
    - `.report_usage(**numbers)`
    - `.cancelled` (True once an extend returns `superseded` or `cancelled`)
  - **Auto-extend**: every `lease/3` seconds and at least once per second for leases of 5 minutes or more, from settings returned by `GET /v1/subscriptions/{sub}`.
  - **Transport**: the standard library `urllib` only, a thread pool for handlers, and a pull loop asking for `concurrency - in_flight` messages with `wait_ms` 5000.

- [ ] **Step 1: Write failing tests** with `unittest`, against a `trib dev` started in `setUpClass` on a free port:
  - `test_publish_and_work`
  - `test_throttled_does_not_count_attempt`
  - `test_checkpoint_resume`: the handler saves progress, then crashes once by raising `SystemExit` inside a subprocess worker; the second attempt sees the checkpoint.
  - `test_supersede_sets_cancelled`
  - `test_effect_key_stable_across_attempts`
- [ ] **Step 2: Run** — `make build && cd clients/python && python3 -m unittest -v`. Expected: FAIL (module missing).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(python): client and worker with checkpoints, effect keys, provider-aware errors"`

---

### Task 12: The demo

**Files:**
- Create: `demo/mock_llm.py`, `demo/workload.py`, `demo/run_celery.py`, `demo/celery_app.py`, `demo/run_tributary.py`, `demo/report.py`, `demo/run.sh`, `demo/docker-compose.yml`, `demo/requirements.txt`, `demo/README.md`

**Interfaces:**
- Consumes: the Python client.
- **Mock LLM** (`mock_llm.py`, standard-library threaded HTTP server, `--port`, `--tpm`, `--rpm`):
  - `POST /v1/complete` with `job`, `step`, `tokens` and `tenant` headers.
  - Over the limit (sliding 60 s window) it returns 429 with `Retry-After` set to the seconds until enough capacity frees.
  - Otherwise it sleeps `0.2 + tokens/4000` seconds, bills `tokens`, and returns 200.
  - It logs each call (time, job, step, tenant, tokens, status, latency) to an in-memory list exposed at `GET /stats` and `POST /reset`.
- **Workload** (`workload.py`), fixed seed 42:
  - 200 `bulk` jobs of 3 steps × 1,500 tokens.
  - Interactive requests from `users` of 1 step × 300 tokens: one every 0.5 s, 120 in total, starting 5 s in.
  - 10% of bulk jobs (chosen by seed) raise a crash once after step 2.
  - Shared by both runners.
- **Defaults**: TPM 60,000, RPM 300, worker concurrency 20.
- **Celery runner**: Redis from `docker-compose.yml`, `acks_late=True`, `task_acks_on_failure_or_timeout=False`, autoretry on HTTP 429 and crashes with `retry_backoff=True`, `max_retries=10`, one queue. A crash restarts the task from step 1, as plain queues do. Interactive latency is measured from enqueue to completion.
- **Tributary runner**:
  - `trib dev`, a pool of 60,000 tokens per minute with burst 3,000, and topic `llm`.
  - Subscription `llm-requests` with `pool` set. Interactive messages carry `priority=interactive`, bulk ones `background`; `tenant` header.
  - On 429 the handler raises `Throttled(retry_after, scope="subscription")`.
  - After each step the handler calls `save_progress`; on redelivery it resumes from the checkpoint.
- **Report** (`report.py`): reads both runs' `/stats` dumps from `demo/out/` and writes `demo/out/report.md`. The table has rows for:
  - total 429s
  - duplicate paid calls (count of `(job, step)` billed more than once)
  - wasted tokens
  - interactive p50 and p95 latency
  - total time
  - completed jobs

  It also writes `demo/out/latency.png` and `demo/out/429s.png` with matplotlib.
- `run.sh` creates a virtualenv `demo/.venv`, installs `requirements.txt` (`celery[redis]==5.*`, `redis`, `matplotlib`), starts Redis with Docker Compose, runs both, writes the report, and tears down.

- [ ] **Step 1: Smoke test the mock** — `python3 demo/mock_llm.py --port 9100 --tpm 1000 &`, then send two 800-token calls. Expected: the second returns 429 with `Retry-After`.
- [ ] **Step 2: Write and run a reduced demo** — `DEMO_SCALE=0.1 demo/run.sh`. Expected: `demo/out/report.md` exists, and the Tributary column shows 0 duplicate paid calls.
- [ ] **Step 3: Run the full demo** — `demo/run.sh`. Record the real numbers. **Do not tune the baseline to lose**: the Celery configuration stays as specified, and `demo/README.md` documents it.
- [ ] **Step 4: Commit** — `git commit -am "feat(demo): mock LLM, Celery vs Tributary benchmark, report"`

---

### Task 13: Launch README

**Files:**
- Modify: `README.md` (rewrite)
- Create: `docs/quickstart.md`

- [ ] **Step 1: Write the README.**
  - **Opening**:
    - the claim line from the spec
    - the demo chart (`demo/out/latency.png`, copied to `docs/img/`) and the report table with the real numbers from Task 12
  - **Install**: `go install github.com/nileshkr9919/tributary/cmd/trib@latest` and building from source.
  - **Examples**:
    - a 30-second quickstart with `trib dev` and a 15-line Python worker
    - one section per AI feature with a code snippet
  - **Status and limits**: "v0: single node, on-disk format not stable", listing what is not in v0.
  - **Links**: a pointer to the design and the research, and a "how the benchmark works" section that states the Celery configuration exactly.
- [ ] **Step 2: Verify every command in the README runs** on a clean `trib dev`.
- [ ] **Step 3: Commit** — `git commit -am "docs: launch README and quickstart"`
