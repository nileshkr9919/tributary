# Tributary: one log, every consumption model

Design document, revision 6, 2026-10-19. Status: awaiting owner approval. Reviewed by a builder
check and a correctness review; their findings are resolved, as listed in the changelog.

This revision is a clean rewrite. It describes only the current design. The reasoning behind
earlier choices, the reviews and the traceability of every research requirement are in
[the changelog](2026-10-19-tributary-changelog.md). The research is in
[docs/research](../../research/README.md).

## 1. Intent

The owner asked for one platform that addresses the pain points of today's streaming platforms
and message queues, for real users in production. The consumer must choose the push or pull
model, pub-sub, retry policy and delivery semantics, all customizable through profiles. It must
also solve real problems created by AI engineering.

The research across ten reports found that most pains come from one structural split and a
handful of unsafe defaults:

- **Logs and queues are separate systems.** A log (Kafka, Kinesis) is durable and replayable but
  cannot ack, retry, delay or dead-letter one message. A queue (SQS, RabbitMQ, Postgres) has
  those ergonomics but cannot replay or fan out cheaply. Teams run both.
- **Defaults lose data or need a team.** Acks before fsync, partition strategies that drop acked
  writes, single-zone write-ahead windows, runtime tuning, several stateful tiers.
- **AI traffic breaks queue assumptions.** Provider limits are in tokens and money, not messages.
  Interactive and bulk requests compete for one quota. Agent runs last minutes, get superseded by
  the user's next message, and repeat paid calls when retried from scratch.

The core idea: **store every message once, in a replicated log, and let each subscription decide
how it receives it.** Queue, stream and pub-sub are not product types; they are profiles on the
same log. The AI focus is **the work queue for LLM and agent traffic**: token-aware quotas,
provider-aware retries, priority and fairness, supersede and cancel, and long runs that resume
instead of restarting.

### Success

A team replaces "Kafka plus SQS (or RabbitMQ, or Celery on Redis) plus a hand-rolled dead-letter
queue plus an LLM rate limiter" with one binary, keeps the semantics it relied on, and never loses
an acknowledged message under the default configuration.

## 2. Goals, non-goals, anti-goals

### Goals

1. **Consumer-oriented.** Each subscription chooses its delivery model, distribution, guarantee,
   ordering, failure handling and backlog policy (section 7).
2. **Profiles.** Choices are bundled into named, versioned, immutable built-in profiles and
   custom namespace profiles; a subscription pins the version it uses (7.4).
3. **Acknowledged means durable.** A produce ack is sent only after the record is fsynced on a
   quorum. Weaker ack levels are explicit per-topic opt-ins with their loss window stated.
4. **One binary.** Broker, metadata and admin API in a single static binary, with the same
   semantics in development and production.
5. **AI work queue.** Token- and cost-aware quota pools, provider back-pressure, priority with
   ageing, fairness across tenants, supersede and cancel, durable leases with progress
   checkpoints, and effect keys for paid calls (section 8).
6. **No consumer stops producers by accident.** Backlog limits and inactivity suspension bound
   what any subscription can pin (7.11).
7. **Boring operations and upgrades.** Versioned formats, rolling upgrades, backup and restore,
   node replacement, health endpoints and shipped alert rules (sections 9 and 12).
8. **Open governance.** Apache-2.0, no feature gated behind a source-available licence.

### Non-goals

- A workflow engine. Step journaling with deterministic replay, workflow versioning, sagas and
  agent memory belong to engines such as Temporal or Restate. Tributary owns one unit of work and
  its lease, retries, ordering, cancellation and quota; a subscription can start engine runs
  using the message ID as the run ID.
- Stream processing and a connector framework.
- Multi-region replication.
- A hosted service or billing.
- End-to-end exactly-once into external systems. Exactly-once covers processing inside the
  platform (7.7); effect keys and the inbox helper cover external effects.
- Everything listed in the annex (section 17) until it is scheduled by a later design.

### Anti-goals: failure even if it technically works

- Losing an acknowledged message under the default configuration, in any tested fault.
- A combination of settings that is accepted but undefined, or a shared feature (seek, replay,
  filters, lookup, metrics) that behaves differently under different settings.
- One stalled, abandoned or misconfigured consumer blocking producers, unless an operator chose
  that explicitly.
- One tenant, runaway agent or retry loop exhausting a shared model quota for everyone else.
- A profile edit or a platform upgrade silently changing how live subscriptions behave.
- Requiring another stateful system to run the core. The one stated exception is object storage
  for large-payload offload and tiered storage, both optional (8.8, 9.9).
- Unreproducible performance claims, or calling anything "Kafka compatible" without a published
  compatibility matrix.

## 3. Scope and delivery slices

The work is cut into vertical slices. Each slice ends with something usable and a measurable
exit bar (section 16). **Version 1.0, the first production release, is slices S1 to S6.**

| Slice | Delivers | Why in this order |
| --- | --- | --- |
| S1 Storage spine | Single node running the real replication stack with one voter: etcd Raft groups and a metadata group of one, the group clock, segment and state stores, checkpoints, producer sessions, the key index, versioned formats, fault-injecting filesystem, history checkers | Everything else stands on it; a single node later grows into a cluster in place, so its formats are the cluster's formats |
| S2 Work queue | Message subscriptions, profiles with pinning, pull, shared distribution, at-least-once, leases and tokens, back-off, delay index, dead letters, requeue, backlog limits, seek, Go client, minimal HTTP | A durable SQS replacement with replay, usable on its own |
| S3 Replication | Growing to three or more nodes: multi-node groups, quorum fsync, elections and fencing, heartbeat coalescing, leadership balancing, whole-replica rebuild, cluster dispatch routing | The riskiest part, done before the consumer surface grows, so S1 and S2 formats are proven |
| S4 Ordering and streams | Per-key and per-shard ordering, cursor subscriptions (the Kafka consumer-group model), push with credits, control channel, `live-pubsub`, ephemeral subscriptions, memory groups and the `ephemeral` and `low-latency` topic profiles with `leader` and `none` ack levels | Completes the consumer model the owner asked for |
| S5 AI work queue | Composite dispatch indexes, priority and fairness, quota pools, provider nack reasons, supersede, cancel, pause, durable leases, checkpoints, effect keys, cost caps, Python client | The differentiating wedge for AI engineering |
| S6 Production readiness | Tenancy, auth and token lifecycle, TLS, operability (section 12), lookup, stuck report, tracing, TypeScript client, docs, examples, dev container, migration recipes | Turns S1 to S5 into version 1.0 |
| S7 Guarantees and integrations | Exactly-once and producer transactions, outbox relay, webhooks, request-reply and schedules, durable broadcast, exclusive distribution, schema registry, large-payload offload, provider batches, fallback routing, Java client | Post-1.0, in order of demand |
| S8 Ecosystem | Kafka protocol gateway subset with matrix, Kafka and SQS import and mirroring | Post-1.0 migration bridge |
| S9 Scale and storage | Automatic shard split, group split and rebalancing, tiered and object storage, compaction, key salting | Post-1.0 elasticity and cost |

Until S4, every topic is replicated with quorum acks, and every subscription is durable.

## 4. Pain point to design decision

| Pain (research report) | Decision | Slice |
| --- | --- | --- |
| Two systems for queue and log (all) | One log; queue, stream and pub-sub are profiles (7) | S2, S4 |
| No per-message ack, retry, delay, DLQ in logs (Kafka, Kinesis) | Message subscriptions with leases, back-off, delays, dead letters (7) | S2 |
| No replay in queues (SQS, RabbitMQ, Postgres) | Seek by offset or time on every subscription (7.13) | S2 |
| Push vs pull fixed by product (RabbitMQ, Kafka, SQS) | Pull, credit-based push, webhook per subscription (7.8) | S2, S4, S7 |
| Poison messages loop or block (all) | Lease expiry counts as an attempt; crash-loop quarantine; per-key parking (7.3, 7.9) | S2, S4 |
| Dead letters have no owner (all) | Dead-letter topics never expire by time, alert on age, requeue without duplicates (7.10) | S2 |
| Abandoned consumers pin storage (Redis, RabbitMQ) | Backlog limits, suspension with auto-resume, pinning report (7.11) | S2 |
| Acknowledged data lost (NATS, RabbitMQ, Redis, S3 write-ahead designs) | Quorum fsync before ack, group commit, checksums, whole-replica rebuild (9) | S1, S3 |
| Ordering vs parallelism (Kafka, SQS FIFO, Postgres) | Per-key ordering parks one key, not the shard (7.9) | S4 |
| Rebalance storms (Kafka) | Server-side incremental assignment; message subscriptions need none (10) | S3, S4 |
| Consensus group per stream runs hot (NATS) | Many topic-shards per replication group (9.1) | S3 |
| Deep backlog costs memory (RabbitMQ, Redis) | Disk-first log and indexes; numeric memory bound (9.8, 15) | S1, S2 |
| Global memory alarm blocks all publishers (RabbitMQ) | Credit flow control per connection, topic and namespace (11.2) | S2, S6 |
| Several stateful tiers, external metadata (Pulsar, WarpStream) | One binary, embedded Raft metadata (9.1) | S1, S3 |
| Upgrades break things (Kafka, RabbitMQ) | Versioned formats, rolling upgrades, tested upgrade path, patch downgrade (9.10, 12) | S1 onward |
| Client defaults differ by language (librdkafka vs Java) | Clients configure themselves from effective settings (7.4) | S2 onward |
| "Where is my message" (all) | Lookup by ID with per-subscription state; age metrics; stuck report (11.3) | S6 |
| Provider limits are tokens and money, not messages (AI inference) | Quota pools with estimates corrected by reported usage (8.3) | S5 |
| 429s burn retries; spend caps look like throttles (AI inference) | `throttled` and `quota_exhausted` nacks that use no attempt (8.4) | S5 |
| Bulk jobs starve interactive requests; noisy tenants (AI inference, agents) | Priority bands with ageing and an overdue lane; fair dispatch (8.2) | S5 |
| Agent runs restart from zero and repeat paid calls (agents) | Durable leases, progress checkpoints, effect keys (8.6) | S5 |
| A new user message should cancel the old run (agents) | Supersede and cancel (8.5) | S5 |
| AI teams use Python and TypeScript | Python client in S5, TypeScript in S6 | S5, S6 |
| Exactly-once claims that leak duplicates (Kafka, Pub/Sub) | Narrow, precisely defined exactly-once with transactional ack (7.7) | S7 |
| Transactional enqueue only in Postgres queues | Outbox relay (6.6) | S7 |
| Serverless consumers need glue | Webhook delivery with egress guard (7.8) | S7 |
| Large and multimodal payloads hit size walls | Configurable limit; optional object-storage offload (8.8) | S2 (limit), S7 (offload) |
| No migration path (Pulsar, all) | Migration recipes in S6; Kafka gateway and import tools in S8 (13.6) | S6, S8 |
| Partition count is a permanent guess (Kafka, Kinesis) | Fixed per topic (default 4) until S9; automatic split from S9 (5.2) | S9 |
| Object storage slow, local disk costly (new wave) | Storage class per topic (9.9) | S9 |
| Relicensing and acquisition risk | Apache-2.0 | S1 |

## 5. Data model

### 5.1 Objects

- **Namespace**: the tenancy unit. Owns quotas, tokens, custom profiles and quota pools.
- **Topic**: a named log configured by a topic profile.
- **Shard**: a unit of ordering and placement, owning a contiguous range of a 64-bit key hash
  space, with a stable 64-bit topic-shard ID.
- **Subscription**: a named consumer of a topic, configured by a subscription profile plus
  inline overrides.
- **Profile**: a named, versioned bundle of settings (7.4).
- **Quota pool**: a named token bucket in a chosen unit (8.3).

### 5.2 Shards

- Records with a key are routed by hashing the key. Records with no key but an idempotency key
  are routed by hashing the idempotency key, so a retry reaches the same shard. Other records are
  spread by the client in sticky batches.
- Until S9 the shard count is fixed at topic creation: default 4, maximum 256. Documentation says
  so plainly.
- From S9, hot shards split into two children with halves of the hash range. Lineage is gated
  per hash range: a child dispatches a key once the parent has nothing pending for that key's
  hash. Idempotency entries move with their hash range and producer sequence state is copied to
  both children.

### 5.3 Message

| Field | Notes |
| --- | --- |
| `id` | Server-assigned: topic-shard ID, the group's leader term at append, and offset. The term makes IDs unique even when an uncommitted record is lost and its offset reused. Lookup goes straight to the record. Opaque to clients. |
| `key` | Optional bytes. Routing and per-key ordering. |
| `headers` | String map. Prefix `trib-` is reserved for broker-managed headers. |
| `payload` | Bytes, compressed per batch. Default limit 8 MiB per topic. |
| `idempotency_key` | Optional application deduplication key (6.2). |
| `deliver_at` | Optional due time on the group clock (5.4), no later than the topic's maximum delay. |
| `ttl` | Optional. Expiry follows the subscription's `on_expired` setting. |
| `produced_at` | Group-clock time at append (5.4). |
| `trib-priority` | Optional priority band name (8.2). |
| `trib-tenant` | Stamped by the broker from the producer's token when the token is bound to a tenant (11.1). |
| `trib-operation-id` | Carried on dead-letter and rerouted copies: the original message's ID (8.6). |
| `reply_to`, `correlation_id` | Optional; used by request-reply (S7). |
| `encryption_key_id` | Reserved and empty. Keeps room for per-subject crypto-shredding (annex) without a format change. |

### 5.4 Time

Wall clocks drift, and a skewed leader must not expire leases early, fire delays late or freeze
timestamps. Every data group and the metadata group keep a **group clock**:

- **Group time advances by measured elapsed time, not by wall clocks.** The leader proposes a
  **time heartbeat** every second. Each heartbeat's group time is the previous committed group
  time plus the leader's monotonic time elapsed since that heartbeat committed, capped at 1.5
  heartbeat intervals. A new leader continues from the last committed heartbeat the same way. A
  node with a wrong wall clock therefore cannot move group time forward or backward.
- Wall clocks are only a **soft check**: a candidate whose wall clock differs from the median of
  its peers by more than 500 milliseconds is refused in pre-vote, so it cannot become leader, and
  a clock-skew alert is raised. Group time is related to calendar time only for display and for
  `deliver_at` values supplied by producers, which are converted using the leader's offset
  between group time and its wall clock, checked against the same bound.
- **All deadlines are absolute group-clock times**: lease expiry, delays, back-off, TTL,
  suspension, ageing and schedule occurrences. A new leader compares them to the group clock, so
  time already elapsed before an election is never granted again, and no deadline moves later
  with each failover.
- `produced_at` is the group clock at append, so it is monotonic per shard and cannot be frozen
  by a skewed leader.

## 6. Producing

### 6.1 Producer sessions

- A client opens a session and receives a producer ID with an epoch. A **producer ID** is 64
  bits: the ID of the data group that allocated it and a counter in that group. It is unique
  across the cluster without any metadata entry, so millions of short-lived producers cost
  nothing in metadata. Every group accepts every producer ID and keeps sequence state per
  producer per shard.
- **Fencing**: a client may give a stable `producer_name`. The name hashes to one data group,
  which records the name's producer ID and epoch. Reopening a session with the same name increases
  the epoch there, and shards reject batches carrying a lower epoch than the highest they have
  seen for that producer, so a zombie producer is fenced. Without a name, every session is new and
  no fencing is needed.
- Every batch carries the producer ID, epoch and a per-shard sequence number. The broker keeps
  the last committed sequence per producer per shard, acks duplicates with the original IDs and
  rejects gaps. This makes retries safe and keeps order under pipelined requests. Sessions are on
  by default in every client and expire after 7 days idle.
- **Sessionless mode** for HTTP and serverless callers: no session, and deduplication through
  idempotency keys only. A sessionless caller cannot buffer and resend after a gap, so its
  produces always use quorum acks, even on topics whose profile allows weaker ones.

### 6.2 Idempotency keys

Optional, for the same business request sent twice, possibly by different processes. The entry is
written in the same replicated log entry as the append, in the shard's group, so check and append
are atomic. The window is a topic setting, default 15 minutes, maximum 7 days; entries live on
disk and count against the namespace storage quota. A client asked to add a key creates it before
the first attempt and reuses it on every retry.

### 6.3 Batches

Clients batch with a linger time (default 5 ms) and size (default 1 MiB) and compress with zstd
(default) or lz4. A batch is **atomic per shard**: every record is appended or none is, and a
rejection lists each failing record with its reason. Clients split multi-shard batches and report
per shard.

### 6.4 Ack levels and gap recovery

- The topic profile sets the ack level: `quorum` (default: fsynced on a majority), `leader`
  (fsynced on the leader), or `none` (in the leader's memory). A request may ask for a stronger
  level, never a weaker one.
- Under `leader` and `none`, a leader failure can lose acknowledged records. Clients keep every
  batch buffered until the broker reports it **committed**, and a later batch rejected as a gap
  carries the last committed sequence, so the client resends from there. Records are lost only if
  the producer process also dies, which is the stated loss window.
- IDs include the leader term (5.3), so an ID returned for a lost record never names a different
  record.

### 6.5 Other producer rules

- `deliver_at` beyond the topic's maximum delay is rejected.
- Topic auto-create is off by default; a namespace may enable it with a named topic profile.

### 6.6 Producer transactions and outbox relay (S7)

- **Producer transactions** append to several topics atomically. Each open transaction keeps
  **tentative** sequence and idempotency state, used for duplicate and gap checks inside the
  transaction, promoted on commit and discarded on abort. Readers never see uncommitted records
  (7.7).
- **Outbox relay**: reads an outbox table in the application's Postgres database through logical
  replication and produces each row with its row ID as the idempotency key. It stores its
  replication position in metadata and confirms it to Postgres only after the produce commits.

## 7. Subscriptions

### 7.1 Two kinds

Every subscription is one of two kinds. The kind decides which settings exist, which removes most
combinations that would otherwise need rules.

- **Message subscriptions** track each message: lease, ack, nack, attempts, delay, dead letter.
  Work queues, ordered work queues, pub-sub and the AI work queue are message subscriptions.
- **Cursor subscriptions** track a position per shard and commit it cumulatively. They read in
  strict shard order, like a Kafka consumer group, at the lowest cost per message.

Both kinds see the same topic, the same records and the same seek, replay, filter and metrics
behaviour.

### 7.2 Message subscription settings

| Setting | Options | Default |
| --- | --- | --- |
| **delivery** | `pull`; `push` (credit-based, S4); `webhook` (S7) | `pull` |
| **distribution** | `shared` (each message to one member); `broadcast` (every member gets every message); `exclusive` (one active member, hot standbys; S7) | `shared` |
| **guarantee** | `at-most-once`; `at-least-once`; `exactly-once` (S7) | `at-least-once` |
| **ordering** | `none`; `per-key` (one message per key in flight); `per-shard` (strict order, one member per shard at a time) | `none` |
| **concurrency** | handler calls in parallel per member, applied by the client | 10 |
| **ack_flush** | client ack batching | 50 ms or 100 acks |
| **rate_limit** | messages and bytes per second, enforced by the broker | none |
| **lease** | `initial`; `extendable`; `max_total`; `durable` (7.7); checkpoint size limit (8.6) | 30 s; yes; 12 h, clamped to the topic's maximum owed age; no; 64 KiB |
| **failure** | `max_attempts`; `backoff`; `quarantine_after`; `on_exhausted`; `on_expired`; `dead_letter` target (7.3) | 10; exponential 1 s to 15 min with jitter; 3; `dead-letter` (durable) or `drop` (ephemeral); `drop`; `<topic>.dlq.<subscription>` |
| **flow** | `batch_size`; `max_in_flight` per member; `max_in_flight` per subscription; `max_bytes_in_flight` | 100; 1,000; 10,000; 64 MiB |
| **start** | `earliest`, `latest`, a time, or a position | `earliest` for durable, `latest` for ephemeral |
| **lifetime** | `durable`; `ephemeral` (deleted after the last member leaves and a grace period) | `durable` |
| **inactivity_ttl** | no consumer attached and no ack for this long suspends the subscription (7.11) | 7 days |
| **broadcast.members** | `anonymous` (removed after `member_ttl`, default 1 hour) or `named` (each member keeps state and resumes; S7) | `anonymous` |
| **broadcast.new_member_start** | where a new member starts | `latest` |
| **backlog** | `max_age`; `max_bytes`; `on_overflow`: `skip` (record the range), `dead-letter`, `suspend`, or `block-producers` (namespace opt-in only) | topic's maximum owed age; none; `dead-letter` for durable at-least-once and exactly-once; `skip` for at-most-once and ephemeral |
| **filter** | a CEL expression over key and headers, evaluated on the broker | none |
| **delay** | `honour` or `ignore` for `deliver_at` and back-off | `honour` |
| **AI settings** | `priority`, `fairness`, `per_key`, `quota`, `failure.max_cost` (section 8) | off |

### 7.3 Failure handling

- Every **lease expiry counts as an attempt**, so a message that crashes its consumer cannot loop
  forever. `quarantine_after` consecutive expiries with no ack or nack send it straight to
  `on_exhausted` with the reason "consumer crash loop".
- A **nack** carries a reason. `retry` counts an attempt and follows `backoff`; `permanent`
  applies `on_exhausted` at once; the AI reasons `throttled`, `quota_exhausted` and `overloaded`
  are in 8.4.
- `on_exhausted`: `dead-letter` (a copy goes to the dead-letter topic, 7.10), `drop`, or `hold`
  (the message is parked until an operator requeues, skips or deletes it; under per-key ordering,
  only its key waits).
- `on_expired` applies when a message's TTL passes before delivery.

### 7.4 Profiles

A **profile** is a named, versioned bundle of settings. A subscription is always created from a
profile, `work-queue@1` if none is named; inline settings are **overrides** on top of it.

**Built-in subscription profiles** are immutable and versioned with the platform. A release may
add `work-queue@2` but never changes `@1`.

| Profile | Kind and settings | Slice |
| --- | --- | --- |
| `work-queue` | message; pull, shared, at-least-once, no ordering, default failure handling | S2 |
| `ordered-work-queue` | message; as `work-queue` with per-key ordering, 5 attempts, back-off 1 s to 1 min | S4 |
| `event-stream` | cursor; pull, start earliest, backlog overflow `skip`. The Kafka consumer-group model | S4 |
| `live-pubsub` | message; push, broadcast with anonymous members, at-most-once, ephemeral, start latest | S4 |
| `llm-requests` | message; pull, shared, at-least-once; bands `interactive`, `standard`, `background` with ageing; fairness on a tenant header; a quota pool (required parameter `pool`); provider nack reasons; `failure.max_cost`; effect keys | S5 |
| `agent-session` | message; push, shared, at-least-once, per-key ordering with supersede, durable 5-minute lease extendable to 6 hours, checkpoints, 3 attempts, `on_exhausted: dead-letter` | S5 |
| `broadcast` | message; push, broadcast with named members, at-least-once, durable | S7 |
| `webhook` | message; webhook, shared, at-least-once | S7 |
| `rpc` | message; push, shared, at-least-once, 1 attempt, 30 s lease; replies to `reply_to` | S7 |
| `exactly-once-pipeline` | message; pull, shared, exactly-once, per-key ordering | S7 |
| `provider-batch` | message; pull, shared, at-least-once, batch window up to 50,000 messages or 1 hour, durable 72-hour lease, `max_in_flight` 100,000 per member and per subscription, 1 GiB in flight | S7 |

**Built-in topic profiles.** Each sets ack level, retention, maximum owed age, maximum delay, group
class, shard count and caps.

| Profile | Settings |
| --- | --- |
| `durable` (default) | quorum ack; retention 7 days; maximum owed age 14 days; maximum delay 7 days; replicated; 4 shards |
| `low-latency` | leader ack (loss window stated); retention 1 day; maximum owed age 3 days; maximum delay 1 day |
| `archive` | quorum ack; retention 365 days; maximum owed age 365 days; maximum delay 7 days |
| `ephemeral` | memory group; ack none; retention 1 hour; maximum owed age 1 hour; no delays |
| `dead-letter` | quorum ack; no time-based deletion; no delays; size cap with alert; age alert at 7 days |

**Profile parameters.** A profile may declare required parameters, such as `pool` for
`llm-requests`. Creating a subscription without them fails with a message naming them.

**Custom profiles** are defined per namespace, may `extend` another profile version, and override
any settings:

```yaml
kind: subscription-profile
name: payments-jobs
extends: ordered-work-queue@1
failure:
  max_attempts: 5
  backoff: [1s, 30s, 5m, 30m]
  on_exhausted: hold
lease: { initial: 2m }
```

**Pinning and upgrades.**

- A subscription pins the profile version it was created with. Editing a custom profile creates a
  new version; existing subscriptions change only through `trib sub upgrade <sub> --to <version>`.
- Every upgrade shows a dry run: setting differences and any rule failures, validated across the
  whole `extends` chain.
- Changing `kind`, `guarantee`, `ordering`, `distribution`, `delivery` or `lifetime` is refused for a
  live subscription; create a new one. Changing `filter` or the dead-letter target needs an
  explicit flag.
- Topic profiles follow the same rules. Shrinking retention or maximum owed age needs an explicit
  flag, and the dry run lists what would be released.
- **Clamping is visible.** Where a default is adjusted to fit the topic, for example a lease
  maximum on a 1-hour topic, `trib sub show` prints the adjusted value, the original and the
  reason. Explicit values that break a rule are rejected, never clamped.
- **Clients follow the effective settings.** A client attaching to a subscription receives them and
  configures its concurrency, batching, credits, ack flushing and lease extension.
- Profiles live in metadata and can be managed with `trib apply -f <yaml>`, so they can be kept in
  Git.

### 7.5 Compatibility rules

The broker checks these at create time and on every change, against the effective settings and
the topic's profile, and names the conflicting settings in any rejection.

| # | Rule | Why |
| --- | --- | --- |
| R1 | `at-most-once` allows no `failure` or `lease` settings | Nothing is redelivered |
| R2 | `broadcast` excludes `per-shard` ordering and `webhook` delivery | Per-shard means one reader per shard; a webhook has no members |
| R3 | `exclusive` excludes `webhook` delivery | A webhook has no members to fail over between |
| R4 | `webhook` requires `shared` and at-most-once or at-least-once; with `per-shard` ordering it requires webhook concurrency 1 | A 2xx is a per-message ack; ordered delivery to one endpoint is serial |
| R5 | `exactly-once` requires `pull` or `push`, a replicated topic with quorum acks (and the same for every output topic), and excludes supersede | Its guarantees rest on quorum durability and per-delivery tokens; supersede would give an input two outcomes |
| R6 | `per_key.on_arrival: supersede` requires `per-key` ordering; `per_key.max_in_flight` above 1 requires `ordering: none` | Supersede acts on the one in-flight message per key |
| R7 | `backlog.max_age`, `lease.max_total` and every back-off step must not exceed the topic's maximum owed age; the topic's maximum delay must not exceed it either | The data would not be kept |
| R8 | `backlog.on_overflow: dead-letter` requires `inactivity_ttl` shorter than `backlog.max_age` | Otherwise an abandoned subscription copies its backlog into a dead-letter topic before it is suspended |
| R9 | Topics in a memory group use ack level `none` | A memory group has no fsync |
| R10 | `block-producers` requires the namespace flag `allow_block_producers` and excludes `ephemeral` | Stopping producers must be an explicit operator choice |
| R11 | `batch_window` requires at-least-once and a durable lease at least as long as the park time (S7) | Members must survive the park |
| R12 | `failure.on_reason` targets must be other topics (S7) | Prevents routing loops |

Settings that a rule excludes are **removed** from the effective settings, not inherited as
defaults: an at-most-once profile has no failure or lease settings at all. Defaults that would
break R7 or R8 on a given topic are clamped and reported (7.4). The
conformance suite tests both sides of every rule, pairwise combinations of every setting's values,
and a high-risk list kept in the repository.

### 7.6 Message state machine

Each message has one state **per subscription** (per member under named broadcast).

| State | Meaning | Leaves by |
| --- | --- | --- |
| `invisible` | Above the subscription's visibility frontier: not yet committed, or behind an open transaction (7.7) | Becoming visible: `pending` |
| `pending` | Eligible for dispatch | Dispatch: `leased`; cancel, supersede, TTL expiry, backlog overflow, cap pressure: the matching terminal state |
| `delayed` | Waiting for `deliver_at` or back-off | Due time: `pending`; cancel, supersede, TTL expiry, backlog overflow, cap pressure: the matching terminal state |
| `leased` | In flight to a member under a token | Ack: `acked`; nack or expiry: `delayed`, `pending` or `on_exhausted`; supersede or cancel: `superseded`, `cancelled`; transaction prepare: `prepared` |
| `prepared` | Input of a prepared transaction (S7) | Commit: `acked`; abort: `pending` with attempt plus one. **No other transition is allowed** |
| `held` | Parked by `on_exhausted: hold` | Operator requeue: `pending`; operator skip: `skipped`; operator delete: `dropped`; cancel: `cancelled`; a superseding message (8.5): `superseded`; reaching the topic's maximum owed age: `dead-lettered` (7.11) |
| `acked` | Done | Terminal |
| `dead-lettered` | Copied to the dead-letter topic | Terminal here; requeue creates a redelivery (7.10) |
| `dropped`, `skipped`, `expired`, `superseded`, `cancelled` | Ended without processing, for the reason named | Terminal |

- **Nothing acts on `invisible` or `prepared` messages**: not overflow, cap pressure, suspension,
  seek, cancel, supersede or operator commands. Prepared messages are listed as unreleasable data
  under cap pressure.
- A **backwards seek** returns every message after the seek point to `pending`, including
  terminal ones. Seeking is an explicit replay.

### 7.7 Guarantees and fencing

**Delivery tokens.** Every delivery carries a token: topic-shard ID, offset, attempt number, and
either a **lease ID** (durable leases) or the group's **leader term** plus the subscription's
**shard epoch** (other leases). Acks, nacks, extends and transactional acks must present it.

- The shard epoch, kept per subscription per shard in the shard's group, increases only on
  assignment changes and seeks, and is committed before the first dispatch that uses it.
- A leader election increases the term, which invalidates non-durable leases on that group only.
  No per-subscription records are written at election time.
- **Ordered subscriptions** (per-key or per-shard ordering) always commit lease grants before
  sending, batched under group commit, so a new leader knows every holder and deadline and never
  dispatches the next message for a key or shard while an earlier one may still be running. When
  a token is revoked by a seek or a shard move, the dispatcher waits for the holder's
  acknowledgement of the revocation or for the lease deadline, whichever is first, before
  dispatching that key or shard again.
- **Durable leases** are those with `lease.durable: true`, an `initial` lease longer than 5
  minutes, covering a batch, or belonging to an ordered subscription. They are committed before the token is sent, validated by lease
  ID, and survive elections; assignment changes and seeks still revoke them. The default 30-second
  lease is not durable, so ordinary dispatch pays no extra commit.

**When state must be committed before acting.**

| Transition | Rule |
| --- | --- |
| At-most-once dispatch | The delivered mark is committed before sending, one commit per dispatch batch |
| Non-durable lease grant | Not committed first; lost on failover, so the message is redelivered (attempt count may undercount by one) |
| Durable lease grant | Committed before the token is sent |
| Ack, nack, extend, cursor commit | Committed before the success response; clients batch with `ack_flush` |
| Transactional ack | Committed through the transaction coordinator (S7) |

State records ride the same group commit as produce traffic.

**Visibility frontier.** Per subscription per shard, the first offset that is not committed or
that belongs to an open transaction. Every read path stops there: dispatch, push, cursor fetch,
seek, replay, lookup (which reports "not yet visible") and the Kafka gateway (S8).

**At-most-once**: no duplicates from the broker; a consumer crash loses the message.
**At-least-once**: redelivered until it reaches a terminal state; duplicates are possible after a
lost lease, and every delivery carries the stable `id` and attempt number.
**Exactly-once (S7)**: exactly-once processing inside the platform.

- A consumer opens a transaction with the namespace's transaction coordinator, acks inputs with
  their tokens and produces outputs. Outputs are invisible until commit.
- Prepare validates tokens and moves inputs to `prepared`; the coordinator writes the decision;
  participants write commit or abort markers. Aborted records are skipped by every reader.
- The open-transaction timeout defaults to 60 seconds, maximum 15 minutes. While a transaction is
  open, every subscription on its shards waits at the visibility frontier, so the oldest open
  transaction is a metric with an alert.
- External side effects are not covered; clients ship effect keys (8.6) and an **inbox helper**,
  a table in the consumer's database keyed by message ID, written in the same database
  transaction as the side effect.

### 7.8 Delivery models

- **Pull**: the consumer requests up to `batch_size` messages and may long-poll. The default.
- **Push** (S4): the consumer opens a stream and grants credits; the broker sends while credits
  remain. HTTP clients use Server-Sent Events.
- **Control channel** (S4): every member keeps a server-to-client stream carrying cancel,
  supersede, pause and lease-lost notices within one second. Extends and acks also return status.
- **Webhook** (S7): the broker POSTs each message, or a batch, signed with HMAC over body and
  timestamp, with `Idempotency-Key` set to the operation ID. A 2xx acks; for batches, a 2xx with a
  list of failed IDs nacks only those. Retryable statuses and timeouts count as attempts; others
  are nacks with the response body as the reason. A circuit breaker pauses delivery without using
  attempts. **Egress guard**: private, loopback, link-local and metadata addresses are refused by
  default, checked after DNS resolution with the address pinned for the request; redirects are not
  followed; namespaces may allowlist, only operators may allow private ranges. Two signing
  secrets can be active for rotation.

### 7.9 Ordering

- **Key index.** Every shard keeps an on-disk index of `(key, offset)` for retained records,
  written once per record from S1 on, and counted in the S1 and S2 performance targets. It serves
  per-key dispatch and future key-level features, and never needs a backfill build because it
  always exists.
- **Per-key ordering**: one message per key in flight. Each subscription keeps a forward-only
  dispatch frontier; a record whose key is busy is skipped and the key is noted. When the key
  frees, a range scan on the key index finds its next record. Memory per subscription is bounded
  by busy keys. If a key's head is retrying, delayed or held, only that key waits.
- **Per-shard ordering**: the server assigns shards to members incrementally (section 10). A
  retrying or delayed head message blocks its shard; the stuck report shows it.
- **Hot keys**: per-key throughput and stall metrics for the top keys. Key salting is in S9.

### 7.10 Dead letters and requeue

- Dead-letter topics use the `dead-letter` profile: no time-based deletion, a size cap with an
  alert, and an alert when the oldest dead letter passes 7 days.
- **Co-location**: a dead-letter topic has the same shard count as its source topic, and its shard
  *i* is placed in the same data group as source shard *i*. Dead-lettering, which changes the
  source message's state and appends the copy, and requeue, which writes a redelivery record that
  pins the copy, are therefore single-group atomic operations, with no cross-group protocol. The
  copy also carries an idempotency key built from the source ID and attempt.
- A dead letter is a copy of the message with `trib-failure-reason`, `trib-attempts`,
  `trib-first-failed-at`, `trib-last-failed-at`, `trib-source` (topic, shard, offset, ID),
  `trib-operation-id` and the last progress checkpoint.
- **Requeue** writes a **redelivery record** in the source subscription's state on the source
  shard, **keyed by the dead-letter copy's ID**, so requeueing the same dead letter twice creates one
  redelivery. Redeliveries form a per-subscription queue that the dispatcher checks before its
  frontier. Because dead-letter shards are co-located with their source shards, the record holds
  a same-group reference count on the copy, which cannot be purged while referenced. The
  redelivery keeps the operation ID and checkpoint unless the operator resets progress. Its token
  names the source shard and the redelivery record, so it works with every guarantee. Other
  subscriptions never see a duplicate.
- **Order on requeue**: under per-key ordering the requeued message goes to the head of its key's
  queue; under per-shard ordering it is dispatched before the shard's next record. The history
  check treats operator requeues as recorded order exceptions.
- If a dead-letter topic is full, dead-lettering falls back to `skip` for that subscription, with
  the range recorded and a critical alert.

### 7.11 Retention, backlog limits and suspension

- **Retention is a floor.** Every record is kept at least as long as the topic's retention. After
  that, it is kept while any non-suspended subscription still owes it, up to that subscription's
  `backlog.max_age`, which may not exceed the topic's **maximum owed age**. Then it is deleted.
- **Backlog overflow**: a message a subscription has owed longer than `backlog.max_age`, or beyond
  `backlog.max_bytes`, follows `on_overflow`. The default never blocks producers.
- **Suspension**: a subscription with no consumer attached and no ack for `inactivity_ttl` is
  suspended. It stops pinning data and raises an alert. It **resumes automatically** when a
  consumer attaches, with the gap recorded and reported to the client. A subscription suspended for
  30 days is deleted, with an alert a week before.
- **Paused** scopes (8.4, 8.5) are not suspended: they keep pinning data and their overflow policy
  is not applied until the topic's maximum owed age, except under cap pressure.
- **Held messages** never outlive their data: a message still held when it reaches the topic's
  maximum owed age is dead-lettered with reason "held past owed age", so a later requeue always
  has its payload.
- **Cap pressure**: each topic and namespace has a hard storage cap. At 90 percent the broker skips,
  with ranges recorded, the oldest owed messages of the subscriptions pinning the oldest segments,
  choosing only segments whose release actually frees space, until usage is back under 85
  percent. Cap pressure never copies to dead-letter topics and never changes the state of invisible,
  leased, held or prepared messages. Instead, when a segment is pinned only by a few such records,
  their bytes are **relocated** into the group's side store (`v/`, counted against the reserve) so
  the segment can be freed. Producers are refused with a retryable "storage full" error only when
  what remains cannot be released or relocated. `trib storage pinning <topic>` names who pins what.
- **Reserves**: state records (acks, leases, transaction markers) and dead-letter appends each have
  reserved space outside the cap, so consumers can always ack and free space. Progress checkpoints
  count against the namespace quota.

### 7.12 Every way a message ends unprocessed

| Cause | Default | Signal |
| --- | --- | --- |
| `on_exhausted: drop` | Ephemeral subscriptions only | Dropped count metric |
| TTL expiry, `on_expired: drop` | All subscriptions | Expired count metric |
| Backlog overflow `skip` | Cursor, at-most-once and ephemeral subscriptions | Skipped range recorded; alert |
| Cap pressure skip | Only near the hard cap | Skipped range recorded; alert |
| Dead-letter topic full | Fallback only | Critical alert |
| Suspension gap | After 7 days with no consumer | Gap recorded; alert; reported on resume |
| Supersede, cancel | Only when configured or requested | Count metrics; reason recorded |
| Consumer crash under at-most-once | At-most-once only | Not detectable by design |
| Leader failure under `leader` or `none` acks with the producer also dead | Those topic profiles only | Gap rejection; loss window documented |
| Memory-group restart | `ephemeral` topics only | Group recreation event |

Dead-lettering and holding are not losses: the message is kept and can be requeued.

### 7.13 Seek and replay

Any subscription can move to an offset, a time, the earliest or the latest record. A seek
increases the shard epochs, invalidating in-flight tokens. Moving backwards returns everything
after the point to `pending` (7.6). Replay into a new subscription never disturbs live ones.

### 7.14 Cursor subscriptions

- Settings: `delivery` (pull or push), `start`, `filter` (filtered records are passed over),
  `lifetime`, `inactivity_ttl`, `backlog` (`skip`, `suspend` or `block-producers`), and
  `failure.quarantine_after` and `failure.dead_letter`.
- Shards are assigned to members by the server, incrementally (section 10).
- **Failures**: a consumer that cannot process a record calls the client's
  `dead_letter(record, reason)`, which appends to the dead-letter topic and commits past it.
- **Crash loops**: when a member's session ends without a clean leave, the next member receives
  that shard in **single-step mode**, one record at a time, until it commits past the position
  the previous member held. Entering single-step mode counts nothing. Only a crash **while a record
  is delivered in single-step mode** counts against that record, and a record that reaches
  `quarantine_after` such crashes is dead-lettered by the broker and the cursor moves past it.
  Network blips and graceful rebalances therefore never dead-letter a healthy record.
- Expired records (TTL) are passed over.

### 7.15 Distribution

- **Shared**: any member may receive any message; no assignment is needed for `none` or
  `per-key` ordering.
- **Broadcast**: every member receives every message, each with its own state. Anonymous members
  (`live-pubsub`) are removed after `member_ttl`; named members (`broadcast`, S7) follow
  `inactivity_ttl` and resume with a recorded gap.
- **Exclusive** (S7): one active member; on failure the whole subscription moves to a standby and
  every shard epoch increases.
- **Fan-out across services**: one subscription per consuming service, each with its own retry and
  dead-letter policy, is the recommended pub-sub pattern. Broadcast is for every-instance cases
  such as cache invalidation.

## 8. The AI work queue

These features make Tributary the work queue for LLM and agent traffic. They apply to message
subscriptions only. They come from the research in
[ai-inference-traffic.md](../../research/ai-inference-traffic.md) and
[ai-agents.md](../../research/ai-agents.md); the changelog maps each research requirement to this
section or to the annex.

### 8.1 Dispatch indexes

Priority and fairness need the dispatcher to find "the next eligible message in band B for tenant
T" without scanning the log.

- A subscription that declares priority or fairness gets a **composite index** on its dimension
  tuple, for example `(band, tenant, offset)`. Subscriptions declaring the same tuple share one
  index. It is written once per record and lives in the state store.
- When a subscription with a new tuple is created on a topic that already holds data, the index is
  **built** from retained records first. The subscription reports `applying` and does not dispatch
  until the build finishes. Indexes no subscription uses are deleted.
- The dispatcher keeps a forward-only frontier per active tuple value. Frontiers live in the state
  store behind a fixed-size cache, and frontiers idle for an hour are evicted, so memory does not
  grow with the number of tenants.
- **Dispatch rate**: a shard leader's dispatcher visits active tuple values in a resumable
  round-robin pass at no less than 50,000 values per second from cache. The **service bound** for a
  value that becomes pending is therefore `10 ms + active values on the shard / 50,000 per second`
  whenever consumer capacity is available: about 0.5 seconds with 25,000 active tenants on a
  shard.

### 8.2 Priority and fairness

- **Bands.** A message may carry `trib-priority`. A subscription declares its bands in order with
  weights (for example `interactive: 8, standard: 2, background: 1`), a maximum wait per band, and
  `priority.max_total_wait`. Bands are served by weight. A message waiting longer than its band's
  maximum moves up one band. A message waiting longer than `max_total_wait` enters the **overdue
  lane**, served before all bands, oldest first.
- A redelivered or delayed message keeps its band; its wait is measured from when it last became
  eligible.
- **Guarantee**: while consumer capacity exceeds the arrival rate of overdue messages, no message
  waits longer than `max_total_wait` plus the service bound of 8.1 on its shard. Across shards, the
  routing in section 10 adds at most the 100-millisecond summary interval.
- **Fairness.** `fairness.key` names a header, typically the broker-stamped `trib-tenant` (11.1).
  Within a band, the dispatcher round-robins across values with pending messages, optionally
  weighted, and `fairness.max_in_flight_per_value` caps any one value. A tenant with a million
  queued messages delays another tenant's first message by at most the service bound of 8.1.
- Priority comes first, then fairness within a band, then key order.

### 8.3 Quota pools

Providers limit tokens and spend, not messages, and the true cost of a call is known only after it
returns.

- A **quota pool** belongs to a namespace and has a unit (`requests`, `tokens`, `cost` or a custom
  unit), a refill rate and a burst. Operators may share a pool across namespaces, matching one
  provider account used by several teams.
- At dispatch a subscription is charged an **estimate**: a header such as `x-est-tokens` or a
  default, multiplied by the subscription's learned ratio of actual to estimated usage. The ack
  carries **actual usage** (any named numbers), and the difference is settled: underuse is
  returned, overuse becomes **debt** that delays the next dispatch.
- `pool.max_outstanding` caps estimated units in flight, which bounds how far actual use can run
  ahead of settlement.
- **Coordination.** Each pool has a coordinator in a data group. Dispatchers request units in
  chunks with a time limit; a dispatcher starts the limit when it sends the request, so a chunk
  can never outlive the coordinator's record of it. Grants are written to a **grant ledger**
  before they are returned; a new coordinator after failover waits out the longest outstanding
  limit before granting again. Ledger entries are compacted once their limit has passed.
- Usage settlements are written to a **settlement outbox** in the same commit as the ack, and
  forwarded to the coordinator with the ack ID until the coordinator acknowledges, then deleted.
  A shard leader failover resends whatever is in the outbox. The coordinator ignores IDs it has
  seen, and expires seen IDs after 24 hours, far longer than any resend.
- **Chunk size** is the pool's burst divided by twice the number of dispatchers currently drawing
  on it, recalculated as dispatchers come and go, with a floor of one estimated message.
- **Bound**: estimated units granted never exceed the pool's refill plus one and a half times its
  burst over any window. Actual use beyond estimates is limited by `max_outstanding` times the
  estimate error and is reported as overshoot and repaid as debt.

### 8.4 Provider-aware nacks

| Reason | Effect |
| --- | --- |
| `retry` (default) | Counts an attempt; follows back-off |
| `throttled` with `retry_after` and a scope (`message`, `key`, `subscription`, `pool`) | Pauses that scope; **uses no attempt** |
| `quota_exhausted` with a scope and optional resume time | Pauses the scope until resumed or the time passes; alerts; uses no attempt |
| `overloaded` | Counts an attempt; feeds fallback routing (S7) |
| `permanent` | Applies `on_exhausted` at once |

- Pausing a **pool** requires the `pool-admin` permission on the consumer's token; otherwise the
  pause applies to the subscription and an alert names the consumer. `retry_after` is at least
  100 milliseconds and at most the pool's `max_pause` (default 15 minutes).
- A message that has spent more than `failure.max_throttled` (default 24 hours) in throttled
  pauses counts an attempt on each further throttle, so it cannot loop forever.

### 8.5 Supersede, cancel and pause

- **Supersede** (`per_key.on_arrival: supersede`): when a new record for a key **becomes visible**
  (committed and below the visibility frontier), every earlier message for that key that is
  pending, delayed, leased or held becomes `superseded`, so a run waiting in back-off cannot run
  later anyway. Its holder gets a notice on the control channel
  within one second. The new record is dispatched when the old lease ends. Because supersede waits
  for visibility, a record that never commits can never cancel the run before it.
- **Cancel** by message ID, or by key within a subscription. Pending and delayed messages become
  `cancelled`; a leased message's holder is notified. Prepared messages are not affected until their
  transaction is decided (7.6).
- **Pause** a subscription, a key, or one fairness value (a tenant, an agent). Dispatch for the
  scope stops once every shard leader involved has confirmed, and holders are notified. Resuming
  is explicit. Pauses are metadata objects and are audited in the operational event log (12.4).

### 8.6 Long runs: durable leases, checkpoints and effect keys

- **Durable leases** (7.7) let a run hold a message for hours across leader elections.
  `agent-session` uses them by default.
- **Progress checkpoints**: an extend or nack may carry up to 64 KiB of progress, stored with the
  message's state and returned on redelivery with the attempt number and the previous holder. A
  successor resumes instead of restarting. Only changed checkpoints are replicated.
- **Operation ID**: the message `id`, or `trib-operation-id` on dead-letter copies and requeues, so
  it is stable across every attempt.
- **Effect keys**: clients provide `effect_key(step)`, a hash of the operation ID and a step name.
  Pass it as the idempotency key to a model provider, payment API or any external call. If a worker
  crashes after the call but before saving a checkpoint, the call runs again on redelivery, and the
  effect key is what lets the provider recognise the repeat. Tributary does not claim a step runs
  only once.

### 8.7 Cost caps and usage reporting

- `failure.max_cost` sends a message to `on_exhausted` once the usage reported across its attempts
  passes the cap.
- Reported usage is aggregated by subscription, pool and configured header labels, such as tenant,
  feature and model, and exposed as metrics and through the admin API.

### 8.8 Later AI features (S7)

- **Large-payload offload**: bodies above a topic threshold go to object storage behind a
  reference; blobs may have a shorter, absolute retention; without one, a blob is
  reference-counted across the record and its dead-letter copies. Blobs are read only through the
  broker. A production cluster needs an S3-compatible store for this feature; a local directory
  works for development.
- **Provider batches**: `batch_window` groups messages into one delivery with a **batch token**,
  parked under a durable lease of up to 72 hours. Any process holding the token and the consume
  permission may ack or nack individual members. Batch tokens are signed and expire with the
  lease.
- **Fallback routing**: `failure.on_reason` rules copy a message to another topic after repeated
  `overloaded` nacks or an open webhook circuit, keeping its operation ID and history.

### 8.9 Workflow engines

Tributary owns one unit of work: its lease, retries, ordering, quota, cancellation and
checkpoint. A run with many effectful steps or multi-day waits belongs in a workflow engine. The
integration is simple: a subscription starts engine runs with the message ID as the run ID, so
redelivery is idempotent, and engines publish events back to topics. S6 documents the pattern with
a Temporal example.

## 9. Storage and replication

### 9.1 Topology

- One binary type. Each node runs the broker, optionally a member of the **metadata group** (three
  or five nodes), and replicas of **data groups**. A single node (S1, S2, `trib dev`) is the same
  design with groups of one.
- A **data group** is a Raft group, three replicas by default, hosting many topic-shards and their
  subscription state. The number of groups grows with nodes, not with topics or subscriptions.
- **Placement until S9**: a cluster runs a fixed number of data groups per node, set at node join
  (`groups_per_node`, default 8; a single node runs 8 groups of one). A new topic's shards are
  placed on the least-loaded groups by bytes and shard count, and a dead-letter topic's shards
  follow their source shards (7.10). Leadership is balanced continuously by transferring leaders
  so each node leads about the same number of groups. Splitting and automatic replica rebalancing
  arrive in S9; until then, operators move replicas with `trib group move`.
- **Group classes**: `replicated` groups follow the full fsync rule; `memory` groups keep their log
  in memory for `ephemeral` topics. The classes never share a group.
- The metadata group holds namespaces, topics, profiles, subscription configuration, placement,
  pools' configuration, pauses and tokens.

### 9.2 Libraries

- **Raft: `go.etcd.io/raft`.** It is storage-agnostic, supports learners and joint-consensus
  membership changes, and is proven in multi-group systems. Tributary supplies storage, transport
  with heartbeat coalescing across groups, and snapshots.
- **State store: Pebble** (`github.com/cockroachdb/pebble`), an embedded LSM key-value store
  written in Go.
- **Filters: CEL** (`github.com/google/cel-go`).

### 9.3 Write path and durability

- The group leader appends each batch with its idempotency entries to the write-ahead log and
  replicates it. Followers fsync before acknowledging, and an entry commits when a majority has
  fsynced it. **This rule never changes per topic.**
- Ack levels change only when the producer is answered (6.4); they can never cause a committed
  record of another topic to be lost.
- **Group commit**: one fsync covers every entry that arrived during the previous one.
- **The data group is the only sequencer**: the leader of the group hosting a topic-shard assigns
  its offsets, for every storage class.

### 9.4 Write-ahead log and state stores

- **One state store per node.** Each node runs one Pebble instance. It holds every group's Raft
  log entries and Raft state, and every group's applied state, with **every key prefixed by the
  group ID** (9.11). Pebble's own write-ahead log is the node's single fsync stream, so group
  commit is per node, not per group. A group's state is one contiguous key range, so a snapshot is
  a range scan and a rebuild is a range delete plus the group's segment directory.
- The Raft log is for replication and recovery only. Committed entries are applied to:
  - the **segment store**: per topic-shard segment files with a format-versioned header,
    length-prefixed batches with CRC32C, and sparse offset and time indexes. Retention works per
    topic-shard on these files;
  - the **state store** (Pebble), with the key schema in 9.11.
- **Checkpoints**: each node periodically fsyncs the segment store and records, per group, the log
  index the segments reflect. A group's Raft log is truncated below the lowest checkpoint the group
  still needs. A lagging follower is caught up with a snapshot of the group's segment files and its
  key range.
- Segment files live in a directory per group and topic-shard.
- **Trade-off**: every byte is written twice, to the log and to segments. A shared log cannot
  support per-topic retention, because it can only be trimmed from the front. Segment writes are
  buffered and fsynced only at checkpoints, and the log is short-lived.

### 9.5 Corruption and recovery

- Every log entry, segment batch, index page, state-store table and snapshot carries a checksum.
- Each replica persists, with every fsync, the highest log index it has acknowledged. Segments and
  applied state are durable only up to their last checkpoint's log index.
- **On start**: a damaged log entry above the acknowledged index is a torn write and is truncated.
  Segments and applied state are validated only up to their checkpoint index; anything after it is
  discarded and rebuilt by replaying the log. An ordinary crash or power loss is therefore never
  treated as corruption.
- **Corruption** is a checksum failure in the log at or below the acknowledged index, or in
  segments or applied state at or below their checkpoint index.
- **Whole-replica rebuild**: a replica that finds corruption stops voting immediately, is removed
  from the group's voters by a membership change, wipes its data for that group, rejoins as a
  learner **under a fresh replica ID** (Raft IDs are never reused, so stale messages from the old
  incarnation are ignored), catches up from a snapshot, and is promoted to voter. This reuses the ordinary
  membership path and is far simpler to verify than repairing individual entries.
- If a majority of a group's replicas are corrupt, the group stops accepting writes and raises a
  critical alert; recovery is an operator action with a documented runbook.
- The membership and rebuild path is model-checked with TLA+ before S3 ends.

### 9.6 Memory groups

- Raft term, vote and membership are always fsynced, in every group class.
- A restarted memory-group replica is removed from the voters and rejoins as a learner under a new
  identity, then is promoted. Rolling upgrades wait for promotion before restarting the next node.
- If a memory group loses quorum, for example after a full power loss, the metadata group recreates
  it empty and reassigns its topics. Data in ephemeral topics is lost by definition, and a
  recreation event is emitted.

### 9.7 Subscription state placement

- A subscription's state for a shard lives in that shard's data group, so it replicates and fails
  over with the data.
- Dispatch for message subscriptions runs on the shard's group leader. Followers serve record bytes
  to cursor subscriptions in their own zone.

### 9.8 Memory bounds

Resident memory per node is bounded by a formula over the node profile (12.6):

`caches + Pebble memtables + unapplied Raft entries + memory-group logs + connections × buffer +
in-flight leases × 1 KiB`

Each term has a cap in the node profile; memory-group logs drop their oldest ephemeral data at the
cap. Backlogs, delayed messages, keys, index frontiers, quota state and idempotency entries live on
disk. Section 15 gives the targets.

### 9.9 Storage classes (S9)

Per topic, changeable online, with one offset space: `local` (replicated local disk), `tiered`
(sealed segments moved to object storage), and `object` (segment bytes written to object storage in
shared batches, then compacted per shard). Backends: S3-compatible APIs and a local directory for
tests. Object storage is optional and is the stated exception to the "no second system" anti-goal.

### 9.10 Formats and upgrades

- Every segment, log entry, state-store record, snapshot and protocol message carries a format
  version from S1.
- Each release reads the previous release's data and runs in a mixed-version cluster during a
  rolling upgrade. A format is dropped only after a release that rewrites it in the background.
- **Patch-level downgrade**: within a minor version, nodes can be rolled back to the previous patch
  release. New formats are written only after every node runs the new minor version and an
  operator finalizes the upgrade.
- Every release's tests upgrade a data directory from the previous release under load, and test
  patch downgrade.

### 9.11 State-store key schema

The builder may refine encodings, but the layout below is fixed, because it decides what is a
range scan. **Every key starts with the group ID**, so all of a group's state is one contiguous
range. Within a group:

| Prefix | Key | Value |
| --- | --- | --- |
| `r/` | log index | Raft log entry |
| `h/` | (none) | Raft hard state: term, vote, commit index; membership |
| `n/` | producer name | producer ID, epoch (in the group the name hashes to) |
| `p/` | shard, producer ID | last committed sequence, epoch, expiry |
| `i/` | shard, idempotency key | message ID, expiry |
| `k/` | shard, key, offset | (empty) — the key index |
| `x/` | shard, tuple ID, tuple values, offset | (empty) — composite indexes |
| `s/` | subscription, shard, `floor` | ack floor |
| `s/` | subscription, shard, `r`, range start | acked range end |
| `s/` | subscription, shard, `m`, offset | state, attempt, lease ID or term and epoch, holder, deadline, checkpoint reference |
| `s/` | subscription, shard, `e` | shard epoch |
| `s/` | subscription, shard, `f`, tuple value | dispatch frontier |
| `d/` | shard, due time, subscription, offset | delay index |
| `c/` | subscription, shard, offset | progress checkpoint bytes |
| `q/` | subscription, shard, dead-letter copy ID | redelivery record: operation ID, checkpoint reference, attempt |
| `a/` | subscription | coordinator state: members, assignments, crash counts, single-step boundaries |
| `o/` | pool, ack ID | settlement outbox entry awaiting the coordinator's acknowledgement |
| `v/` | shard, offset | relocated record bytes for held, leased or prepared records (7.11) |
| `t/` | transaction ID | transaction state (S7) |
| `g/` | pool, chunk ID | grant ledger entry (in the pool coordinator's group) |
| `u/` | pool, ack ID | settlement dedupe entry (in the pool coordinator's group) |

## 10. Cluster dispatch routing

- **Leader map**: clients and nodes cache, from the metadata group, which node leads each shard's
  data group.
- **Native clients** connect directly to the nodes that lead their subscription's shards, one
  multiplexed connection per node. Receive requests carry credits per node. Each leader sends a
  **band summary** every 100 milliseconds on the control channel, and the client splits its credits
  across nodes in proportion to where the highest bands and oldest messages are. The client merges
  deliveries into its handler queue.
- **HTTP and serverless callers** use any node as a gateway. The gateway fans out long-polls with
  credits to the leaders, merges the results using the same summaries, and forwards acks.
- **Acks** go to the leader of the shard named in the token. A redirect error with the new leader
  is returned after an election; non-durable leases from the old term are revoked.
- **Shard assignment** for per-shard ordering, exclusive distribution and cursor subscriptions is
  done by a **subscription coordinator** hosted in a data group that metadata assigns to the
  subscription when it is created (recorded, not hashed, so adding groups never moves it). Its
  state lives in that group under the `a/` prefix: members, assignments, crash counts and
  single-step boundaries, so a coordinator failover loses nothing.
- Members heartbeat to the coordinator. When a member joins or leaves, only the shards that must
  move are moved. **A shard move** is: the coordinator asks the shard's group to commit a new epoch;
  the shard's leader revokes the old member's tokens and waits for its acknowledgement or lease
  deadline (7.7); the coordinator then hands the shard to the new member. There is no
  stop-the-world barrier.

The sequence for one pull by a native client:

1. The client looks up the subscription's shard leaders and opens connections to their nodes.
2. It sends each node a receive request with credits weighted by the latest band summaries.
3. Each leader runs its dispatcher, grants leases and returns messages with tokens.
4. The client runs handlers and sends acks, batched by `ack_flush`, to each shard's leader.
5. Each leader commits the acks and settles quota usage before responding.

## 11. Platform features

### 11.1 Tenancy, auth and tokens (S6)

- Namespaces with quotas: produce and consume bytes per second, stored bytes, topics,
  subscriptions, connections, idempotency entries and checkpoint bytes.
- **API tokens** carry scopes: namespace, topic-name prefixes, and permissions (`produce`,
  `consume`, `admin`, `pool-admin`). Tokens expire (default 90 days), two can be active per
  identity for rotation, and revocation reaches every node within one second.
- A token may be **bound to a tenant**. The broker stamps `trib-tenant` on every record it produces
  and rejects a different client-supplied value, so fairness and quotas by tenant cannot be
  spoofed.
- TLS for clients and mTLS between nodes.
- **Before S6**, with no auth configured, the server listens on localhost only unless started with
  an explicit insecure flag.

### 11.2 Flow control

Credit-based per connection and per topic (S2) and per namespace (S6). A throttled producer gets a
retryable error with a suggested back-off. There is no cluster-wide stop. Memory limits are
absolute values from the node profile, never percentages.

### 11.3 Observability

- **Prometheus metrics** per subscription: backlog count and bytes, oldest owed message age, in
  flight, redeliveries, dead-letter rate, quarantines, skipped ranges, suspensions, stalled keys.
  Per topic: bytes in, out and stored. Per node: memory against each bound, fsync latency, group
  leadership, clock offset.
- **AI metrics** (S5): per pool, units granted, settled, debt and overshoot; per band, wait against
  its maximum; per fairness value, dwell time and in flight; superseded, cancelled, throttled and
  paused counts; reported usage by label.
- **Message lookup** (S6): the ID locates the record directly. The result shows, for every
  subscription, its state from 7.6, with holder, attempt and deadline, plus a one-hour history of
  transitions. Older acks show "acked" without a time, because ack state is stored as ranges.
- **Stuck report** (S6): why a subscription is not progressing: no consumers, all leases held,
  head-of-key or head-of-shard retries, held messages, delays, pauses, quota debt, rate limits, an
  open transaction at the visibility frontier, or an index being built.
- **Tracing** (S6): incoming `traceparent`, `tracestate` and `baggage` headers are kept byte for
  byte, and the broker emits OpenTelemetry messaging spans for produce, dispatch and ack.

### 11.4 Schema registry (S7)

Per-topic schemas with versions (JSON Schema and Protobuf), compatibility modes (backward by
default), readable diffs on incompatible registration, and optional produce-time validation with
per-record errors.

### 11.5 Cost visibility

Per namespace and topic: bytes produced, delivered and stored, cross-zone bytes, idempotency
entries, checkpoint bytes, and reported usage by subscription, pool and label.

## 12. Operability (S6)

### 12.1 Health

`/healthz` reports the process is alive; `/readyz` reports the node has joined its groups, caught
up within a bound, and can serve. Both are on the admin port.

### 12.2 Alerts

An alert is three things: a named metric condition, a rule in the **shipped Prometheus rule pack**,
and an event on the operational event log (12.4). The rule pack covers every alert this document
names, including clock skew, corruption, majority corruption, storage cap pressure, dead-letter
age and fullness, suspension, quota overshoot, open-transaction age and memory bounds. Each alert
has a runbook page.

### 12.3 Node lifecycle

- **Add**: a new node joins, and an operator moves replicas onto it with `trib group move`.
  Automatic rebalancing arrives in S9.
- **Decommission**: `trib node drain` moves every replica and leadership off the node, then the
  node can be removed.
- **Replace a failed node**: remove the dead replicas from their groups' voters, then add new
  replicas on the replacement as learners; they are promoted once caught up.
- Metadata-group membership is changed the same way, one node at a time.

### 12.4 Operational event log

A system topic `_events` per namespace, plus one for the cluster, records configuration changes,
profile upgrades, pauses, cancels by operators, suspensions, skipped ranges, group recreations,
rebuilds and alerts. It uses the `durable` profile with one-year retention. It is an operational
record, not a compliance audit log (that is in the annex).

### 12.5 Backup and restore

- `trib backup` writes a snapshot of the metadata group and a checkpoint of every data group to
  object storage or a directory. Each group's checkpoint is consistent; a backup is not atomic
  across groups, and the restore report lists each group's snapshot position.
- `trib restore` builds a new cluster from a backup.
- **Reconciliation on restore**: pool grant ledgers restart empty, so pools refill from zero;
  subscription coordinators rebuild assignments from the restored members' next heartbeats; any
  metadata object whose group state is missing is reported in the restore report. Dead-letter
  copies and their redelivery records are co-located, so they are always restored together.
- A restore drill runs in CI on every release: back up a loaded cluster, restore it, and run both
  the producer and consumer history checks up to each group's snapshot position.

### 12.6 Node profile

Every resource setting is an absolute value with a default and is validated at start.

| Setting | Default |
| --- | --- |
| State-store block cache | 1 GiB |
| Segment read cache | 512 MiB |
| Dispatch frontier and index caches | 256 MiB |
| Per-connection buffer | 256 KiB |
| Maximum connections | 10,000 |
| Pebble memtables | 256 MiB |
| Unapplied Raft entries | 256 MiB |
| Memory-group logs | 1 GiB |
| Maximum data group replicas | 200 |
| Maximum topic-shards | 2,000 |
| Group-commit maximum wait | 2 ms |
| Raft heartbeat and election timeouts | 100 ms and 1 s |
| Time heartbeat interval and clock-skew bound | 1 s and 500 ms |
| Checkpoint interval | 30 s |

### 12.7 Sizing and deployment

- A **sizing guide**, derived from the S5 and S6 benchmarks, gives throughput, storage and memory
  per node for each built-in profile.
- A container image, a Kubernetes StatefulSet example with readiness probes and anti-affinity, and
  a systemd unit ship with S6.
- Encryption at rest relies on encrypted disks and encrypted object-storage buckets, as documented;
  native encryption is in the annex.

## 13. Interfaces

### 13.1 API sketch

Protocol Buffers over gRPC. The builder fixes field names; the operations are fixed here.

| Service | Operations |
| --- | --- |
| `Producer` | `OpenSession`; `Produce` (batch to one shard; returns IDs, ack level reached, last committed sequence); `CommitStatus` (stream of committed sequences) |
| `Consumer` | `Receive` (subscription, credits; returns deliveries); `Stream` (push deliveries and control notices); `Ack` (tokens, reported usage); `Nack` (token, reason, `retry_after`, scope, checkpoint); `Extend` (token, duration, checkpoint); `Commit` (cursor positions); `DeadLetter` (cursor record, reason); `Seek` |
| `Transaction` (S7) | `Begin`; `Ack`; `Produce`; `Commit`; `Abort` |
| `Admin` | topics; subscriptions (`create`, `show`, `upgrade`, `resume`, `delete`); profiles (`apply`, `show`, `diff`); pools; `Pause` and `Resume`; `Cancel`; `Lookup`; `Stuck`; `Pinning`; groups and nodes (`move`, `drain`); `Backup` and `Restore` |

A **delivery** carries: ID, key, headers, payload, attempt, token, operation ID, priority band,
progress checkpoint and previous holder.

### 13.2 HTTP gateway

HTTP/JSON for produce, pull, ack, nack and extend from S2; push and the control channel over
Server-Sent Events from S4; full admin parity from S6.

### 13.3 Clients

Go in S2, Python in S5, TypeScript in S6, Java in S7. Each is generated from the protocol plus a
thin layer for sessions, batching, credits, ack flushing, handlers, effect keys and the inbox
helper. Safe defaults everywhere: sessions on, acks only after the handler returns, failures raised
to the caller, never silent drops.

### 13.4 Command line

`trib` is the same binary as the server: `server`, `dev`, `apply`, `topic`, `sub`
(`create`, `show`, `upgrade`, `resume`, `stuck`), `profile`, `pool`, `pause`, `resume`, `cancel`,
`produce`, `consume`, `message get`, `dlq` (`list`, `requeue`), `storage pinning`, `group move`,
`node drain`, `backup`, `restore`.

### 13.5 Development and testing tools

- `trib dev` runs a single node with temporary storage, the same semantics as a cluster, and
  listens on localhost.
- A container image and **Testcontainers modules** for each client language as it ships.
- A **test clock**: in dev mode, tests can advance the group clock, so delays, leases, back-off,
  ageing and suspension are tested instantly.
- The server can be embedded in-process in Go tests.
- An **example application** for every built-in profile, each run in CI.

### 13.6 Migration

- **S6 recipes**, each with an example app and a dual-run procedure, for the queues AI teams use
  most: Celery (on Redis or RabbitMQ), BullMQ, RabbitMQ work queues, Postgres queues (River,
  Graphile Worker, `SKIP LOCKED` tables) and SQS. Each maps the source's semantics to a profile.
- **S8**: a Kafka protocol gateway for produce, fetch, metadata and consumer groups, mapped onto
  topics and cursor subscriptions, with a published compatibility matrix; Kafka import that keeps
  offsets as positions; continuous Kafka mirroring for dual runs; draining an SQS queue into a
  topic.

## 14. Testing models

Every check names the slice that introduces it.

- **Fault-injecting filesystem** (S1): all disk I/O goes through an interface. On a simulated crash
  the test implementation discards unsynced data, directory entries and renames, tears the last
  write at any byte, reorders unsynced writes, and can fail fsync. A mutation that removes any
  directory fsync must fail the suite.
- **Producer history check** (S1): every acknowledged record is present after recovery, once per
  producer sequence and idempotency key, in per-shard order.
- **Consumer history check** (S2, extended per slice): no acked message is redelivered under
  at-least-once; every owed message reaches a terminal state or is still owed; at most one ack is
  accepted per token; nothing is ever delivered from beyond the visibility frontier; at-most-once
  never delivers twice; per-key and per-shard order hold, and **processing intervals for the same
  key or shard never overlap**, measured from handler start to finish in the test harness, except
  recorded operator requeues (S4);
  prepared messages change state only by the transaction's decision, and exactly-once outputs
  appear once with no aborted or open records read (S7).
- **AI checks** (S5): the ageing bound of 8.2; a small tenant's first message beside a tenant with
  a million queued messages is dispatched within the service bound of 8.1; the pool bound of 8.3 holds across
  coordinator failover; throttled nacks use no attempts until `max_throttled`; supersede happens
  only after the superseding record is visible; cancelled and superseded messages are never
  redelivered except by an explicit seek; durable leases survive elections.
- **Cluster fault suite** (S3): partitions, pauses, kill -9, clock skew, corruption on one replica
  and on a majority, elections during assignment changes, pool coordination and transactions (S7),
  and, from S4, memory-group quorum loss. The history checks run on every test.
- **Model checking** (S3): TLA+ models of membership changes with whole-replica rebuild, and of
  token fencing with durable leases and epochs.
- **Conformance suite** (S2 onward): both sides of every rule active in that slice, pairwise
  combinations of setting values, and a high-risk list kept in the repository.
- **Upgrade, downgrade and restore** (S3 onward for upgrades; S6 for downgrade and restore drills).

## 15. Performance and resource targets

Reference machine: 8 vCPUs, 32 GiB memory, local NVMe, 10 Gbit network. Messages are 1 KiB,
uncompressed, quorum acks, three replicas in the cluster column.

| Measure | Single node (S2) | Three-node cluster (S3, confirmed in S6) |
| --- | --- | --- |
| Produce throughput | 150 MB/s | 150 MB/s cluster ingress |
| p99 produce ack at half that load | 5 ms | 10 ms |
| Work-queue throughput (dispatch plus ack) | 50,000 messages/s | 100,000 messages/s |
| p99 produce-to-delivery latency at half load | 15 ms | 25 ms |
| Resident memory, default node profile | Within the 9.8 formula: at most 6 GiB with 1,000 connections, and at most the formula's value at 10,000 connections | Same, per node |
| Restart to serving with 100 GB of data | 30 s | Leader failover within 2 s |

**Scale scenarios** that must stay within the memory bound: a 100-million-message backlog; 10
million delayed messages (S2); 1 million distinct pending keys (S4); 100,000 tenants with pending
work (S5); idempotency keys at 50,000 per second for a full 15-minute window (S2).

A slice exits only when it meets its targets. A target changes only through a revision of this
document, with the benchmark evidence.

## 16. Slices and exit bars

| Slice | Exit bar |
| --- | --- |
| **S1 Storage spine** | Zero acknowledged loss across 1,000 simulated crashes on the fault-injecting filesystem, plus a kill -9 run on a real filesystem; the directory-fsync mutation test fails as required; producer history check passes; every on-disk structure carries a format version; single-node produce targets met. |
| **S2 Work queue** | Consumer history check for at-least-once and at-most-once; conformance suite for S2 rules; memory bound with the 100-million backlog, 10 million delayed messages and the idempotency scenario; single-node work-queue targets; upgrade of S1 data; Go client and HTTP produce, pull, ack, nack and extend. |
| **S3 Replication** | Cluster fault suite with both history checks and zero acknowledged loss; a node with a skewed clock is refused leadership in pre-vote and group time does not move; whole-replica rebuild and majority-corruption halt; TLA+ models checked; leader failover within 2 s; cluster produce targets; a single S2 node grows in place into a three-node cluster under load, with its data. |
| **S4 Ordering and streams** | Per-key, per-shard and cursor order hold under the cluster fault suite; a poison record at position 57 of a cursor batch is the only record dead-lettered; push credits are never exceeded; control-channel notices arrive within one second at p99; memory bound with 1 million pending keys. |
| **S5 AI work queue** | AI checks under the cluster fault suite; token-weighted dispatch over 5 minutes, with mixed message sizes, within the 8.3 bound for a 10,000-per-minute pool with a burst of 1,000; 100 throttled nacks leave attempts at zero; memory bound with 100,000 tenants; durable leases survive 10 consecutive elections without redelivery; Python client passes the conformance suite. |
| **S6 Production readiness (version 1.0)** | Auth and TLS tests; token revocation within one second; backup and restore drill in CI; automated node-replacement test; rolling upgrade and patch downgrade; every alert fires in its fault scenario; TypeScript client conformance; an example app for every built-in profile and every migration recipe passes in CI; all section 15 targets met and published with the harness. |
| **S7 Guarantees and integrations** | Exactly-once consumer history check under the fault suite; a retry inside an open transaction is never duplicated; webhook egress-guard tests including DNS rebinding, redirects and metadata addresses; blob lifecycle tests; batch tokens survive elections and reject forgery and expiry; Java client conformance. |
| **S8 Ecosystem** | Kafka client test suites pass for the supported subset; matrix published; Kafka import preserves positions; SQS drain verified. |
| **S9 Scale and storage** | Splits under load preserve order in the consumer history check; fault suite with object-store faults; per-class cost and latency targets, set by the S9 design, met. |

## 17. Annex: unscheduled

These came out of the research but are outside the current scope. Each needs its own design
before it is scheduled. The changelog maps every research requirement to this annex or to the
sections above.

- **Data governance**: keyed delete markers reaching every copy, erasure receipts, data-class
  labels and masking, field-level access by token scope, gated topics for PII and
  prompt-injection scanning, shadow replay, a compliance audit log for EU AI Act record keeping,
  and per-subject crypto-shredding (the `encryption_key_id` field is reserved for it).
- **Agent protocols**: an MCP server, an A2A transport, and a callback-ingest endpoint.
- **Streams and key features**: resumable token streams, key reads and latest-value-by-key, forking
  a key.
- **Data-pipeline features**: key-consistent sampling, changed-field delivery, a backfill profile
  with pool priority, and an Iceberg table view.
- **More AI dispatch controls**: per-key debounce and coalesce, per-key quotas, nested quota pools
  and budgets, a saturation gate, hop limits, and a step helper.
- **Platform**: bulk profile rollout across subscriptions, native encryption at rest, multi-region
  replication, wildcard subscriptions, connectors, shard merge, AMQP and MQTT gateways.

## 18. Decisions

### 18.1 Decided by the owner

- One platform that addresses the pain points of streaming platforms and message queues, for real
  users in production, consumer-oriented with every behaviour chosen through profiles, and
  solving real problems from AI engineering.
- Written in Go; the TypeScript prototype is removed. The project is named Tributary.
- Narrow the AI scope to the work queue for LLM and agent traffic, move governance, agent
  protocols and the other extras to the annex, and deliver in vertical slices with an early
  replication slice (approved 2026-10-19).

### 18.2 Decided by the designer, open to override

- Version 1.0 is S1 to S6; the slice order in section 3.
- `go.etcd.io/raft`, Pebble and CEL; one Pebble instance per node holding every group's Raft log
  and state under group-prefixed keys; Raft from S1 with one voter.
- Eight data groups per node with least-loaded placement until S9; dead-letter shards co-located
  with their source shards.
- Producer IDs that embed the allocating group; optional producer names for fencing.
- Replicated topics with quorum acks only until S4.
- Two subscription kinds, message and cursor.
- The group clock and absolute deadlines; IDs that include the leader term.
- Leases are durable only when asked for, when the initial lease exceeds 5 minutes, or for
  batches.
- Whole-replica rebuild instead of per-entry repair.
- Retention as a floor with a maximum owed age; suspension after 7 days with automatic resume and
  deletion after 30 days suspended; cap pressure skips rather than dead-letters.
- Single-step redelivery to find poison records in cursor subscriptions.
- Producer IDs allocated in ranges by data groups, plus a sessionless mode.
- Pause without a metadata-lease hard stop, so a metadata hiccup never halts dispatch.
- The numeric targets in section 15 and the node-profile defaults in 12.6.
- Object storage as the one optional external dependency.
- Apache-2.0; binary and CLI `trib`; header prefix `trib-`; Go module
  `github.com/nileshkr9919/tributary`.

### 18.3 Left to the builder

- Exact encodings within the layouts of 9.11 and 13.1, checkpoint cadence within the node profile,
  and internal package structure.
- The benchmark harness and the CI layout.
- CLI ergonomics beyond the command list in 13.4.
