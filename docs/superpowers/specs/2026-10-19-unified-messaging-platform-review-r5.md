# Review of the Tributary design, revision 5

Reviews [the design at revision 5](archive/2026-10-19-design-r5.md). Three
independent reads, merged and deduplicated:

- the designer's own read;
- a distributed-systems correctness review that re-derived every claim, ignoring the
  resolution tables in section 14;
- a buildability and product review.

Each finding was checked against the spec text.

## Verdict

**Not ready to turn into an implementation plan.** The core idea holds and fits the owner's ask:
one replicated log, with every consumption choice made per subscription through pinned,
immutable profiles. The fencing, durability and memory reasoning is much stronger than in
revision 1.

But five revisions of patching have produced three structural problems, and more patching will
not fix them:

1. **Scope.** M1 is several milestones of work. The design maps all 54 AI research requirements
   into scope, including a compliance product and two agent protocols the owner did not ask for.
2. **Sequencing.** The riskiest work comes after M1 freezes the on-disk and token formats that
   M2 must read: replication, fencing under elections, and cross-node dispatch.
3. **Interactions.** Features added in different revisions now collide where they meet:
   supersede, gates, the last stable offset, backlog overflow, prepared inputs and delete
   markers. Each feature is sound on its own; the bugs are at the seams.

The status line in revision 5 says "verified". That is inaccurate: no review had read
revision 5 before this one.

## Recommended path

Rewrite the spec as a clean revision 6 instead of patching revision 5.

1. **Cut milestones into vertical slices**, each usable and testable:
   1. storage spine and history checkers;
   2. `work-queue` end to end;
   3. a 3-node replication spike;
   4. per-key ordering and `event-stream`;
   5. the AI work-queue wedge.
2. **Narrow the AI scope to one wedge, "the work queue for LLM and agent traffic"**: quota
   pools, throttle-aware nacks, priority and fairness, supersede and cancel, durable leases with
   checkpoints and effect keys. Move these to an unscheduled annex: governance (7.6), the MCP
   and A2A protocols, token streams, fork, sampling and the Iceberg view.
3. **Choose the Raft library now**, and replace per-entry corruption repair with whole-replica
   rebuild.
4. **Add the missing planning artifacts**:
   - one message state machine;
   - an API sketch;
   - a state-store key schema;
   - a cluster dispatch routing model;
   - an operability section;
   - numeric exit targets.
5. **Fix the interaction bugs below** as part of the rewrite.
6. **Move the patch history** (sections 14 to 14.3) to a separate changelog, so the spec
   describes only the current design.

## Critical

| # | Finding | Failure | Fix |
| --- | --- | --- | --- |
| C1 | **M1 is a programme, not a milestone** (10). It bundles a new storage engine, every distribution, ordering and ack model, 24 rules, profile rollout, full HTTP parity, and every AI dispatch feature. | Nothing is usable or testable until all of it is done. | Vertical slices as above. |
| C2 | **The riskiest work comes after formats freeze** (10, 6.9). Replication, election fencing and cross-node dispatch arrive in M2, but M1 freezes the token, ID and write-ahead-log formats that M2 must read. | M2 forces a format rewrite. | Run a replication spike in slice 3, before adding consumer features. |
| C3 | **Scope dilutes the ask, and the AI audience cannot use M1** (2, 5.14, 7.6, 8.1). Compliance, protocol bets and table views were not requested. The AI features ship with only a Go client; Python and TypeScript arrive in M2. | The design becomes a superset of Kafka, SQS, an LLM gateway and a governance product. The research's lesson from Pulsar applies: ecosystem gravity beats feature count. | Name one wedge. Move the rest to an annex. Ship a thin Python client with the AI slice. |
| C4 | **Production operations are missing** (6, 7.4, 10). There is no backup or restore. There is no way to add, remove or replace a node before M4. There are no health endpoints and no alerting mechanism (the word "alert" appears 16 times). There is no downgrade path, encryption at rest, sizing guide or deployment artifact. | The first dead disk in production has no procedure. | Add an operability section and put it in M2's exit bar. |
| C5 | **Supersede fires before the new record is visible** (5.14.3, 5.6, 7.6). The notice goes out at append time. | The old run is cancelled for good, but the new message never arrives: its leader fails before commit, it is an aborted transactional output, the gate rejects it, or it waits behind an open transaction. Both messages are lost. | Supersede only once the new record is committed, below the last stable offset and approved by the gate. Or make the superseded mark provisional. |
| C6 | **Gates and the last stable offset can be bypassed** (7.6, 5.6, 5.10). Overflow and cap pressure act on records a subscription cannot yet see. Key reads, lookup, the Kafka gateway and the MCP server are not said to respect gates. | A stalled gate plus the default `dead-letter` overflow copies unapproved PII or prompt-injection records into a readable dead-letter topic. Uncommitted transactional records can leak the same way. | Overflow and cap pressure never act at or above a subscription's visibility frontier. List every read path and require each to enforce gates and the last stable offset. |
| C7 | **Transactional producers lose deduplication** (5.6, 5.2). Sequence and idempotency state applies only at commit. | A retry inside an open transaction is appended twice and both copies commit. A pipelined batch is rejected as a gap. | Keep tentative sequence and idempotency state per open transaction; promote it on commit, discard it on abort. |
| C8 | **Prepared inputs can reach two terminal states** (5.6, 5.10, 5.12, 7.6). Only supersede, coalesce and cancel are fenced. Overflow, cap pressure, suspension, a backwards seek and delete-marker removal are not. | A prepared input is dead-lettered by overflow, and then the commit acks it too. | One rule: a prepared input is immune to every disposition until its transaction is decided. List prepared inputs as unreleasable under cap pressure. |

## Major

| # | Finding | Fix |
| --- | --- | --- |
| J1 | **Durable leases by default** (5.5, 5.6, 12). A lease is durable when its maximum exceeds 5 minutes, and the default maximum is 12 hours. So every default dispatch is committed first, contradicting the throughput trade-off in 5.6 and 12. Durable tokens also carry an epoch that elections bump. Found by all three reads. | Make leases durable only when explicitly set or when `lease.initial` exceeds 5 minutes. Validate durable tokens by lease ID. Have `audit: deliveries` force durable leases. Benchmark the chosen path. |
| J2 | **Wall-clock time leaks in** (5.5, 4.3, 5.11). `deliver_at`, TTL, schedules, budget periods and `produced_at` use wall clocks. Re-arming full durations at each election pushes long delays later. | Use a replicated hybrid logical clock. Bound how far a leader's clock may lead its quorum. Carry elapsed time across elections with committed time heartbeats. |
| J3 | **Memory groups cannot recover from correlated restarts** (6.1). Old replica identities remain voters that never return. | Recreate memory groups from metadata when quorum is lost. Make rolling upgrades wait for re-promotion. |
| J4 | **Leader-ack and no-ack topics reuse message IDs** (6.2, 4.3). A lost leader-acked record's offset is reassigned to another record. | Add the leader term to the ID, or return IDs only after commit. Specify gap recovery for every producer. |
| J5 | **The cursor crash-loop rule dead-letters healthy records** (R1). The committed position is the start of a batch, not the poison record, and graceful rebalances also count as crashes. | After the first failure, redeliver one record at a time, or have clients report the in-progress offset. Count only crashes. |
| J6 | **Delete markers can still be lost** (7.6, R2, R19). Supersede, cancel by key, `on_exhausted: drop`, per-key quota dead-lettering and dead-letter-full fallback can each drop one. Markers are delivered at-least-once to at-most-once subscriptions, but R2 gives those no ack path. | Make markers immune to supersede, cancel, drop and skip. Define an ack path for markers on at-most-once subscriptions. |
| J7 | **Unbounded state** (7.6, 5.14.4, 5.14.1). A suspended subscription blocks erasure indefinitely and grows the marker log. Other gaps: the grant ledger, the ack-ID dedupe set and high-cardinality frontiers have no compaction. Erasure misses checkpoints, idempotency entries, index keys, the log tail and snapshots. | Remove records once the marker is durable, independent of delivery. Expire suspended subscriptions. Compact ledgers and dedupe sets. Widen erasure scope. |
| J8 | **`broadcast` loses messages after a one-hour absence** (5.3, 5.10). `member_ttl` (1 hour) removes members while 5.10 says named members are suspended after 3 days. A returning member then starts at `latest`. | Apply `member_ttl` only to unnamed members. Named members follow `inactivity_ttl` and resume with a recorded gap. |
| J9 | **Cap pressure works against itself** (5.10, 5.9, 6.6). The default overflow copies data into dead-letter topics, which consume a finite reserve and never expire. | Prefer `skip` with a recorded range under cap pressure. Target only segments whose release frees space. |
| J10 | **Index build and backfill are unspecified** (5.8, 5.14.1). A new per-key or `llm-requests` subscription with `start: earliest` finds no index entries for existing data, and keyed delete has no index. | Define index builds with an `applying` state and garbage-collect unused indexes. Consider keeping the key index always on. |
| J11 | **Requeue semantics across topics** (5.9). The redelivery record points into a dead-letter topic in another group. Ordering, purge of the copy, and exactly-once tokens are undefined. | Define requeue as entering the key's queue at its head, pin or reference-count the copy, and define token handling. |
| J12 | **`agent-session` can freeze a user's session** (5.4, 5.8, 5.14.3). The profile uses `on_exhausted: hold` with per-key ordering. A failed run parks the session key until an operator acts, and supersede does not apply to a held message, so the user's next messages queue behind it indefinitely. | Use `dead-letter` for `agent-session`, or let a new message supersede a held one. |
| J13 | **The step helper overclaims** (5.14.7). It "runs `fn` once per operation". A crash after `fn`'s side effect but before the checkpoint is saved runs it again. | State that `fn` runs at least once and its stored result is returned once saved. Require `effect_key` for external effects inside `fn`. |
| J14 | **Kill-switch metadata leases couple data to metadata availability** (5.14.6). Every leader pauses dispatch when its metadata lease is over one second old. A metadata-group hiccup then becomes a cluster-wide consumption outage. | Pause only scopes with an active kill switch, or make the hard-stop mode opt-in. |
| J15 | **The cluster dispatch path is unspecified** (5.14.1, 6.5, 5.7). Nothing says how a pull that reaches any node gathers messages from up to 256 shard leaders, or how band summaries and pool chunks fit into that path. | Specify one routing model, with a sequence diagram. |
| J16 | **The settings model is hard to reason about** (5.3, R1 to R24). Many rules exist because cumulative ack is really a different kind of subscription. Defaults depend on other settings and are silently clamped by topic. `llm-requests` uses "required profile parameters", which are never defined. | Add `kind: cursor` or `message`. Require a profile at create time. Report clamped values in `trib sub show`. Define profile parameters. |
| J17 | **Data-release semantics are unpredictable** (5.3, 5.10, 6.6). It is unclear whether retention is a floor or a ceiling for pinned data. A 3-day inactivity suspension catches long weekends, with no automatic resume. Default loss paths are scattered across sections. | Define retention in one sentence. Resume a suspended subscription automatically when a consumer attaches. Add one table of every way a message ends unprocessed, with its default and alert. |
| J18 | **Exit bars lack numbers** (9, 10). "The stated bound" for memory is never stated. There are no throughput or latency targets. "One dispatch round" is undefined but used in four bounds. | Numeric targets on a reference machine, and "meet, then publish". |
| J19 | **Raft choice and corruption recovery** (6.4, 13). The library choice constrains the frozen log format, and per-entry repair is hard to model-check. | Choose now; go.etcd.io/raft is the most adaptable. Use whole-replica rebuild. |
| J20 | **Developer experience and migration** (8). Only Go can embed the server. There is no test clock, no examples, and migration covers only Kafka and SQS, while AI teams mostly run Celery and Redis, BullMQ, RabbitMQ or Postgres queues. | A dev container image and Testcontainers module, a test-clock API, one example per profile, and migration recipes for those queues. |

## Minor

- **Contradictions left by patching.** Section 3 says `quota_exhausted` suspends; 5.14.5 says it
  pauses. The 5.3 fairness default contradicts the M1 rule in 5.14.2. 5.12 says "subscription
  epoch" where 5.5 defines per-shard epochs.
- **The lease maximum is not clamped.** Default subscriptions on `rpc-inbox`, `token-stream` or
  `ephemeral` topics exceed the topic's retention. `provider-batch` does not say whether its
  `max_in_flight` is per member or per subscription.
- **Producer sessions in metadata** may explode under serverless producers. Allocate ID ranges
  from data groups, and offer a sessionless mode.
- **The M1 exit bar references later features.** Tag each rule and check with its milestone.
- **Epoch bumps on every election scale with subscriptions times shards.** Put the group's term
  in the token instead.
- **Small undefined cases.** Debounce has no maximum wait. Seek-by-key under cumulative ack
  matches no rule. It is undefined whether a backwards seek redelivers terminal states.
- **Batch tokens are bearer credentials.** They need signing, an expiry and a consume
  permission check.
- **Security timeline.** M1 should listen on localhost only by default. Specify the token
  lifecycle in M2 and rate-limit callback ingest.
- **The claim-check needs S3 in production**, which contradicts the anti-goal of needing no
  second system. State the exception or offer a local replicated blob store.
- **Filter language.** Choose one now; CEL is the obvious Go choice.
- **Not fully verified.** Repair of corruption in segments or the state store below the truncated
  log is unspecified. The pool coordinator's failover wait is safe only if dispatchers start their
  chunk timer when they send the request.
