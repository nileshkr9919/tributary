# Tributary design changelog

History, reviews and traceability for
[the design](2026-10-19-unified-messaging-platform-design.md). The design itself describes only the
current state.

## Revisions

| Revision | Date | Change |
| --- | --- | --- |
| 1 | 2026-10-19 | First design from the seven platform research reports: one log, queue and stream modes. |
| 2 | 2026-10-19 | Owner asked for full consumer choice through profiles: settings model, profiles, three guarantees, webhooks. Resolved [the first review](2026-10-19-unified-messaging-platform-review.md): write-ahead log split, delivery tokens, idempotency placement, backlog limits, group classes, transaction read isolation, versioned formats. |
| 3 | 2026-10-19 | Resolved a second verification pass: prepared-ack fencing, torn-write detection, memory-group election state, tentative state on abort, cap pressure, per-key index, rule additions. |
| 4 | 2026-10-19 | Owner asked for AI-era problems: three AI research reports and 54 requirements mapped into the design. |
| 5 | 2026-10-19 | Resolved verification of the AI additions: durable leases, pool coordination, composite indexes, ageing overdue lane, kill-switch confirmation, governance fixes. Archived at [archive/2026-10-19-design-r5.md](archive/2026-10-19-design-r5.md). |
| 6 | 2026-10-19 | Clean rewrite after [the revision 5 review](2026-10-19-unified-messaging-platform-review-r5.md), on the owner's approval of its recommendation. Changes are listed below. |

## Revision 6 changes

**Scope and sequencing**

- The AI scope is narrowed to the work queue for LLM and agent traffic. Governance, agent
  protocols, token streams, key reads, sampling, backfill, the Iceberg view, debounce, coalesce,
  per-key quotas, nested pools, budgets, the saturation gate, hop limits and the step helper move
  to an unscheduled annex.
- Milestones M1 to M5 become slices S1 to S9. Version 1.0 is S1 to S6. Replication is S3, before
  ordering and AI features.

**Decisions now made**

- The Raft library (`go.etcd.io/raft`), state store (Pebble) and filter language (CEL).
- Two subscription kinds, message and cursor, replace the cumulative-ack setting. The rules drop
  from 24 to 12.

**New material**

- A message state machine, an API sketch, a state-store key schema, cluster dispatch routing, an
  operability section, numeric performance targets, and a numeric definition of a dispatch round.

**Fixes from the revision 5 review**

| Finding | Resolution in revision 6 |
| --- | --- |
| Supersede before the new record is visible | Supersede fires only on visibility (8.5) |
| Gates and the last stable offset bypassed | Visibility frontier on every read path; nothing acts on invisible messages (7.6, 7.7); gates moved to the annex |
| Transactional producers lose deduplication | Tentative per-transaction state (6.6) |
| Prepared inputs reach two terminal states | Only the decision moves a prepared message (7.6) |
| Durable leases by default | Durable only on request, for initial leases over 5 minutes, or for batches (7.7) |
| Wall-clock time | Group clock with absolute deadlines and skew step-down (5.4) |
| Memory groups cannot recover | Learner rejoin, wait during upgrades, recreation on quorum loss (9.6) |
| Message IDs reused | IDs include the leader term; committed-status gap recovery for every producer (5.3, 6.4) |
| Cursor crash loops dead-letter healthy records | Single-step redelivery after a crash (7.14) |
| Delete markers lost | Feature moved to the annex |
| Unbounded state | Ledger and dedupe compaction, idle frontier eviction, deletion after 30 days suspended (8.1, 8.3, 7.11) |
| Broadcast loses messages after an hour | Named members follow inactivity and resume (7.15) |
| Cap pressure works against itself | Skips with recorded ranges, only where space is freed (7.11) |
| Index build unspecified | Key index always on; composite index build with `applying` (7.9, 8.1) |
| Requeue semantics | Redelivery record on the source shard, pinned copy, defined order (7.10) |
| `agent-session` freezes a session | Dead-letters instead of holding; supersede also replaces held messages (7.4, 8.5) |
| Step helper overclaims | Removed; effect-key semantics stated precisely (8.6) |
| Kill switch couples data to metadata | Pause without a metadata-lease hard stop (8.5) |
| Cluster dispatch unspecified | Section 10 |
| Settings model hard to reason about | Two kinds, required profile, visible clamping, profile parameters (7.1, 7.4) |
| Retention semantics unclear | Retention is a floor with a maximum owed age; auto-resume; loss table (7.11, 7.12) |
| Exit bars lack numbers | Section 15 and section 16 |
| Raft choice and corruption | `go.etcd.io/raft`; whole-replica rebuild (9.2, 9.5) |
| Developer experience and migration | Test clock, Testcontainers, examples, recipes for Celery, BullMQ, RabbitMQ, Postgres and SQS (13.5, 13.6) |
| Production operations missing | Section 12 |
| Producer sessions in metadata | ID ranges from data groups and a sessionless mode (6.1) |
| Epoch writes at every election | Leader term in the token; epochs only for assignment and seeks (7.7) |
| Batch tokens as bearer credentials | Signed, expiring, with consume permission (8.8) |
| Security before auth | Localhost-only until S6 unless explicitly overridden (11.1) |
| Claim-check needs S3 | Stated as the one optional external dependency (2, 9.9) |

## Revision 6 reviews

Two fresh reviews read revision 6: a builder check of S1 to S3 and a correctness review. All
findings are resolved in revision 6.

**Builder check: decisions recorded**

| Question | Decision |
| --- | --- |
| One store per group or per node | One Pebble instance per node holding every group's Raft log and state under group-prefixed keys; one fsync stream per node (9.4, 9.11) |
| Does S1 run Raft | Yes, with one voter, so a single node grows in place into a cluster (3, 16) |
| When the key index starts | Written from S1 (7.9) |
| Groups and placement before S9 | Eight groups per node, least-loaded placement, leadership balancing (9.1) |
| Cross-group dead-lettering | Dead-letter shards co-located with source shards (7.10) |
| Memory groups and weak acks | From S4; quorum-only and durable subscriptions until then (3) |
| Producer identity and fencing | IDs embed the allocating group; optional producer names for fencing (6.1) |

**Correctness review: fixes**

| Finding | Resolution |
| --- | --- |
| A skewed leader moves group time forward permanently | Group time advances by measured elapsed time; skewed candidates refused in pre-vote (5.4) |
| Ordinary crashes classed as corruption | Segments and state validated only to their checkpoint; the rest replayed from the log (9.5) |
| Ordering broken by elections and shard moves | Ordered subscriptions commit lease grants; revocation waits for acknowledgement or deadline; history check verifies non-overlapping processing (7.7, 10, 14) |
| Rebuilt replica reuses its Raft ID | Fresh replica ID (9.5) |
| Held and paused messages vs retention | Held messages dead-lettered at the maximum owed age; cap pressure relocates pinning records (7.11) |
| Redelivery records outside the state model | Keyed by dead-letter copy ID; redelivery queue before the frontier; same-group reference count (7.10, 9.11) |
| Quota settlement lost on failover; bound without numbers | Settlement outbox; seen-ID expiry; chunk size and numeric bound (8.3) |
| Subscription coordinator state | Persisted under `a/`; assigned through metadata; shard-move protocol; single-step crash counting (10, 7.14) |
| Sessionless producers and weak acks | Sessionless produces always use quorum acks (6.1) |
| Memory target unreachable with defaults | Memory formula with capped terms; revised defaults and target (9.8, 12.6, 15) |
| "Dispatch round" self-contradictory | Dispatch rate and a service bound in milliseconds (8.1, 8.2) |
| State machine incomplete; supersede misses retrying heads | Completed transitions; supersede covers pending, delayed, leased and held (7.6, 8.5) |
| Broadcast and overflow defaults inconsistent | Aligned (7.2, 7.12) |
| Rule-excluded settings inherited | Removed from effective settings (7.5) |
| Restore drill too weak | Consumer history check and reconciliation rules (12.5) |

## AI research requirement traceability

Where each requirement from the AI research reports lives in revision 6.

| ID | Requirement | Where |
| --- | --- | --- |
| I1 | Token-weighted rate limits | 8.3 |
| I2 | Shared quota pools | 8.3 (single level); nesting in the annex |
| I3 | `throttled` nack, no attempt | 8.4 |
| I4 | `quota_exhausted` pauses and alerts | 8.4 |
| I5 | Priority bands with ageing | 8.2 |
| I6 | Fair dispatch per tenant | 8.2 |
| I7 | Saturation-gated dispatch | Annex |
| I8 | Progress-carrying leases | 8.6 |
| I9 | Operation ID across attempts | 8.6 |
| I10 | Resumable token streams | Annex |
| I11 | Cancellation | 8.5 |
| I12 | Provider batches | 8.8 (S7) |
| I13 | Usage reporting | 8.7 |
| I14 | Budgets and cost-capped retries | 8.7 (cost cap); budgets in the annex |
| I15 | Claim-check | 8.8 (S7) |
| I16 | Fallback routing | 8.8 (S7) |
| A1 | Supersede | 8.5 |
| A2 | Debounce and coalesce | Annex |
| A3 | Per-key concurrency and fairness | 8.2; per-key concurrency in the annex |
| A4 | Cancel by ID or key | 8.5 |
| A5 | Long leases with progress | 7.7, 8.6 |
| A6 | Durable correlation inbox | Request-reply (S7) |
| A7 | Cancellable delayed messages | 8.5 |
| A8 | Effect keys, webhook `Idempotency-Key` | 8.6, 7.8 |
| A9 | Do-not-retry nack | 7.3 (`permanent`) |
| A10 | Large payload offload | 8.8 (S7) |
| A11 | Read and replay one key | Annex |
| A12 | Fork a key | Annex |
| A13 | Trace pass-through | 11.3 |
| A14 | Delivery audit | Annex (compliance audit); operational events in 12.4 |
| A15 | Hop limit | Annex |
| A16 | Per-key quota | Annex |
| A17 | Kill switch | 8.5 (pause) |
| A18 | Cost annotations on ack | 8.7 |
| A19 | Callback ingest | Annex |
| A20 | `agent-session` profile | 7.4 |
| D1 | Latest-wins coalescing | Annex |
| D2 | Delete markers to every copy | Annex |
| D3 | Shared rate budget; 429 pauses | 8.3, 8.4 |
| D4 | Backfill beside live traffic | Annex |
| D5 | Changed-field delivery | Annex |
| D6 | Iceberg table view | Annex |
| D7 | Key-consistent sampling | Annex |
| D8 | Claim-check with separate retention | 8.8 (S7) |
| D9 | Shadow replay | Annex |
| D10 | Data-class labels | Annex |
| D11 | Field-level access | Annex |
| D12 | Access audit log | Annex |
| D13 | Verifiable erasure | Annex |
| D14 | Provenance headers | Annex |
| D15 | Gated topics | Annex |
| D16 | Server-stamped tenant | 11.1 |
| D17 | Freshness metrics | 11.3 (oldest owed age) |
| D18 | Latest value by key | Annex |
