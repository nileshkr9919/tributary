# Database-backed queues and cross-cutting pains

Research date: 2026-10-19. Scope: (A) queues built on Postgres, SQLite and Redis, plus durable-execution engines; (B) complaints that show up across every queue and streaming product.

**Method and limits.** Sources are Hacker News threads, maintainer blogs, vendor blogs (marked), GitHub discussions and one benchmark repo. Reddit could not be read (fetches are blocked here and searches returned no Reddit threads), so the "how often" column in Part B rests on HN, blogs and GitHub. Some HN dates are approximate months. Severity scores and frequency labels are my ratings, not the sources'.

## Part A: database-backed and durable-execution queues

### Architecture

**Postgres SKIP LOCKED queues** (pgmq, Graphile Worker, River, Oban, Solid Queue, pg-boss, Procrastinate, PGQueuer). A `jobs` table; workers run `SELECT ... FOR UPDATE SKIP LOCKED` to claim a row, update its state, and later mark it done or delete it. Wake-up is polling, often shortened with `LISTEN/NOTIFY`. Oban's author said the "advanced features we wanted were available in PostgreSQL," which is why he built on it rather than Redis [31].

**Append-only log on Postgres** (PgQue). Rather than updating rows, PgQue appends events and recycles tables with `TRUNCATE`. Its author calls it "an event-streaming tool, not an MQ," "closer to Kafka topics than to a job queue" [2].

**SQLite queues** (goqite, liteq, Huey). Same pattern, single file, single writer. goqite's README reports about 18,500 messages/s with one producer/consumer and about 12,500 with 16 of each, because "SQLite only allows one writer at a time, so the parallelism just creates write contention" [26].

**Redis queues** (Sidekiq, BullMQ). Lists and sorted sets in Redis, with Lua scripts for atomic moves. Fast and familiar, but correctness depends on Redis configuration.

**Durable execution** (Temporal, Inngest, Trigger.dev). The engine persists a history of every step; workflow code replays deterministically after a crash. Queues become an internal detail.

**Kafka alternatives on databases/object stores** (WarpStream, AutoMQ, Bufstream, RudderStack's Postgres engine). These keep the Kafka API but move storage to S3 or Postgres, mainly to cut cross-AZ cost and remove stateful brokers.

### Strengths

- **Transactional enqueue.** The reason most often given. Brandur (River): a Redis job can be "picked up for work, but the transaction that emitted it isn't yet committed," and a job "emitted from a transaction which then rolls back" will "fail every subsequent retry, pointlessly eating resources" [4]. HN commenter dralley: "leveraging the same transactions as your business logic solves many many issues" [9].
- **One fewer system.** "No ElastiCache, no Redis" [4]. HN user Justsignedup: "Having to just manage a database is far easier infra wise than a complex system for small rails operations" [6]. rietta moved from Sidekiq back to a DB queue because "the juice [was] not worth the squeeze" [6].
- **Queryability.** RudderStack picked Postgres over Kafka partly because "Having a SQL-like query interface to the persisted event helped a lot" with debugging [22]. HN users keep DLQs in Postgres for "High visibility, easy requeue, durable store" [8].
- **It goes further than people assume.** HN user wordofx reports "~70k messages/second average" on one r8g instance at ~20% CPU using a partitioned table [1]; philipbjorge ran "millions of jobs per day" on SKIP LOCKED [5]; Sidekiq's author counters that he has "dozens of customers doing 1b+ jobs/day" on Redis [6].

### Pain points table

| System | Pain | Evidence (quote + link + date) | Severity 1-5 | Root cause |
|---|---|---|---|---|
| Postgres SKIP LOCKED | Dead tuples outrun autovacuum; table and index bloat | "Dead tuples accumulate faster than autovacuum can clean them"; tables grow "to tens of gigabytes when live data is only a few megabytes" — Richard Yen [3], 2026-05-04. "The vacuum pressure is real" — mind-blight, HN [2], ~Apr 2026 | 4 | Every claim/complete is an UPDATE under MVCC; the partial index on `status='pending'` churns constantly |
| Postgres SKIP LOCKED | Polling hammers the DB as depth grows; scaling workers makes it worse | "skip locked technique + polling caused massive DB perf issues as the queue depth grew ... Scaling the workers sometimes exacerbates the problem because you run into connection limits" — mind-blight [2] | 4 | No push delivery; each worker is a DB connection; the "what's pending" query is the hot path |
| Postgres SKIP LOCKED | Queue depth, not throughput, is the danger | "if you have more than 5k or 10k jobs in the table at any given time, you're in dangerous territory" — nightpool, HN [5], 2023-04-11 | 3 | SKIP LOCKED still scans past locked/done rows; backlog makes every claim slower |
| Postgres SKIP LOCKED | Long-held xmin stalls everything | Under a held xmin, SKIP LOCKED queues ran "55-87%" CPU and some "did not catch up after the blockage cleared" — PgQue author benchmark [2]; River executes jobs outside the transaction because of "the risks of long-open Postgres transactions" [5] | 4 | Any long transaction on the same instance blocks vacuum of the queue table |
| Postgres SKIP LOCKED | Lock pressure under many workers | `LWLock:MultiXactSLRU` and `ProcArrayLock` waits; guidance is "fine under 100 concurrent workers with simple jobs" [3] | 3 | Row locks become MultiXact entries; SLRU lookups serialize |
| Postgres SKIP LOCKED | Shares the OLTP database (SPOF, connection competition) | "you built a SPOF"; polling causes "continuous connection churn, which can deplete your pool" — dev.to opinion piece [16]; "The db is already so full on a normal Rails application that adding more reads doesn't make sense" — zanellato19 [6] | 3 | Queue load and user traffic contend for the same disk, connections and vacuum budget |
| Postgres SKIP LOCKED | No fan-out, no replay, no streaming | "I've yet to hear of an implementation of streams in Postgresql" — adamcharnock [1]; "There's no connector ecosystem for Postgres like there is for Kafka" — Morling [21], 2025-11-03 | 3 | Rows are consumed and deleted; multiple independent consumer groups need per-group bookkeeping |
| Postgres SKIP LOCKED | Throughput cliffs differ wildly by library | 2026-05-09 sweep: pgmq 11,277 jobs/s but "Anti-scales past 16 workers; active-readers cliff"; pg-boss 2,387; River 501; Oban 284 — hardbyte benchmark [19] (maintained by a competing author) | 3 | Lock design, batch size and vacuum strategy vary per library |
| SQLite queues | Single writer | "SQLite only allows one writer at a time" — goqite README [26] | 2 | Database-level write lock |
| Redis (Sidekiq, BullMQ) | Jobs disappear under eviction or restart | "You should not run Redis with any other eviction policy than 'noeviction'" — BullMQ maintainer [18], 2021-03-07 | 4 | Redis is a cache first; durability is opt-in |
| Redis (Sidekiq, BullMQ) | Not transactional with the app DB | See Brandur quote above [4] | 3 | Two systems, no shared commit |
| Temporal | Determinism and versioning | "anything that modifies the history event ordering breaks determinism in already-deployed workers" — fcarraldo; "Determinism sucks, you do have to work hard and make everything idempotent in activities" — inglor, HN [17], ~Jun 2026 | 4 | Replay-based execution model |
| Temporal | Self-hosting burden and cost | "Hosting it yourself is what makes it miserable" — cnlwsu; an upgrade "lost some data" — graerg; managed is "pricey" with "very meh" linear scaling — dtech [17] | 4 | Multi-service deployment (DB, Elasticsearch, several roles) |
| Temporal | Visibility | "Visibility can be slow and misses information" — inglor [17] | 2 | Secondary index lags event history |
| S3-backed Kafka clones | Latency | "we've made a significant one with WarpStream: latency"; produce P99 ~400 ms at launch — WarpStream [23], 2023-07-25 (vendor) | 2 | Must persist to S3 before ack |

### When teams outgrow them

The sources converge on four triggers, not one number:

1. **Sustained throughput past roughly 10k jobs/s or a backlog past ~10k rows.** Below that, Postgres works; above it, vacuum and polling dominate [2][3][5]. Morling's point stands: "all writes go to a single node" [21].
2. **Fan-out.** The moment a second independent consumer must see every event (analytics plus billing plus search), a consume-and-delete table stops fitting. Morling: reproducing consumer groups means re-implementing "Kafka's rebalance protocol," which is "far from trivial" [21].
3. **Replay and retention.** "Records are not deleted after processing" is the Kafka property teams miss; jemaclus has "lost critical messages" when queues couldn't be recreated [1].
4. **Blast radius.** When a queue incident takes down customer traffic, teams split them. evantbyrne's caution cuts the other way: "it is a lot more difficult to evolve complex systems into simpler ones" [6], and dharmab called a Postgres queue "one of the worst decisions I've seen made" after "years unwinding the consequences" [9].

Durable execution absorbs the *workflow* use case (multi-step, long-running, human-in-the-loop) but not the *event bus* use case; several HN commenters suggested Oban instead of Temporal for plain jobs [17].

## Part B: cross-cutting pains across all platforms

Frequency labels: **Very high** (appears in most threads read), **High** (several independent sources), **Medium** (two or three), **Low** (one, or inferred).

| Pain | How often it comes up | Evidence (quote + link + date) | Which platforms suffer | What people wish existed |
|---|---|---|---|---|
| Operational weight; needs a dedicated team | Very high | "it's not operationally simple" — lmm [1]; "too operationally complex for our needs" — jolux [1]; "Pulsar's feature set is amazing, but it looks like a beast to operate?" — atombender [1]; "self-hosted Kafka clusters require a dedicated team of experts" — WarpStream [23] | Kafka, Pulsar, Temporal, RabbitMQ clusters ("would always go split-brain" — mstaoru [1]) | "one binary to deploy with Systemd" (KingOfCoders on NATS [1]); tools that "don't require babysitting" |
| Queue vs log: neither model covers both needs ("two systems") | Very high | "Kafka is not really a message queue -- it's a persistent structured log" — jemaclus [1]; "There's no concept in Kafka (yet...) of 'acking' or DLQs" — EdwardDiego [7]; "Kafka comes with very little of the ergonomics that make MQ good" — munchbunny [7]; share groups still have "no notion of a dead letter queue" and "a released message will be retried immediately" — Morling [20], 2025-03-05 | Kafka (no per-message ack), SQS/RabbitMQ (no replay: SQS "can't replay messages" [1]), Postgres (no streams) | One system with per-message ack, DLQ, delay, *and* replayable retention |
| Exactly-once confusion; idempotency burden lands on consumers | Very high | "We enabled exactly-once but we're still seeing duplicates in the database" — Conduktor [12], 2025-10-15; "Don't you always need a database after reading events from Kafka to deduplication?" — gmokki [7]; handlers must be "safely execute[d] twice or more times" — threedots [10] | All (Kafka EOS stops at the cluster edge; everything else is at-least-once) | Broker-side dedup keyed by a client-supplied idempotency key, with the ID visible end to end |
| Dead letters and poison messages | Very high | "infinite retry is an outage, so dead letters need an owner and a replay path" (EDA blog, via search); "Only thing worse than your main queue backing off is you dropping items from going into the DLQ because it can't stay up" — shayonj [8], ~Feb 2026; "you have not other option but to skip it or stop on it" — konart [8] | All; Kafka worst (no native DLQ; head-of-line blocking per partition [20]) | Native DLQ with reason, retry count, query, bulk requeue, and a circuit breaker |
| Observability: where is my message, how far behind am I | High | "clients stop receiving messages while all metrics looked green" — mstaoru [1]; "you publish an event, and then the other end just doesn't react to it" — threedots [10]; "Queue depth is a vital sign, not a diagnosis" — Inngest [15]; OTel messaging attributes "are still marked Development" [28], 2026-01-07 | All | Per-message lifecycle lookup by ID; built-in trace propagation; message age (not just depth) as a first-class metric |
| Ordering vs parallelism | High | "Each partition is processed by not more than one consumer"; "that consumer can't easily move beyond of that message" — Morling [20]; "you can't quickly and effortlessly scale int/out consumers" — guhcampos [7]; advice to keep "as narrow ordering as you can" — threedots [10] | Kafka/Kinesis (partition bound), SQS FIFO (group throughput cap), Postgres (`SKIP LOCKED` breaks order: "If you need to process messages in order, you don't want SKIP LOCKED" — sa46 [5]) | Per-key ordering with free scaling of consumers; ordering as a per-message attribute, not a topic-wide setting |
| Schema evolution breaks consumers | High | "We added a field without a default value. Deploy passed. Then 47 consumers started throwing deserialization errors" — Conduktor [14], 2025-03-15; "If you have a big event that 10 teams depend on, you can't just" change it — threedots [10] | All; Kafka adds a separate Schema Registry to run ("Kafka does not include a schema registry") | Schema registry built into the broker, with compatibility checks at produce time and CI |
| Replay and backfill are risky | High | When committed offsets expire, "messages with offsets 5-10 will never be processed" — Quix [25], 2024-02-26; "you cannot reset offsets to selectively skip successfully processed messages" (runbook via search); replay "can impact live consumers if not managed carefully" | Kafka (coarse offset reset), queues (no replay at all) | Replay by time range or filter into a separate consumer, without touching live groups; selective redelivery of failed IDs |
| Client library quality differs by language | High | Java has idempotence on by default; "Your Python producer, out of the box, has no ordering guarantees during retries and no deduplication"; librdkafka auto-commit "commits offsets for messages your application received but hasn't processed"; "If your Python code doesn't catch it, the message is silently dropped" — Conduktor [13], 2026-05-05; "very little public work/discussion on this so you'll be on your own" — rileymichael [7] | Kafka above all; Temporal SDK parity also uneven (unverified) | A thin wire protocol with identical, safe defaults in every language |
| Cost opacity | High | Networking is "Kafka's biggest infrastructure cost and is too often ignored"; "~90% (!) of your infrastructure costs" with tiered storage; costs "all mixed up with all the other networking usage" — Confluent [24], 2023-04-20; "70-90% of the cost of the workload is just inter-zone bandwidth fees" — WarpStream [23]; managed Temporal costs "more money than you expected" — svara [17] | Kafka (self-hosted and managed), Temporal Cloud, Confluent add-ons | A bill that itemizes per topic/tenant; no hidden cross-AZ charge |
| Delayed, scheduled and priority messages | Medium | Kafka "has no built-in scheduling mechanism"; KIP-1277 (delayed delivery) is still a proposal [27]; share groups: "a released message will be retried immediately" [20]; SKIP LOCKED "isn't quite enough" — needs "priorities, and dead-letter handling" — mike_hearn [2] | Kafka, NATS core, Postgres DIY; RabbitMQ/SQS partial (delay plugin, 15-min cap) | Delay, cron and priority as message attributes, broker-enforced |
| Large payloads (claim check) | Medium | NATS "maximum message size of 10MB" — atombender [1]; SQS was 256 KB until Aug 2025 [30]; claim check "introduces an external dependency and a lifecycle to manage (TTL, purge, access rights ...)" (via search) | All | Transparent offload to object storage with the reference managed by the broker |
| Backpressure | Medium | RabbitMQ memory alarm "blocks every publisher" cluster-wide, has "no hysteresis" and "can flap" [29]; queue growth in Postgres hits the DB itself [2][3] | RabbitMQ (binary alarms), Postgres (shared disk), Kafka (lag is unbounded but silent) | Per-tenant, per-queue flow control with graceful producer slowdown, not cluster-wide stop |
| Retention and compliance deletion (GDPR) | Medium | "Kafka's append-only architecture makes traditional deletion impossible"; "Data exists in active segments, replicas, consumer state stores, downstream databases" — Conduktor [11], 2024-04-24; crypto-shredding has key-management cost; tombstones fail for consumers offline past `delete.retention.ms` | Any immutable log; queues are fine (delete on consume) | Per-subject crypto-shredding as a built-in feature, with downstream deletion fan-out |
| Local development and testing | Medium | Local developers' services "were consuming our messages and the handlers were doing changes in the local database" — threedots [10]; Compose stacks missing schema registry "weaken integration testing"; "pure docker deployment can be a pain because of networking setup" (StackShare review via search) | Kafka worst (broker + registry + Connect), Temporal (multi-service) | Single-binary or in-process mode with identical semantics; per-developer namespaces |
| Security and multi-tenancy | Medium | "ACL management can become a significant operational overhead"; teams split into separate clusters "to avoid multi-tenancy complexity" (Conduktor via search); "Subject authorization is also painful to implement" — RedShift1 on NATS [1] | Kafka, NATS | Role-based, prefix-scoped auth that spans broker, schema registry and connectors |
| Migrations between systems | Low-Medium | Partitioner hashes differ, so "key-based ordering and co-partitioning break on migration" [13]; dual-write during migration "might time out ... and retry – duplicating messages" (lab via search) | All | Stable key hashing, importable offsets, and a documented dual-run procedure |

## The features people keep asking for

Ranked by how many independent sources ask for them.

1. **One system that is both a queue and a log** — per-message ack, DLQ, delay, *and* replayable retention, so teams stop running Kafka plus RabbitMQ/SQS [1][7][20].
2. **Operational simplicity: a single binary, no sidecar services** — the NATS and Postgres pitch, repeated by nearly every HN commenter who left Kafka [1][6][23].
3. **Native dead-letter handling with ownership tools** — reason, retry count, query, bulk requeue, circuit breaker on the DLQ itself [8][20].
4. **Per-message observability** — look up a message by ID and see produced, queued, delivered, acked, retried; message age as a metric; traces propagated by the broker, not by hand [1][10][15][28].
5. **Transactional enqueue (outbox built in)** — the single biggest reason people choose Postgres queues [4][9][21].
6. **Broker-side deduplication and clear delivery semantics** — stop the exactly-once marketing confusion; make idempotency keys first class [7][12].
7. **Per-key ordering that still scales consumers** — the share-group promise without losing ordering [7][20].
8. **Schema registry in the box** with produce-time compatibility checks [14].
9. **Delay, scheduling, and priority as message attributes** [2][20][27].
10. **Transparent cost** — per-topic/per-tenant metering and no cross-AZ surprise [23][24].

## Design lessons for a new platform

- **Make the log the storage primitive and expose queue semantics on top of it.** The two-system problem is the most repeated complaint. Per-message ack, DLQ and delay must be native, not bolted on as share groups were (which shipped without DLQ, delay or ordering [20]).
- **Never hold a database row lock or an MVCC snapshot for the life of a job.** Postgres queues fail on vacuum, xmin horizon and lock churn [2][3]. Claim/complete must be O(1) appends, with periodic table or segment truncation (PgQue's approach) rather than per-row UPDATE/DELETE.
- **Push, don't poll, but keep polling as the fallback.** Polling is the second-biggest Postgres failure [2][9]; LISTEN/NOTIFY "is not a failsafe delivery system" [9].
- **Offer transactional enqueue to the application's database.** Either ship an outbox relay (CDC from Postgres into the platform) or support a two-phase "prepare in app DB, confirm to broker" API, so teams keep the one feature Postgres queues win on [4][21].
- **Ship an idempotency key in the protocol and dedup on the broker.** Be explicit that side effects outside the broker are the consumer's problem, and give consumers an inbox helper [12].
- **Ordering is a per-key promise, not a per-partition cage.** Allow more consumers than keys, with per-key serialization, so parallelism and ordering stop fighting [7][20].
- **DLQ is a product surface.** Store failure reason, attempts, first-seen and last-seen; allow query, inspect, edit and bulk requeue; rate-limit the DLQ writer [8].
- **Every message has an ID and a lifecycle you can look up.** Trace context travels in broker-managed headers; expose message age and oldest-unacked, not just depth [1][10][28].
- **Delay, priority, cron and TTL are message attributes enforced by the broker.** Kafka's absence of these is a decade-old gap [27].
- **Schemas live in the broker.** Compatibility checks at produce time; reject incompatible writes rather than crash 47 consumers [14].
- **Build compliance deletion in.** Per-subject encryption keys with shred-on-request, plus a deletion event fan-out, because tombstones and retention do not satisfy erasure [11].
- **One protocol, one reference client, identical defaults.** Idempotent producer on, bounded in-flight, auto-commit after processing (not before), fail loud when buffers are full [13].
- **Single binary for dev and small deployments; same semantics at scale.** Local mode should need no Docker Compose and no registry sidecar [10].
- **Make cost visible.** Meter bytes and requests per topic and tenant; avoid designs that force cross-AZ replication for every byte [23][24].

## Sources

1. Ask HN: What's your go-to message queue in 2025? — https://news.ycombinator.com/item?id=43993982 — 2025-05-18
2. HN: PgQue: Zero-Bloat Postgres Queue (incl. comment "The vacuum pressure is real", item 47820748) — https://news.ycombinator.com/item?id=47817349 — ~April 2026 (HN shows "6 months ago")
3. Richard Yen, Potential Consequences of Using Postgres as a Job Queue — https://richyen.com/postgres/2026/05/04/postgres_job_queue.html (also on Microsoft Tech Community) — 2026-05-04
4. Brandur Leach, River: a Fast, Robust Job Queue for Go + Postgres — https://brandur.org/river — 2023-11-20
5. HN: SQL Maxis: Why We Ditched RabbitMQ and Replaced It with a Postgres Queue — https://news.ycombinator.com/item?id=35526846 — 2023-04-11
6. HN: An Introduction to Solid Queue for Ruby on Rails — https://news.ycombinator.com/item?id=43924082 — 2025-05-11
7. HN: Kafka is Fast — https://news.ycombinator.com/item?id=45747018 — ~November 2025 (HN shows "11 months ago")
8. HN: Using PostgreSQL as a Dead Letter Queue for Event-Driven Systems — https://news.ycombinator.com/item?id=46755115 — ~February 2026 (HN shows "8 months ago")
9. HN: Postgres as queue (leontrolski) — https://news.ycombinator.com/item?id=39315833 — 2024-02-10
10. Three Dots Labs, Event-Driven Architecture: The Hard Parts — https://threedots.tech/episode/event-driven-architecture/ — last updated 2025-06-04
11. Conduktor, GDPR and Kafka: Right to Erasure — https://www.conduktor.io/blog/gdpr-kafka-right-to-erasure — 2024-04-24 (vendor)
12. Conduktor, Kafka Exactly-Once: When It Works and When It Doesn't — https://www.conduktor.io/blog/exactly-once-semantics-when-it-works — 2025-10-15 (vendor)
13. Conduktor, librdkafka vs Java Client: 10 Config Defaults That Differ — https://www.conduktor.io/blog/librdkafka-vs-java-client — 2026-05-05 (vendor)
14. Conduktor, Avro Schema Evolution: Compatibility Guide — https://www.conduktor.io/blog/schema-evolution-avro-compatibility-guide — 2025-03-15 (vendor)
15. Inngest, Best job queue alternatives in 2026 — https://www.inngest.com/blog/best-job-queue-alternatives — 2026-08-26 (vendor)
16. Aditya Agarwal, Postgres as your message queue is a SPOF you'll regret at 3am — https://dev.to/adioof/postgres-as-your-message-queue-is-a-spof-youll-regret-at-3am-22ha — Aug 23, year not shown; cites a 2023-05-15 benchmark (opinion piece)
17. HN: SQLite is all you need for durable workflows (Temporal discussion) — https://news.ycombinator.com/item?id=48327121 — ~June 2026 (HN shows "4 months ago")
18. BullMQ discussion #420, How does bullmq ensure jobs aren't deleted by Redis evictions? — https://github.com/taskforcesh/bullmq/discussions/420 — 2021-03-07, follow-up 2023-08-28
19. hardbyte, postgresql-job-queue-benchmarking — https://github.com/hardbyte/postgresql-job-queue-benchmarking — sweep dated 2026-05-09 (maintained by the author of one benchmarked system)
20. Gunnar Morling, Let's Take a Look at... KIP-932: Queues for Kafka! — https://www.morling.dev/blog/kip-932-queues-for-kafka/ — 2025-03-05
21. Gunnar Morling, "You Don't Need Kafka, Just Use Postgres" Considered Harmful — https://www.morling.dev/blog/you-dont-need-kafka-just-use-postgres-considered-harmful/ — 2025-11-03
22. RudderStack, Why RudderStack used Postgres over Apache Kafka — https://dev.to/rudderstack/why-rudderstack-used-postgres-over-apache-kafka-for-streaming-engine-3p7c — 2020-09-22 (older than the preferred window; included as the clearest "Kafka alternative on a database" rationale)
23. WarpStream, Kafka Is Dead, Long Live Kafka — https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka — 2023-07-25 (vendor)
24. Confluent, Uncovering Kafka's Hidden Infrastructure Costs — https://www.confluent.io/blog/understanding-and-optimizing-your-kafka-costs-part-1-infrastructure/ — 2023-04-20 (vendor)
25. Quix, Understanding Kafka's auto offset reset configuration — https://quix.io/docs/blog/2024/02/26/kafka-auto-offset-reset-use-cases-and-pitfalls.html — 2024-02-26
26. goqite README — https://github.com/maragudk/goqite — undated (benchmarks quoted from README)
27. Conduktor KIP options explorer, KIP-1277 Support Delayed Message in Kafka — https://kafka-options-explorer.conduktor.io/kip/1277/ — undated; mailing-list discussion at http://www.mail-archive.com/dev@kafka.apache.org/msg154103.html
28. OneUptime, How to Instrument Message Queues with OpenTelemetry — https://oneuptime.com/blog/post/2026-01-07-opentelemetry-message-queue-tracing/view — 2026-01-07
29. Netdata, RabbitMQ memory resource limit alarm — https://www.netdata.cloud/guides/rabbitmq/rabbitmq-memory-resource-limit-alarm/ — undated
30. AWS re:Post, Eliminate the S3 Claim-Check Pattern with SQS 1 MiB Messages — https://www.repost.aws/articles/AR7yxoDQz6TfmEiobTC0J30Q/eliminate-the-s3-claim-check-pattern-with-sqs-1-mib-messages — 2025 (SQS limit raised August 2025)
31. Oban author (sorentwo) HN comment on why Postgres over Redis — https://news.ycombinator.com/item?id=21941263 — 2020 (older than preferred window)

Not retrievable: Kai Waehner, "When (Not) to Use Queues for Kafka?" (2026-01-28) failed with a TLS error; Reddit threads in r/dataengineering, r/devops, r/softwarearchitecture, r/ExperiencedDevs, r/golang, r/node, r/java and r/rust could not be fetched or surfaced by search in this session. Claims attributed "via search" above come from search-engine summaries of pages I did not open in full; treat them as lower confidence.
