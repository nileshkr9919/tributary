# Pulsar family

Scope: Apache Pulsar, Apache BookKeeper, StreamNative (Oxia, KoP, Ursa) and the "compute/storage separated" lineage. Researched 2026-10-19. Method note: reddit.com blocks the crawler used here and no Reddit threads surfaced via search, so community sentiment comes from Hacker News, GitHub issues, engineering blogs, analyst posts and vendor material. Vendor sources are flagged.

## Architecture

**Three tiers.** A cluster is brokers + a BookKeeper cluster + a metadata store [1]. Brokers are "a stateless component" that own topics and serve reads from a managed-ledger cache, falling back to bookies when the backlog exceeds the cache. A ledger is "an append-only data structure with a single writer," replicated across bookies with ensemble/write-quorum/ack-quorum settings. A topic is a *managed ledger*: a chain of ledgers rolled over on time, size or failure, so any broker can take ownership without copying data [1]. Each bookie writes every entry twice, to an fsynced journal and to ledger storage, which is both its durability story and its throughput cost [17].

**Metadata.** ZooKeeper held topic ownership, ledger metadata, schemas and load data, plus a separate instance-wide configuration store for tenants/namespaces/clusters [1]. StreamNative open-sourced Oxia in April 2023 to lift ZooKeeper's size and throughput limits [26]. Pulsar 5.0.0-M1 (June 2026, preview) makes Oxia "the recommended metadata store for new Pulsar clusters," adds live ZooKeeper-to-Oxia migration (PIP-454), drops etcd, and introduces "scalable topics" (range segments that split/merge while keeping per-key order) plus a V5 Java client with queue/stream/checkpoint consumers [6].

**Topic model.** Persistent and non-persistent (memory only, lossy) topics; partitioned topics are N internal topics spread across brokers with key-hash or round-robin routing. Namespaces sit under tenants and carry policies (retention, TTL, quotas, offload, dedup) [2].

**Subscriptions.** Exclusive (one consumer, ordered), Failover (hot standby), Shared (round-robin, per-message acks, no ordering), Key_Shared (same key to one consumer at a time). Individual and cumulative acks; cumulative is not allowed on Shared/Key_Shared. A message is deleted only after every subscription acks it [2].

**Redelivery fine print** [2]: negative acks (default 1 min, with backoff) "can deliver failed messages out of original order" on ordered types; the redelivery counter is in memory and resets on broker restart or unload, so `maxRedeliverCount` "may never be reached"; retry-letter topics are "enabled only for Shared subscriptions"; dead-letter topics only for Shared and Key_Shared; delayed delivery only for Shared and Key_Shared, tracked in memory, and delayed messages can block ledger deletion. Key_Shared producers must disable batching or use key-based batching.

**Built-ins.** Tiered storage offloads sealed ledgers to S3/GCS/Azure/OSS/HDFS; reads use a sparse index and ranged object reads, and timestamp seeks "can be expensive" [4]. Geo-replication is asynchronous by default; subscriptions, cursors and acks stay local, and replicated subscriptions sync only the mark-delete position for failover, not active-active [3]. Schema registry stores schemas in BookKeeper "without additional system required," per topic, with compatibility checks [27]. Pulsar Functions run Java/Python/Go in thread, process or Kubernetes runtimes with state in BookKeeper [27]. Transactions span produce, consume and ack across partitions; the coordinator runs inside the broker; "developer preview" in 2.7 (2020), declared stable by 2022, Java client only in the docs [28].

## Strengths

- **Queue and stream in one system.** Shared/Key_Shared with per-message acks, nacks, DLQs and delayed delivery "give it native message-queue semantics that Kafka lacks without application-level workarounds" [21]. Iterable adopted it because Kafka "lacked the necessary work-queue semantics" and Pulsar "covers all of our use cases for Kafka, RabbitMQ, and SQS" [23]. A 2025 HN user: "Without it we would have to manage two separate messaging systems" [11].
- **Stateless brokers, no partition rebalance.** Ownership moves without data copy; one 2026 comparison measured <5 s node-failure recovery vs ~180 s for Kafka (single unverified benchmark) [22].
- **Millions of cheap topics** and **first-class multi-tenancy** (tenants/namespaces, quotas, 4.0 token-bucket rate limiting), which is why multi-tenant SaaS picks it [5][21][22][24].
- **Fewer side systems**: geo-replication, tiered storage, schema registry and functions are built in [3][4][20].
- **Vendor cost claims** (StreamNative, 2024): 2.5x cheaper compute at baseline, ~6x at 10x fan-out, "nearly a 60% reduction" in infra for a 100/300 MBps, 7-day workload [16]. Marketing; see Cost.

## Pain points

| Pain | Evidence (quote + link + date) | Severity 1-5 | Root cause |
|---|---|---|---|
| Three stateful tiers to run | "You run and monitor three tiers (brokers, bookies, ZooKeeper/metadata store) instead of Kafka's single broker tier." Conduktor, 2026-10-16 [21]. A Pulsar advocate still counts 9 nodes minimum, 2024-05-03 [20]. "a lot of maintenance liability" [24]. One symptom "can point to brokers, bookies, metadata, clients, disks, or networks." AutoMQ (vendor), 2026-06-02 [19] | 5 | Disaggregation traded data movement for component count; metadata is still a separate quorum |
| Bugs causing downtime | "we've found it to be quite complex and prone to numerous bugs. These issues have unfortunately led to significant downtime within our production environment." HN, 2024-04-29 [12] | 4 | Large surface (broker, BK, metadata, functions) with many interacting features |
| Key_Shared stuck consumers and ordering | "Consumers can get stuck with Key_Shared subscriptions in all previous versions of Pulsar before 4.0.0." #23480, 2024-10-18 [7]. Broker would "keep reading redelivered messages, discarding them and not dispatching any single messages." #21656, 2023-12-02 [8]. After 4.0, nacks "get redelivered after a delay without considering the ordering" [7] | 4 | Per-key order + per-message ack + consumer churn with dispatcher state in broker RAM; fix (PIP-379) only landed Oct 2024 [5] |
| Direct-memory OOM, bad defaults | Maintainer: "the defaults in Pulsar are not optimal"; cache is "20% of direct memory," set explicitly "to avoid direct memory OOM"; dispatch-in-subscription-thread is "a known issue" for OOM. #23466, 2024-10-16, fixed in 4.1.x (Dec 2025) [9] | 4 | Publish buffer, ledger cache and in-flight reads share one Netty pool with percentage defaults |
| Bookie recovery | AutoRecovery "didn't find any underreplication ledger" and never rereplicated after bookie loss until the pod was redeployed; 3.0.0, fixed in BK 4.16.3. #21987, 2024-01-29 [10]. Docs advise dedicated AutoRecovery nodes for large clusters [1] | 3 | Repair is a separate auditor/worker subsystem driven by metadata watches |
| Throughput and tail latency | Confluent (vendor): Pulsar 305 MB/s vs Kafka 605 MB/s; p99 25 ms vs 5 ms; the double write is "a fundamental design choice" with "profound negative impact on throughput," 2020-08-21 [17]. StreamNative's 2022 report claims the reverse [16]. 2026 third-party table: Kafka 850 vs Pulsar 720 MB/s, but Pulsar p99 4 ms vs 12 ms under fan-out [22] | 3 | fsynced journal + ledger double write; extra client→broker→bookie hop |
| Small community, no traction | "I see zero traction in the market... I was close to removing Apache Pulsar." Waehner, 2024-12-04 [14]. "doesn't seem to have the ecosystem momentum to compete with the Kafka juggernaut." HN, 2025-02-19 [11]. "the Pulsar talent pool is significantly smaller than Kafka's" [25] | 4 | Late start, Kafka ecosystem lock-in, vendor pivot |
| Vendor pivot (StreamNative) | "Even StreamNative is effectively abandoning Pulsar and going all-in on the Kafka protocol." HN, 2025-02-19; CTO reply: "Ursa supports Pulsar protocol as well" [11]. Ursa's goal: "eliminate the need for BookKeeper as a WAL storage solution," 2024-05-14 [15]. 2026 Lakestream launch leads with Ursa For Kafka; Pulsar-native on the cost tier is "coming after Pulsar 5.0" (search summary). IBM/DataStax's Pulsar line is 3.1, unsupported upstream since 2024 (secondary source) | 4 | One dominant vendor whose revenue follows the Kafka API |
| Docs and onboarding | Docs "absurdly bad"; a week "of work and experiments" vs "literally 2 hours spent in Kafka." HN, 2020-07-10 [13]. Iterable: "incomplete documentation" [23]. Dated, but the 2024 defaults issue shows tuning knowledge still lives with maintainers [9] | 3 | Dozens of cache/dispatcher knobs with percentage defaults |
| Inconsistent feature matrix | Retry topics "only for Shared"; DLQ and delayed delivery only Shared/Key_Shared; no cumulative ack on Shared; nack reorders ordered subs; redelivery count lost on restart. Docs, 2026 [2] | 3 | Features bolted onto four subscription types instead of one model |
| Tiered storage reads | Seeks "can be expensive"; offset-probe tuning needs a restart; offloaders are a separate package per broker [4]; "demands careful tuning to avoid latency spikes on reads" [25] | 2 | Sparse index per 64 MiB block; offloader is an add-on |
| Non-Java client maturity | Go changelog lists repeated producer/consumer deadlocks and an "availablePermits leak that could cause the consumer stuck" (0.8–0.12); Python async client "exposes little beyond basic sends" (maintainer gist, undated); transactions Java-only [28]; V5 client is Java first, other SDKs "planned before GA" [6] | 3 | Permits, chunking, key batching and txn are reimplemented per client |
| Connector and tooling gaps | "no live migration tooling between Kafka and Pulsar equivalent to MirrorMaker" [21]; "had to build an S3 sink" [24]; 5.0 moves connectors out of the main repo [6] | 3 | Pulsar IO is its own model; ecosystem effort went to KoP |

## Why people leave

No company has published a "we left Pulsar" post that this research could find; evidence is indirect (HN, analyst posts, vendor replacement guides). The pattern:

1. **Operational load for a small team.** The 2024 HN commenter with "significant downtime" was evaluating AutoMQ to make *Kafka* easier, not a Pulsar fix [12]. AutoMQ (vendor) cites "the accumulation of small operational costs," concentration risk ("One or two engineers learn enough about BookKeeper, metadata services, and broker behavior"), and that "running Pulsar may force translation at too many edges" [18][19].
2. **Ecosystem gravity.** Flink, Debezium, Connect and the schema tooling are Kafka-first; Pulsar's answer was KoP, then Ursa, which signals that the Kafka API is the safe bet [11][14][15].
3. **Hiring** against a smaller talent pool with fewer public post-mortems [24][25].
4. **Vendor risk.** The primary vendor's flagship is now a Kafka-API product on object storage; IBM's distribution lags upstream [11][15].
5. **Disputed performance edge.** Nobody leaves Kafka for a contested 2x [16][17][22].

Where they go: Kafka (MSK, Confluent) for ecosystem; Redpanda for single-binary ops; WarpStream/AutoMQ-style Kafka-on-S3 for the cost argument Pulsar used to own; NATS JetStream when the real need was small-scale queue semantics. Who stays: multi-tenant SaaS with millions of topics, and teams needing queue + stream + geo-replication in one box [11][21][22].

## Cost

- **Software:** Apache 2.0.
- **Footprint:** 3 brokers + 3 bookies + 3 ZK/Oxia (9 nodes), or 6 with bookies colocated; plus proxies and function workers if used [1][20]. Bookies want two disk classes (SSD journal, HDD/SSD ledger) [16].
- **Infra vs Kafka:** StreamNative claims ~60% lower infra, 77% lower compute for 100 MBps in / 300 out, 7-day retention, and >90% storage savings with S3 offload [16]. Confluent's benchmark implies the opposite for throughput-bound clusters [17]. A neutral 2026 guide cites "30-50% infrastructure savings" case studies but says "the operational overhead is real and should be budgeted honestly" [25]. Cross-AZ replication can be ~90% of spend once storage is tiered, which is what Ursa targets [15][16].
- **Managed:** StreamNative list prices start at $73/mo serverless, $365/mo BYOC, $505/mo dedicated (floor prices from the 2026 pricing page, not independently verified).
- **Headcount:** No source gives a hard number. One blog estimates 0.5 FTE (unsourced); AutoMQ's "one or two engineers" framing [19] and the "operational tax" language [25] are the closest. Plan on 1–2 engineers with BookKeeper skills as the floor for self-hosting.

## Design lessons for a new platform

- **Keep storage/compute separation; collapse the tier count.** Stateless serving and segment-based topics are Pulsar's best ideas. Embed metadata (Raft) and use object storage or one stateful service so the minimum cluster is a single binary type.
- **One subscription model, not four.** A single consumer abstraction (queue with per-message acks, stream with checkpoints) where nack, delay, retry, DLQ and ordering all work. Pulsar's "only for Shared" caveats come from layering features onto subscription types [2]; its 5.0 V5 client admits this [6].
- **Per-key ordering as a storage concept.** Key_Shared bugs ran 2020–2024 because ordering state lived in broker RAM [7][8]. Persist per-key dispatch state or shard keys into durable sub-logs.
- **Persist redelivery counters, delay timers and retry state.** Pulsar loses all three on restart/unload [2].
- **No percentage-of-memory defaults.** Ship absolute, profile-derived defaults validated at startup [9].
- **Avoid the double write.** Journal + ledger per replica halves disk throughput [17]; use one append log with group commit, or object storage as the WAL.
- **Recovery is part of the write path's controller**, observable in one metric, not a sidecar that can silently idle [10].
- **Tiered storage core with a dense index**; offload by default and treat cold reads as a measured first-class path [4].
- **Protocol first, thin clients.** Put batching, chunking, key routing and flow control on the server so every language gets every feature [6][28].
- **Speak the Kafka protocol natively from day one.** Every Pulsar vendor ended up building one; ecosystem gravity, not architecture, is what killed traction [14][15].
- **Keep real multi-tenancy**: tenants, quotas, token-bucket limits, millions of cheap topics [5][21][22].
- **Replicate cursor state across regions**, not just data [3].
- **One "why is this subscription stuck" endpoint** and end-to-end trace ids; Pulsar only added Key_Shared diagnostics in 4.0 [5].

## Sources

1. Pulsar docs, Architecture overview, https://pulsar.apache.org/docs/next/concepts-architecture-overview/ (read 2026-10-19)
2. Pulsar docs, Messaging concepts, https://pulsar.apache.org/docs/next/concepts-messaging/ (read 2026-10-19)
3. Pulsar docs, Geo-replication, https://pulsar.apache.org/docs/next/concepts-replication/ (read 2026-10-19)
4. Pulsar docs, Tiered storage overview, https://pulsar.apache.org/docs/next/tiered-storage-overview/ (read 2026-10-19)
5. Announcing Apache Pulsar 4.0, https://pulsar.apache.org/blog/2024/10/24/announcing-apache-pulsar-4-0 (2024-10-24)
6. Pulsar 5.0.0-M1 release notes, https://pulsar.apache.org/release-notes/versioned/pulsar-5.0.0-M1/ (2026-06-23)
7. GitHub apache/pulsar #23480, nack with Key_Shared preserving ordering, https://github.com/apache/pulsar/issues/23480 (2024-10-18)
8. GitHub apache/pulsar #21656, Key_Shared infinite read loop, https://github.com/apache/pulsar/issues/21656 (2023-12-02)
9. GitHub apache/pulsar #23466, broker cache defaults, https://github.com/apache/pulsar/issues/23466 (2024-10-16, closed 2025-12-09)
10. GitHub apache/pulsar #21987, AutoRecovery not rereplicating, https://github.com/apache/pulsar/issues/21987 (2024-01-29)
11. Hacker News, StreamNative abandoning Pulsar thread, https://news.ycombinator.com/item?id=43103251 (2025-02-19)
12. Hacker News, streaming platform comparison thread, https://news.ycombinator.com/item?id=40195195 (2024-04-29)
13. Hacker News, Pulsar vs. Kafka, https://news.ycombinator.com/item?id=23787958 (2020-07-10)
14. Kai Waehner, The Data Streaming Landscape 2025, https://www.kai-waehner.de/blog/2024/12/04/the-data-streaming-landscape-2025/ (2024-12-04)
15. StreamNative, Ursa: Reimagine Apache Kafka, https://streamnative.io/blog/ursa-reimagine-apache-kafka-for-the-cost-conscious-data-streaming (2024-05-14, vendor)
16. StreamNative, Evaluating the Infrastructure Costs of Pulsar and Kafka, https://streamnative.io/blog/a-guide-to-evaluating-the-infrastructure-costs-of-apache-pulsar-and-apache-kafka (2024-05-06, vendor)
17. Confluent, Benchmarking Kafka vs Pulsar vs RabbitMQ, https://www.confluent.io/blog/kafka-fastest-messaging-system/ (2020-08-21, vendor)
18. AutoMQ, Pulsar Replacement Guide, https://www.automq.com/blog/pulsar-replacement-guide-when-to-move-off-pulsar-and-what-to-choose (2026-06-02, vendor)
19. AutoMQ, Pulsar Operations Cost, https://www.automq.com/blog/pulsar-operations-cost-running-pulsar-in-production (2026-06-02, vendor)
20. Kiryl Valkovich, Demystifying Apache Pulsar Deployment Complexity, https://visortelle.substack.com/p/demystifying-apache-pulsar-deployment (2024-05-03)
21. Conduktor, Kafka vs Pulsar: Architecture Compared, https://www.conduktor.io/glossary/kafka-vs-pulsar (2026-10-16)
22. sanj.dev, Pulsar vs Kafka 2026: The Post-ZooKeeper Era, https://sanj.dev/post/pulsar-vs-kafka-deep-dive/ (2024-04-28, updated 2026-04-08)
23. InfoQ, How Apache Pulsar is Helping Iterable Scale, https://www.infoq.com/articles/pulsar-customer-engagement-platform/ (2020-11-30)
24. StreamNative blog (streamsql), Why we moved from Kafka to Pulsar, https://streamnative.io/blog/moved-from-apache-kafka-to-apache-pulsar (2020-04-21)
25. modern-datatools, Apache Pulsar Pricing, https://www.modern-datatools.com/tools/apache-pulsar/pricing (updated 2026-07-25)
26. StreamNative, Introducing Oxia, https://streamnative.io/blog/introducing-oxia-scalable-metadata-and-coordination (2023-04-27, from search summary)
27. Pulsar docs, Functions overview and Schema overview, https://pulsar.apache.org/docs/next/functions-overview/ and https://pulsar.apache.org/docs/next/schema-overview/ (read 2026-10-19)
28. Pulsar docs, Why transactions, https://pulsar.apache.org/docs/next/txn-why/ ; StreamNative, Transaction coordinators deep dive, https://streamnative.io/blog/deep-dive-transaction-coordinators-apache-pulsar ("developer preview" in the 2.7.0 release post, 2020-12-25; "stable" per ApacheCon Asia 2022 abstract)
