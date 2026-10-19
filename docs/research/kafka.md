# Kafka family

Scope: Apache Kafka (KRaft, ZooKeeper legacy, tiered storage KIP-405, Kafka Streams, Kafka Connect) and the managed offerings Confluent Cloud, Amazon MSK and Aiven Kafka. Research date: 2026-10-19. Sources are numbered in `## Sources`; dates are given per source.

Research caveat: Reddit could not be fetched in this environment and search never surfaced r/apachekafka, r/dataengineering or r/devops threads, so community sentiment comes from Hacker News, a 10,000-post analysis of the Confluent Community Forum [8], and practitioner blogs. No Reddit quotes are included because none could be verified.

## Architecture

**Brokers, topics, partitions.** A topic is split into partitions; each partition is an append-only log owned by one leader broker and copied to follower brokers (replication factor, usually 3). Producers choose a partition (by key hash or round-robin). Partition count is the unit of parallelism and can only be increased, never decreased [3][9].

**Replication and ISR.** Followers fetch from the leader. The in-sync replica set (ISR) is the subset that is caught up within `replica.lag.time.max.ms` (default 30 s). A write is "committed" when all ISR members have it; `acks=all` plus `min.insync.replicas=2` is the usual durability setting. `unclean.leader.election.enable` is off by default, so losing every ISR member makes the partition unavailable rather than lossy [3]. The classic failure chain is "follower lags -> ISR shrinks -> leader dies -> no electable replica -> offline partition" [31].

**Storage model.** Each partition is a directory of segment files plus offset and time indexes. Brokers write through the OS page cache and rely on sequential I/O; reads use `sendfile` zero-copy from page cache to socket (disabled when TLS is on). Batches are compressed end to end (gzip, snappy, lz4, zstd) and stay compressed on disk [3]. Retention is time- or size-based per topic. Log compaction (`cleanup.policy=compact`) keeps the latest value per key; a null value is a tombstone removed after `delete.retention.ms` (24 h default) [3].

**Tiered storage (KIP-405).** Closed segments are copied to object storage via a pluggable `RemoteStorageManager`; local retention drops to hours, remote retention runs for months. Production-ready in 3.9. Compacted topics and JBOD are unsupported, `remote.storage.enable` cannot be turned off, and followers still replicate the local tier, so tiering does not remove cross-AZ replication traffic [4].

**Consumer groups and rebalancing.** Consumers pull with long polling and commit one offset per partition; a partition is read by at most one consumer per group, so parallelism is capped at partition count [3][6]. The classic protocol was a client-driven JoinGroup/SyncGroup handshake with a group-wide barrier ("eager rebalancing worked on a stop-the-world principle") [5]. KIP-848, GA in 4.0 (March 2025), moves assignment to the broker-side coordinator, drives it through heartbeats and revokes only partitions that move: "There's no group-wide stop-the-world pause" [1][5]. Java clients opt in with `group.protocol=consumer`; librdkafka was still early access in mid-2025 [5]. Kafka Streams got its own server-side protocol (KIP-1071), GA with limits in 4.2 [2].

**Share groups (KIP-932).** A queue-style group type: any number of consumers read the same partitions, records are handed out under a 30 s lock, and each can be acknowledged, released, rejected or (4.2, KIP-1222) renewed. Delivery count is tracked, default limit 5, then the record is archived. Early access in 4.0, preview in 4.1, "production-ready" in 4.2 (February 2026). No cross-batch ordering, no built-in dead-letter topic yet (KIP-1191 targets 4.4), Java 4.2+ clients only as of March 2026 [2][6][7].

**KRaft controller.** Kafka 4.0 is "the first major release to operate entirely without Apache ZooKeeper"; metadata lives in a Raft-replicated log on a controller quorum. ZooKeeper clusters must migrate on a 3.x release first [1][33]. 4.0 also dropped Java 8, message formats v0/v1 and old protocol versions, and requires Java 17 on brokers [1].

**Exactly-once and transactions.** Since 0.11 the idempotent producer uses a producer ID plus sequence numbers to de-duplicate retries. Transactions (`transactional.id`) write to several partitions atomically and let consumers read with `isolation.level=read_committed`; "exactly-once" in practice means consume-transform-produce inside Kafka, which Kafka Streams wraps natively [3]. Open transactions block share-group progress under `read_committed` [6].

**Protocol.** A custom binary TCP protocol with versioned request types (Produce, Fetch, Metadata, OffsetCommit, Heartbeat); clients bootstrap from a broker list and talk to each partition leader directly, so every client library carries its own routing, batching and retry logic [3][8].

## Strengths

- **Throughput and durability at scale.** "Our Kafka cluster of equivalent resources didn't break a sweat at 500k TPS" after moving off RabbitMQ (HN, 2025) [13]. Sequential I/O, batching and zero-copy make it cheap per byte once volume is high.
- **The log abstraction.** Replay, multiple independent consumer groups and long retention are what LinkedIn built it for: "the main selling point was replayability" [14]. It "is very good at what it does by being deliberately stupid" [13].
- **Ecosystem.** Connect, Streams, Debezium CDC, Flink, Schema Registry and every language client; migrating away is hardest for people with "Kafka Connectors and Avro" [16].
- **Dominant workloads.** Event sourcing, CDC pipelines, log and metrics ingestion, stream processing, inter-service event buses at high volume.
- **Recent fixes to old complaints.** ZooKeeper gone (4.0), incremental rebalances (KIP-848), tiered storage (KIP-405), queue semantics (KIP-932), Streams DLQ (KIP-1034), and an accepted plan for object-storage-native "diskless" topics (KIP-1150, voted 2026-03-02) [1][2][23].

## Pain points

| Pain | Evidence (quote + link + date) | Severity 1-5 | Root cause |
|---|---|---|---|
| Operational burden / JVM / on-call | "constant JVM tuning, watching GC pauses creep in at the worst possible time ... waking up to rebalances" [18] (2025-11); "stupidly complex and very hard to operate reliably in an HA configuration" [14] (2025-08); node replacement and scaling need "a dedicated team of experts and significant amounts of custom tooling" [9] (2023-07) | 5 | Stateful brokers with local disks; leader placement, reassignment and balancing are manual (Cruise Control is a separate JVM service) |
| Configuration complexity and cryptic errors | "The overwhelming majority of issues trace back to one root cause: configuration complexity" ... "Security configuration provides minimal helpful error messages" [8] (2025-08) | 4 | Hundreds of interdependent broker/client knobs with minimal validation |
| Rebalance storms / stop-the-world | Classic protocol "worked on a stop-the-world principle" and "still relied on a group-wide synchronization barrier" even in cooperative mode [5] (2025-06); forum reports of groups that "rebalance too often" and "group has already rebalanced" commit failures [8] | 4 (3 after KIP-848) | Client-driven assignment with a global barrier; a single slow consumer disturbs the whole group. KIP-848 fixes the protocol but needs new clients |
| Partition count as a permanent, upfront decision; consumer scaling tied to it | "it requires predicting the key distribution and number of partitions ahead of time" [14]; "you can never change it!" [9]; "you can't quickly and effortlessly scale int/out consumers" [13] (2025); guidance of 4,000 replicas/broker and 200,000/cluster "is not enforced by Kafka" [27] | 4 | Partition = unit of ordering, parallelism and placement; 1:1 partition-to-consumer mapping makes users "over-partition" for peak [6] |
| Cross-AZ network cost | Best case "$0.053 per GiB streamed through Kafka" and "70-90% of workload cost is inter-zone bandwidth" at scale [9] (2023-07); cross-AZ transfer "often costs more than compute ... sometimes by 5-10x when fan-out is high" [22] (2026-05); an MSK model put over 70% of cost in data transfer [41] | 4 (cloud only) | Leader-based replication plus producer/consumer traffic crosses zones by design; KIP-392 follower fetch only helps reads |
| No native queue semantics / no per-message ack / head-of-line blocking | "The first is head of line blocking ... there is no work stealing or other mechanism for other consumers to help out" [15] (2024-04); Uber had to build uForwarder so "out-of-order commits can improve resiliency to transient poison-pill messages" [17] (2026-02) | 4 (3 after 4.2) | Single committed offset per partition; share groups (4.2) add per-record ack but drop ordering and still lack a DLQ until KIP-1191 |
| No dead-letter queue, no delayed messages | DLQ handled only in Connect/Streams/client libraries; share-group DLQ "targeting Apache Kafka 4.4 in 2026" [7]; delayed delivery listed as "No, not on roadmap" [34] | 3 | Broker has no per-message state or scheduler; retries are built as extra topics plus pauses |
| Kafka Connect and Schema Registry as separate, fragile pieces | Connect was the single largest problem category (831 forum topics) and Schema Registry second (181) [8]; Schema Registry image "is not FOSS" and is under the Confluent Community License since 2018 [25][26] | 3 | Core broker has no schema or connector concept; both bolt-ons run as separate JVM services under a non-OSI license |
| Client complexity and version matrix | "It's unclear which client versions work with which broker versions"; Python/.NET/Node clients "have limited documentation" [8]; new features (KIP-848, share groups) ship in the Java client first, librdkafka later [5][7] | 3 | Thick-client protocol: partition routing, batching, rebalancing and transactions all live client-side |
| Upgrades and ZK->KRaft migration | Migration "can stall" if the ZK security tool was ever run [33]; Strimzi users reporting migrations "stuck" (2025); Sentry held its migration because an earlier attempt "would result in data loss" [32]; 4.0 has no in-place path from ZooKeeper | 3 | Metadata system swap under a running cluster; long-lived clusters accumulate state the migrator did not anticipate |
| Managed-service cost and vendor risk | "Confluent cloud is very expensive, and you still have to deal with a surprising amount of scaling headaches" [16] (2025-12); post-IBM, "Confluent's current pricing may not survive contact with IBM's enterprise sales model" [24] (2026-03) | 3 | Per-eCKU plus per-GB plus PrivateLink pricing; concentration of maintainers in one vendor |
| Consumer lag and tail latency | Lag "persists, especially in CDC pipelines, with reports of 45+ minute delays"; Streams apps "report 5 to 17 second latencies"; producers see "intermittent 5 to 10 second delays" [8] | 3 | Page-cache misses on cold reads, GC pauses, rebalances, KTable join amplification |
| Retention and replay gotchas | Size-based retention "can silently remove records that were still eligible for delivery" in share groups [6]; KRaft deployments "risk data loss if /tmp is cleared" [8]; StreamNative found "infinite retention ... expensive and unmaintainable" [29] | 2 | Retention is a broker-side time/size policy unaware of consumer progress |
| Weak multi-tenancy | Only per-client-id/user byte-rate and request-rate quotas [3]; "Any modern data system should be built with multi-tenancy in mind from the ground up" [12] (2025-04) | 2 | Flat topic namespace, no tenant object, no isolation of page cache or disk per tenant |
| Local developer experience | "setting up a Kafka environment for local testing can be a challenging task"; Docker/Kubernetes "add networking, volume, and permission problems" [8][35] | 2 | JVM broker plus separate registry/Connect containers; advertised-listener confusion |
| Small-message overhead | No quotable source verified; the design doc attributes efficiency to batching, implying high per-record cost when batches are small [3] | 2 | Per-record headers, index entries and per-partition fetch overhead amortised only by batching |

## Why people leave

- **It was never a queue.** Work-queue users hit head-of-line blocking, no per-message ack and no DLQ; "the mitigations for it aren't pretty" [15]. Destinations: RabbitMQ, SQS, Pub/Sub, Pulsar (per-message ack), NATS JetStream ("sounds like the exactly perfect fit", though its 1 MB message cap was a "showstopper" for another commenter) [36].
- **Overkill at low volume.** HN rule of thumb: Postgres below ~100 msg/s, Kafka or Redpanda above ~100k/s [13]. Dagster chose Postgres because "there are fewer moving parts than a large Kafka deployment", accepting failover downtime and over-provisioning [19]. Reviewers call Kafka "a kind of overkill for a small-medium service" [37].
- **Ops fatigue -> Kafka-compatible replacements.** Redpanda (C++, single binary): "Maintenance became a daytime activity instead of something you planned a weekend around" [18]; "a superior wire-compatible alternative to Kafka for years" [16]. Protocol compatibility keeps clients and Connect.
- **Cloud bill -> object-storage-native systems.** WarpStream, AutoMQ, Bufstream and Confluent Freight sell "the reliability and availability of running in three AZs but with the cost of one" [10]; the trade-off is 100 ms-plus produce latency [10].
- **Retention and large topics -> Pulsar.** StreamNative's account: "performance degraded and latencies became volatile on their larger topics" and infinite retention was unmaintainable [29] (vendor source, date not verified).
- **Vendor risk after IBM.** The $11B acquisition (announced 2025-12-08, closed 2026-03-17) prompted "Great time to be a kafka alternative" [16] and reports of up to 800 Confluent layoffs with "no official statement" [24].

## Cost

Numbers are what sources cite, not validated benchmarks.

- **Self-hosted.** Small 3-broker clusters: roughly $850-1,500/month on EC2 or EKS before staff [38]; the AxonOps model (100 GiB/day in, 7-day retention, 2 consumer groups, RF 3) prices disks plus EKS at ~$312 plus ~$120-129 cross-AZ replication, excluding compute [20]. The real line item is people: "$8,000-12,000 per month for a Site Reliability Engineer" [38].
- **Amazon MSK.** Official example: 3 x kafka.m7g.large at $0.204/broker-hour = $455/month plus $0.10/GB-month storage [21]; ~$740 with storage on the AxonOps workload. Express brokers cost ~2x ($0.408/h) plus $0.01/GB ingest [20]. In-cluster replication is free, client cross-AZ traffic is not; one FinOps model found "over 70% of the cost is network data transfer" at ~$10.8k/month for a high-ingress case [41].
- **Confluent Cloud.** Basic starts near $0 (first eCKU free); Standard is $0.75/eCKU-hour, so 2 eCKUs is ~$1,080/month before traffic ($0.035-0.050/GB) and storage, ~$1,395-1,530 on the AxonOps workload [20]. Dedicated and Freight start in the low thousands (Freight ~$2,300/month per one comparison) [39]. AxonOps flags a pre- vs post-replication storage pricing contradiction in Confluent's own docs [20].
- **Cross-AZ dominates at scale.** WarpStream's example: 140 MiB/s in, 560 MiB/s out costs ~$641/day in inter-zone fees on Kafka vs under $15/day on an S3-backed design [9]. Where cross-AZ is cheap, "such solutions" exist only because "AWS pricing is whack" [11].

## Design lessons for a new platform

- **Broker-side group state (keep KIP-848's direction):** server-side, incremental, heartbeat-driven assignment; never a global barrier; no client-side assignor every language must reimplement.
- **Logs and queues on the same data,** with per-message ack, lease/renew, delivery counts, retry delay and a built-in dead-letter destination from day one. Do not force a choice between ordering and elastic scaling: support key-level ordering inside a shared subscription.
- **Decouple parallelism from storage layout.** Partition count must not be a permanent upfront guess: allow split/merge or make ordering a property of keys, and let consumers scale past shard count.
- **Separate storage from compute; object storage as the durable tier,** with a local tier for low-latency tail reads, chosen per topic so latency-sensitive topics can still use quorum replication. Avoid cross-AZ replication as the default path.
- **Consumer-aware retention:** never silently delete data an active subscription still owes a consumer; expose unacked backlog as a first-class metric.
- **Schema and connectors inside the system, same open license,** with broker-side schema validation and a thin, versioned wire protocol so non-Java clients are not second-class.
- **Multi-tenancy as a core object:** namespaces with isolated quotas, storage accounting and auth, not just client-id throttles.
- **Delayed and scheduled delivery built in,** so retry topics and paused fetchers (Uber's uForwarder) are unnecessary.
- **Operational guardrails by default:** single binary, no JVM tuning, validated config with actionable errors, enforced partition limits (KIP-578's idea), automatic data rebalancing, in-place rolling upgrades with no bridge-release migrations, and a one-container local setup with registry and admin UI included.
- **Keep what Kafka got right:** append-only log with offsets and replay, end-to-end batching and compression, idempotent producers, cumulative ack for streaming consumers, compaction, pull with long polling, and protocol stability that lets an ecosystem form.

## Sources

1. Apache Kafka 4.0.0 release announcement, 2025-03-18 — https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement
2. Apache Kafka 4.2.0 release announcement, 2026-02-17 — https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/
3. Apache Kafka 4.2 documentation, Design, 2026 — https://kafka.apache.org/42/design/design/
4. KIP-405: Kafka Tiered Storage (production-ready in 3.9, 2024-11) — https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage
5. Confluent blog, KIP-848 consumer rebalance protocol, 2025-06-03 — https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/
6. KIP-932: Queues for Kafka (wiki updated 2026-01-26) — https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
7. Confluent blog, Kafka queue semantics GA with share consumer, 2026-03-03 — https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/
8. Conduktor, "The Real Problems with Apache Kafka: 10,000 Forum Posts Analyzed", 2025-08-11 — https://www.conduktor.io/blog/apache-kafka-what-10-000-forum-posts-reveal
9. WarpStream, "Kafka Is Dead, Long Live Kafka", 2023-07-25 — https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka
10. Hacker News thread on the WarpStream post, 2023-08-07 — https://news.ycombinator.com/item?id=37036635
11. Hacker News, "What If We Could Rebuild Kafka from Scratch?", 2025-04-25 — https://news.ycombinator.com/item?id=43790420
12. Gunnar Morling, "What If We Could Rebuild Kafka From Scratch?", 2025-04-24 — https://www.morling.dev/blog/what-if-we-could-rebuild-kafka-from-scratch/
13. Hacker News, "Kafka is Fast – I'll use Postgres", ~2025-11 — https://news.ycombinator.com/item?id=45747018
14. Hacker News, "Why was Apache Kafka created?", 2025-08-23 — https://news.ycombinator.com/item?id=44988845
15. Hacker News comment on Kafka as a work queue (AutoMQ Show HN), 2024-04-08 — https://news.ycombinator.com/item?id=39966666
16. Hacker News, "IBM to acquire Confluent" discussion, 2025-12 — https://news.ycombinator.com/item?id=46192728
17. Uber Engineering, "Introducing uForwarder", 2026-02-05 — https://www.uber.com/blog/introducing-ufowarder/
18. Pavel Sanikovich, "We Migrated From Kafka to Redpanda — What Actually Happened", 2025-11-29 — https://dev.to/devflex-pro/we-migrated-from-kafka-to-redpanda-what-actually-happened-1g14
19. Dagster, "Skip Kafka, use Postgres message queue", 2022-10-04 — https://dagster.io/blog/skip-kafka-use-postgres-message-queue
20. AxonOps, "Self-Hosted Kafka vs Cloud Costs in 2026 (Revised)", updated 2026-10-07 — https://axonops.com/blog/kafka-cost-comparison-2026-revised/
21. Amazon MSK pricing page, 2026 — https://aws.amazon.com/msk/pricing/
22. "Kafka Compute Is Cheap. Network Is Not", dev.to, 2026-05-20 — https://dev.to/eeldor/kafka-compute-is-cheap-network-is-not-2bdh
23. Aiven, "KIP-1150 Accepted, and the Road Ahead", 2026-03 — https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead
24. Factor House, "What the IBM-Confluent deal means for Kafka users", 2026-03-26 (updated 2026-10-05) — https://factorhouse.io/articles/what-the-ibm-confluent-acquisition-means-for-kafka-users/
25. InfoQ, "License Changes for Confluent Platform Restricting Cloud Vendor Usage", 2018-12 — https://www.infoq.com/news/2018/12/confluent-license-changes/
26. Aiven statement on Confluent Platform license changes, 2018-12 — https://aiven.io/blog/aiven-statement-on-kafka-license
27. KIP-578: Add configuration to limit number of partitions (2020) — https://cwiki.apache.org/confluence/spaces/KAFKA/pages/148641509/KIP-578+Add+configuration+to+limit+number+of+partitions
28. Aiven community FAQ on partition limits — https://aiven.io/community/forum/t/faq-is-there-a-limit-on-the-total-number-of-kafka-partitions-in-the-cluster/215
29. StreamNative, "Why we moved from Apache Kafka to Apache Pulsar" (vendor; date not verified) — https://streamnative.io/blog/moved-from-apache-kafka-to-apache-pulsar
30. IBM newsroom, IBM to acquire Confluent, 2025-12-08 — https://newsroom.ibm.com/2025-12-08-ibm-to-acquire-confluent-to-create-smart-data-platform-for-enterprise-generative-ai
31. Netdata guide, Kafka ISR shrink storm (undated) — https://www.netdata.cloud/guides/kafka/kafka-isr-shrink-storm/
32. Sentry self-hosted issue on ZooKeeper to KRaft migration data-loss risk — https://github.com/getsentry/self-hosted/issues/2501
33. Apache Kafka 3.7 KRaft operations docs (migration stall warning) — https://kafka.apache.org/37/operations/kraft/
34. Redisson, "Is Kafka a Message Queue?", 2026-08 (vendor comparison table) — https://redisson.pro/blog/is-kafka-a-message-queue.html
35. tech-tales.blog, "A simple Kafka Development Setup in Docker", 2025 — https://tech-tales.blog/en/posts/2025/kafka-development-setup/
36. Hacker News comments on NATS JetStream vs Kafka, 2025-08 — https://news.ycombinator.com/item?id=44999442
37. StackShare, Kafka vs RabbitMQ user decisions — https://stackshare.io/stackups/kafka-vs-amazon-sqs
38. OneUptime, "Confluent Cloud vs AWS MSK vs Self-Hosted Kafka", 2026-01-21 — https://oneuptime.com/blog/post/2026-01-21-kafka-managed-comparison/view
39. tech-insider.org, "Confluent vs MSK vs Redpanda", 2026-09 — https://tech-insider.org/confluent-vs-msk-vs-redpanda-2026/
40. Apache Kafka dev list, KIP-1150 vote discussion (Colin McCabe), 2025-05 — https://www.mail-archive.com/dev@kafka.apache.org/msg149838.html
41. Cloudatler, Managed Kafka cost comparison MSK vs Confluent (FinOps model, 2026) — https://cloudatler.com/blog/managed-kafka-cost-comparison-amazon-msk-vs-confluent-cloud
