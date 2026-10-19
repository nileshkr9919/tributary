# Kafka-compatible new wave

Research date: 2026-10-19. Scope: Redpanda, WarpStream, AutoMQ, Bufstream, Confluent Freight / KIP-1150 diskless topics, Apache Kafka tiered storage, S2.dev, plus Uber's federation story. Method note: reddit.com blocks the fetcher used here, so community sentiment comes from Hacker News threads, independent newsletters and GitHub issues rather than Reddit. Every quote was read from the linked page; unverified claims are marked.

## Architecture

### Redpanda
- C++ broker on Seastar: it "implements a thread-per-core programming model through its use of the Seastar library," threads pinned to cores, message passing between them. Each partition is a Raft group; a controller partition (also Raft) holds metadata [15].
- Memory is "Upfront memory allocation, partitioned and pinned to CPU cores." Rules: "at least 2 GB of memory per core" and "at least 2 MB of memory for each topic partition replica. If insufficient memory is available, topic operations will fail" [15][16]. Per-shard memory is fixed at start, so one hot shard can exhaust memory while the host has RAM free [17]. The docs do not mention io_uring or zero-copy; I could not verify those marketing claims from official sources.
- Local NVMe "strongly recommended." Tiered Storage, RBAC and Kerberos are Enterprise-only; Community Edition is BSL, meant to "deter cloud providers from offering our work as a service" [16][18].
- Cloud Topics (GA March 2026) is the hybrid answer to diskless: payloads batched in memory "defined by time (e.g., 0.25 seconds) or size (e.g., 4MB)," written to S3 as mixed L0 objects, then "a placeholder batch with the data's location is replicated through the partition's Raft log, and only then is the producer acknowledged." A Reconciler rewrites L0 into per-partition L1 files. "The data payload lives in the cloud, but the guarantees live in Redpanda" [19]. Metadata and transactions still ride Raft on local disk.

### WarpStream (Confluent, now IBM)
- "Agents are stateless Go binaries (no JVM!) that speak the Kafka protocol," and "any WarpStream Agent can act as the 'leader' for any topic" [1]. Agents write batched files to the customer's S3; each file mixes many partitions, so PUTs scale with agents x flush interval, not partitions [2][3].
- Metadata lives in a hosted control plane: "The control plane runs in our cloud, where we decide which Agents will be compacting your data files" [3]; "We have a custom metadata database running in our cloud control plane which handles ordering" [2]. Only metadata leaves the customer VPC.
- Latency at launch: "P99 of ~400ms for Produce requests" and "P99 latency of data end-to-end from producer-to-consumer is around 1s" [1]. 2024 benchmark: "sub-second P99 producer latency and sub-2s P99 end-to-end latency in (almost) every workload" [6]. Lightning Topics (Feb 2026) on S3 Express One Zone: "33ms median produce latency"; Ripcord Mode "lets Agents continue processing writes during Control Plane outages" [5].
- Compatibility: at launch "We do not expose support for transactions or idempotent producing today" [2]; transactions shipped January 2025 [5] (gaps today in the table). Zone-aware clients hit same-AZ agents; S3 is the replication, so inter-AZ bytes are near zero [1][6].

### AutoMQ
- A Kafka fork whose log layer is replaced by "S3Stream"; brokers become stateless, KRaft and the compute layer are unmodified, so client compatibility is inherited rather than re-implemented [22][23].
- Writes go to a WAL first ("a write-ahead log optimized for frequent writes and low IOPS to reduce S3 API costs") and upload asynchronously; acks happen "once the data is committed to the EBS WAL" [24]. The WAL is the fork line: "AutoMQ Open Source supports only S3-compatible storage services as WAL storage options," hence "AutoMQ defaults to running on S3 directly, resulting in hundreds of milliseconds of latency. The enterprise version offers single-digit millisecond latency" [22][23]. Multi-AZ with EBS WAL depends on Regional EBS (Azure/GCP) or failover upload [24]. A rack-aware router sends producers to a same-zone broker [25]. Core is Apache 2.0 today (earlier BSL 1.1; change date not found).

### Bufstream (Buf, acquired by CoreWeave May 2026)
- "Bufstream brokers are leaderless"; needs "a metadata store (Postgres, Google Cloud Spanner, or etcd)" plus S3/GCS/Azure; any broker in any zone accepts writes, batched to S3 and acked after persistence [26][25]. Schema-aware: brokers "directly connect to your schema registry," validate with Protovalidate, reject batches or DLQ records [26], at the cost that "the brokers now have more work to do because it consumes more CPU" [27].
- "CoreWeave has acquired Bufstream" and "has added Bufstream to its internal platform as part of the W&B Models and Weave product lines." Product pages now redirect to that notice; nothing is said about pricing or continued availability [28].

### Confluent Freight / Kora
- "Freight clusters write data directly to object storage, such as Amazon S3, and bypass local broker storage." "Freight clusters have higher latency than other cluster types, on the order of seconds instead of milliseconds." Select AWS regions only, no share consumers, compacted partitions capped at 1,000 per eCKU vs 3,000, scaling "roughly 4 eCKU every 10 minutes" [10]. Fully proprietary [9].

### KIP-1150 diskless topics (Aiven, upstream Kafka)
- Accepted March 2, 2026 "with overwhelming support of 9 binding votes and 5 non-binding ones"; Slack's rival KIP-1183 authors "dropped their KIP and officially supported KIP-1150" [30]. It is an umbrella; KIP-1163 (core) and KIP-1164 (coordinator) carry the design.
- Leaderless produce: any broker batches many partitions into one object, then "The BatchCoordinator acts as a sequencer and persists the offsets in its backing store" (default a `__diskless-metadata` topic materialized in SQLite; Inkless uses Postgres) [31]. The KIP says only "Diskless Topics will incur higher produce latency"; brokers keep disks for KRaft, batch metadata and cache [29]. Compaction is deferred to tiered storage (KIP-1272) [29].

### Apache Kafka tiered storage (KIP-405)
- "Currently, tiered storage is not available for compacted topics"; only one partition's remote data is served per Fetch, which "can limit client throughput" [33]. It removes retention from brokers but not replication: cross-AZ traffic and the leader hot path remain [11].

### S2.dev
- Not Kafka-compatible by design: records, streams, basins; APPEND/READ/TRIM with fencing tokens. "all writes will be safe in S3 with regional durability before being acknowledged." Standard p99 <500 ms; Express (quorum of three S3 Express One Zone buckets) p99 <50 ms; 125 MiB/s write per stream [34]. Usage-only pricing (see Cost). S2-lite (MIT, Rust, SlateDB) shipped 2026; a Kafka layer is "planned."

### Uber federation (context)
- Regional plus aggregate clusters; uReplicator copies data so "Each cluster contains aggregated data from all other regions," with an offset manager because aggregation scrambles order [37].

## Strengths
- **Cross-AZ elimination.** Replication becomes a property of the object store. WarpStream: inter-zone traffic "can be over 80% of the infrastructure cost of a Kafka deployment at scale" [1]; Kozlovski's independent KIP-1150 analysis agrees networking goes "from 83%+ of cost to nearly zero" on AWS [31].
- **Ops simplicity and elasticity.** Stateless nodes scale in seconds with no reassignment (WarpStream scaled 6 to 3 to 6 agents mid-benchmark [6]; AutoMQ "can scale in/out within seconds" [22]). No ZooKeeper, no JVM, one binary.
- **Durability by construction.** S3's durability replaces Raft/ISR for data; "Deleting files in S3 is free" [2] and retention becomes a price, not a capacity plan.
- **Redpanda's local-disk path** keeps single-digit-ms p99 and is the only design here that offers both modes in one binary [19].
- **Bufstream's broker-side schema enforcement:** "Breaking schemas will never enter, and only code-reviewed, compatible schemas will be shipped" [27].

## Pain points

| System | Pain | Evidence (quote + link + date) | Severity 1-5 | Root cause |
|---|---|---|---|---|
| WarpStream | ~1 s end-to-end latency on S3 Standard | "Engineering is about trade-offs, and we've made a significant one with WarpStream: latency." [1] 2023-07-25; HN bothra90: "p99 of 1s can be too high for some use-cases"; founder: "if you need latency that low WarpStream is not a good fit" [2] 2023-08-07 | 4 | Ack only after S3 PUT; batching to amortize PUT cost |
| WarpStream | Vendor-hosted metadata store is a dependency and SPOF | HN chaotic-good: metadata database is "a bottleneck"; banditelol: "surprised there's no option for self hosting metadata on the free tier" [2] 2023-08-07; Ripcord Mode added in 2026 so agents "continue processing writes during Control Plane outages" [5] | 3 | Sequencing centralized in SaaS control plane |
| WarpStream | Cost claims inflated; price rose after acquisition | "the actual Kafka cost is overstated by a factor of three (2.8x)"; "Post-acquisition, WarpStream increased their pricing 1.5x-2.5x"; "It's simpler. But not cheaper." [7] 2024-12-13; "In two out of the three public clouds, WarpStream is actually not cheaper than running Kafka yourself" [8] 2024-12-13 | 3 | Calculator assumptions; control-plane fee is 59% of WarpStream's own quoted bill [6] |
| WarpStream | Closed core, acquired twice (Confluent 2024, IBM 2026) | HN enether: "Net win for both companies, probably a loss for customers though. Warpstream was aggressively commoditizing Kafka" [9] 2024-09-10; "Public information does not indicate that customers can audit, modify, self-maintain, or fork" [38] 2026-05-03 (competitor source) | 3 | Proprietary agents plus hosted control plane |
| WarpStream | Compatibility gaps, long tail of differences | "We do not expose support for transactions or idempotent producing today" [2] 2023; now "Throttling: Not enforced," tagged fields ignored, 15 s request cap [4] | 2 | Stateless re-implementation of a stateful protocol |
| Redpanda | Benchmark credibility wars; degrades with many producers and after retention | "Redpanda's claims regarding Kafka and even their own performance are greatly exaggerated"; Kafka run with `log.flush.interval.messages=1`; "end-to-end latencies reaching 24 seconds" at 50 producers; "deteriorated significantly when run for more than 12 hours" [12] 2023-05-15; HN purpleblue on the Confluent-employed critic: "I don't think we can get a less reliable or trustworthy set of performance tests" [13] | 3 | Both sides vendor-funded; fsync per batch across 288 files; NVMe write amplification |
| Redpanda | Fixed per-shard memory; OOM under load | Flush tasks "accumulate without bound, eventually causing the shard to run out of memory" (sev/high, v23.1.1) [17] 2023-03-17; "If insufficient memory is available, topic operations will fail" [16] | 3 | Seastar pre-allocates per core; no page cache to absorb bursts |
| Redpanda | BSL gating, license confusion, cloud trust and tier churn | Tiered Storage, RBAC, Kerberos Enterprise-only [16]; Connect repo: license changes "are not clearly stated" (GitHub #2621, 2024); HN RadiozRadioz on the GCP outage post: "Nothing about the way they architected their system even mattered in this incident." [20] 2025-06-21; Serverless Standard retired 2025-08-30 [21] (snippet only, page 404'd) | 2 | Source-available business model; marketing outran engineering |
| AutoMQ | Open-source edition is slow; fast path is paid | "AutoMQ defaults to running on S3 directly, resulting in hundreds of milliseconds of latency. The enterprise version offers single-digit millisecond latency." [22] | 3 | EBS WAL and control plane withheld from OSS |
| AutoMQ | Maturity, unverified claims, vendor-authored comparisons | HN metadat: "They're making a lot of claims and in the readme present zero measurements." [24] 2024-04-08; README: Quick Start "is not suitable for production use" | 3 | Young fork, thin independent community |
| AutoMQ | EBS WAL is single-AZ unless Regional EBS exists | "If an EBS volume goes offline, we promptly upload all data to S3 and close all partitions on the affected broker" [24] 2024 | 3 | Durability window between WAL ack and S3 upload |
| Bufstream | Vendor exit into CoreWeave's internal platform; extra infra and CPU | "CoreWeave has acquired Bufstream, our Apache Kafka-compatible streaming platform, from Buf" [28] 2026-05-08, product/docs pages redirect; needs Postgres/Spanner/etcd [26]; "the brokers now have more work to do because it consumes more CPU" [27] 2025-10-23 | 4 | Small vendor, narrow go-to-market; externalized metadata |
| Confluent Freight | Seconds of latency, feature gaps, slow scaling | "on the order of seconds instead of milliseconds"; "don't support share consumers"; "roughly 4 eCKU every 10 minutes" [10] | 3 | Same S3 batching; SaaS capacity provisioning |
| KIP-1150 | Design incomplete; coordinator and cache unspecified; latency | "Latency suffers massively in this Diskless architecture"; Aiven docs show "up to 2.4 seconds" end-to-end [31] 2025-06-26; Inkless lacks transactions and compaction [32] | 3 | Umbrella accepted before sub-KIPs; SQLite coordinator is a serialization point |
| Kafka tiered storage | No compacted topics; fetch throughput limited | "tiered storage is not available for compacted topics"; one remote partition per Fetch "can limit client throughput" [33] | 3 | Bolt-on to leader/ISR; replication cost unchanged |
| S2 | Not Kafka-compatible; closed core; young | HN evantbyrne: "Such a bummer that the it is not source available."; founder: "the system is young, and there will be kinks" [34][35] 2024-12 | 2 | New API, startup risk |

## Why people adopt and later complain

**Adoption drivers.** Keep your Kafka clients, delete the cross-AZ bill, stop doing partition reassignments at 3 a.m. WarpStream's HN launch hit 392 points because it put a dollar figure on inter-AZ traffic most teams had never isolated [2]. Redpanda sells "no ZooKeeper, no JVM, one binary" plus low tail latency on NVMe [15]. AutoMQ wins where teams want the economics but refuse closed source. Bufstream won lakehouse teams who wanted broker-side schema enforcement and Iceberg output.

**Complaints afterwards.**
1. *Latency surprises.* "Kafka compatible" read as "Kafka latency." S3-native p99 is 500 ms to 2 s unless you pay for S3 Express or an EBS WAL; as one HN commenter put it, "Object storage, simply by the overhead of HTTP + SSL, has higher latency" [11]. Every vendor has since shipped a low-latency tier (Lightning Topics, AutoMQ enterprise WAL, S2 Express), which quietly reintroduces zone-local storage and its cost.
2. *Smaller bill, not 10x smaller.* WarpStream's own TCO post shows $5,327 infrastructure but $12,868 with control-plane fees, against a Kafka baseline others call 2.8x inflated [6][7]. On Azure, where cross-AZ is free, diskless "does not materially reduce the bill" [31].
3. *Ownership churn.* In 24 months WarpStream went startup to Confluent to IBM, Bufstream went Buf to CoreWeave's internal platform, Redpanda retired Serverless Standard. Protocol compatibility protects client code, not pricing, roadmap or access to your data's format.
4. *Compatibility long tail.* Transactions, idempotence, compaction, quotas and timestamp retention are where re-implementations diverge [4][32][33]; Kafka Streams, Connect and MirrorMaker expose them.
5. *New single points.* A hosted sequencer (WarpStream), Postgres/etcd (Bufstream), a SQLite-backed coordinator (KIP-1150), a fixed per-shard memory budget (Redpanda): each swaps Kafka's familiar failure modes for unfamiliar ones.
6. *Vendor-authored noise.* Nearly every "X vs Y" search result is written by a competitor (AutoMQ's blog dominates); independent practitioner write-ups are scarce.

## Cost

Published comparisons (AWS, roughly 1 GiB/s unless noted):
- **WarpStream (Mar 2024):** self-hosted Kafka "$54,591/month in cloud infrastructure costs alone" (3-day retention; $94,134 at 7 days), of which $27,288 is inter-AZ. WarpStream $5,327 infra + $7,541 control plane = $12,868, "more than 4x cheaper" [6].
- **Bufstream:** EBS $42,025/month vs $4,625 object storage; cross-AZ $34,732 vs $226; "8x less expensive to operate" including license (vendor figures via search summaries; original post now redirects) [26].
- **AutoMQ:** "how we cut Apache Kafka billing by 90%"; "over 80% of costs arise from cross-AZ traffic" [22].
- **KIP-1150 independent estimate:** 500 MB/s, 7-day retention on AWS drops "from about $1.05M/yr to about $200k/yr, roughly 5x"; object storage ~20x cheaper than EBS; no saving on Azure; not cheaper at low throughput because of PUT overhead [31].
- **S2:** $0.05/GiB in, $0.05/GiB-month stored, $0.05–0.10/GiB out; "no fixed costs in S2 like instances or cluster units" [34][36].

Skeptical takes:
- Kozlovski rebuilt WarpStream's calculator: disks overestimated ~6.66x (gp2 vs HDD, free-space padding), instances ~5x, a replication multiplier "apparently doubled," compression changed from 5:1 to 4:1. Net: Kafka overstated 2.8x (5.6x if tuned); WarpStream cheaper on AWS "only by 30%," not cheaper on GCP/Azure [7][8].
- HN on "5-10x cheaper": michaelcampbell: "'1x cheaper' would be free" [2]; on the S3 Express tier, solatic: "you have little reason not to use Kafka, which scales costs within a single AZ without a problem" [11].
- What both sides agree on: cross-AZ egress is the dominant Kafka cost on AWS/GCP, so savings scale with throughput and with the networking share of your bill. Below a few MB/s, PUT costs and control-plane fees dominate and savings vanish [31].

## Design lessons for a new platform

- **Offer both storage tiers per topic, in one binary, from day one.** The market converged on "local-disk topic for ms latency, object-storage topic for cheap bulk" (Redpanda Cloud Topics, WarpStream Lightning, AutoMQ WAL, S2 Express, KIP-1150 beside classic topics). Make tier a topic property that can change online, and keep one offset space so consumers never notice.
- **Make the ack point explicit and configurable.** Three durability levels: (a) zone-local WAL quorum on NVMe/EBS, ~2–10 ms; (b) S3 Express One Zone quorum across zones, ~30–50 ms; (c) S3 Standard, ~300–600 ms. Price them separately and publish exact failure semantics; most pain above is a gap between what the ack meant and what users assumed.
- **Never let the local WAL become a single-AZ hole.** AutoMQ's EBS WAL and KIP-1150's staging disks both have a window between ack and upload. Replicate the WAL to a second zone before acking, or use an S3 Express quorum; do not depend on Regional EBS.
- **Batch across partitions, index per partition.** Every S3 design writes mixed-partition L0 objects to amortize PUTs, then needs compaction into per-partition L1 files and a GC story (Redpanda Reconciler, WarpStream compaction, AutoMQ compaction). Design both layers and garbage collection into the object format up front.
- **Own the metadata store; let customers run it.** The most repeated worry is a vendor-hosted sequencer. Ship the coordinator as an embeddable Raft group with an optional hosted mode; never make hosted-only the free tier. Keep offset assignment on a local quorum, off the S3 critical path.
- **Shard per core, but budget globally.** Thread-per-core wins tail latency, yet "2 GB per core, 2 MB per partition, operations fail otherwise" and the unbounded-flush OOM show the price. Give shards a shared spill/backpressure budget so a hot shard sheds load instead of aborting.
- **Make cross-AZ bytes a first-class metric.** Zone-aware produce and fetch routing is table stakes; also report inter-AZ bytes per topic so users can see the saving they were promised.
- **Define "Kafka compatible" with a published matrix.** Transactions, idempotent producers, compaction, quotas, timestamp retention, share groups, Connect/Streams are where everyone diverged. Either keep Kafka's compute layer (AutoMQ) or put the gap table on the front page.
- **Compaction must work on the object tier.** Tiered storage, Freight and Inkless all exclude or cap compacted topics; make key-based compaction part of the L1 rewrite step, not a broker-local pass.
- **Price against real Kafka, with an open calculator.** Control-plane fees were 59% of WarpStream's own bill and the independent verdict was "simpler, but not cheaper." Use S2-style per-GiB usage pricing and editable Kafka assumptions; credibility is the scarce resource.
- **Open the data plane, charge for the control plane.** BSL gating, closed agents, Bufstream's disappearance and S2's closed core are all cited as trust problems; AutoMQ's move to Apache 2.0 is the counter-example users point to.
- **Benchmark long and reproducibly.** Vanlightly's findings came from 24–36 hour runs past the retention limit with 50 producers; vendor runs were short and favorable. Publish configs and run past retention.
- **Treat multi-cluster and migration as product features.** Uber's federation layer and WarpStream's producer-migration proxy show that moving workloads is the hardest part of adoption; build offset-preserving mirroring and protocol-level redirect from the start.

## Sources

1. WarpStream, "Kafka Is Dead, Long Live Kafka" — https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka — 2023-07-25
2. Hacker News, "Kafka is dead, long live Kafka" (392 pts, 287 comments) — https://news.ycombinator.com/item?id=37036291 — 2023-08-07
3. WarpStream docs, Architecture — https://docs.warpstream.com/warpstream/overview/architecture — read 2026-10-19
4. WarpStream docs, Protocol and Feature Support — https://docs.warpstream.com/warpstream/kafka/reference/protocol-and-feature-support — read 2026-10-19
5. WarpStream changelog — https://www.warpstream.com/changelog — entries 2024-02 to 2026-07
6. WarpStream, "Benchmarks and TCO" — https://www.warpstream.com/blog/warpstream-benchmarks-and-tco — 2024-03-05
7. S. Kozlovski, "The Brutal Truth about Kafka Cost Calculators" — https://bigdata.2minutestreaming.com/p/the-brutal-truth-about-apache-kafka-cost-calculators — 2024-12-13
8. S. Kozlovski, "The real Apache Kafka cost" — https://blog.2minutestreaming.com/p/the-real-apache-kafka-cost — 2024-12-13
9. Hacker News, "WarpStream joined Confluent" — https://news.ycombinator.com/item?id=41493178 — 2024-09-09
10. Confluent Cloud docs, Cluster types (Freight) — https://docs.confluent.io/cloud/current/clusters/cluster-types.html — read 2026-10-19
11. Hacker News, "Tiered storage won't fix Kafka" — https://news.ycombinator.com/item?id=40199118 — 2024-04-29
12. J. Vanlightly, "Kafka vs Redpanda Performance - Do the claims add up?" — https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up — 2023-05-15
13. Hacker News thread on [12] — https://news.ycombinator.com/item?id=35949771 — 2023-05-15
14. Hacker News, Redpanda "Why fsync()" — https://news.ycombinator.com/item?id=35959482 — 2023-05-16
15. Redpanda docs, Architecture — https://docs.redpanda.com/current/get-started/architecture/ — read 2026-10-19
16. Redpanda docs, Production requirements — https://docs.redpanda.com/current/deploy/redpanda/manual/production/requirements/ — read 2026-10-19
17. Redpanda GitHub issue #9533, "OOM and possible leak under load" — https://github.com/redpanda-data/redpanda/issues/9533 — 2023-03-17
18. Redpanda, "Redpanda is now free and Source Available" — https://www.redpanda.com/blog/open-source — 2020-11-12
19. Redpanda, "Under the hood: Cloud Topics architecture" — https://www.redpanda.com/blog/cloud-topics-architecture — 2026-03-30
20. Hacker News, "Redpanda Cloud's response to the GCP outage" — https://news.ycombinator.com/item?id=44338089 — 2025-06-21
21. Redpanda, Serverless Standard deprecation FAQ — https://www.redpanda.com/blog/serverless-standard-deprecation-faq — 2025 (fetch returned 404; details from search snippet, unverified)
22. AutoMQ GitHub README — https://github.com/AutoMQ/automq — read 2026-10-19
23. AutoMQ docs, Architecture overview — https://docs.automq.com/automq/architecture/overview — read 2026-10-19
24. Hacker News, "Show HN: AutoMQ" — https://news.ycombinator.com/item?id=39966114 — 2024-04-08
25. V. Trinh, "Deep dive into the challenges of building Kafka on top of S3" — https://vutr.substack.com/p/deep-dive-into-the-challenges-of — 2025-05-08
26. Bufstream docs (mirror) — https://docs.bufbuild.ru/bufstream/ — read 2026-10-19
27. V. Trinh, "Is Your Data Valid? Why Bufstream Guarantees What Kafka Can't" — https://vutr.substack.com/p/is-your-data-valid-why-bufstream — 2025-10-23
28. Buf, "CoreWeave acquires Bufstream" — https://buf.build/blog/coreweave-acquires-bufstream — 2026-05-08
29. Apache Kafka wiki, KIP-1150: Diskless Topics — https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics — updated 2026-03-02
30. Aiven, "KIP-1150 Accepted, and the Road Ahead" — https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead — 2026-03
31. S. Kozlovski, "KIP-1150 brings 20x cheaper storage..." — https://blog.2minutestreaming.com/p/diskless-kafka-topics-kip-1150 — 2025-06-26
32. The Geek Narrator, "Diskless - The Cloud-Native Evolution of Kafka" — https://www.geeknarrator.com/blog/diskless-warpstream-confluentfreight — 2025-06-09
33. Apache Kafka wiki, Tiered Storage GA Release Notes — https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Tiered+Storage+GA+Release+Notes — read 2026-10-19
34. S2, "Introducing S2" — https://s2.dev/blog/intro — 2024-12-20
35. Hacker News, "Introducing S2" (372 pts, 195 comments) — https://news.ycombinator.com/item?id=42480105 — 2024-12-21
36. S2 pricing — https://s2.dev/pricing — read 2026-10-19
37. InfoQ, "Uber multi-region Kafka" — https://www.infoq.com/news/2021/01/uber-multi-region-kafka/ — 2021-01-04
38. AutoMQ (competitor blog), "WarpStream after Confluent acquisition" — https://www.automq.com/blog/warpstream-after-confluent-acquisition-what-changed — 2026-05-03
