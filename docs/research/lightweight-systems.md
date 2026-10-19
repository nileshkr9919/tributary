# Lightweight and developer-first systems

Research date: 2026-10-19. Scope: NATS / JetStream, Redis Streams and Pub/Sub (plus Valkey), NSQ, Apache Iggy, Fluvio, Memphis.dev, and ZeroMQ/nanomsg. Sources are numbered in brackets. Claims that could not be verified from a fetched source are marked. Reddit threads could not be fetched in this session (reddit.com is blocked by the fetch tool and search returned no Reddit results), so community sentiment comes from Hacker News, GitHub issues and engineering blogs.

## Architecture

### NATS core and JetStream

Core NATS is a subject-based router. A subject "is just a string of characters that form a name the publisher and subscriber can use to find each other"; `.` builds a hierarchy, `*` "will match a single token", and `>` "will match one or more tokens, and can only appear at the end of the subject" [14]. Core NATS "delivers messages only to subscribers connected at the moment of publication - at most once, never replayed" [15].

JetStream adds persistence. A stream is "a server-side store of messages, bound to one or more subjects" (file or memory storage); a consumer is "a server-side, stateful view of a stream that tracks how far a client has progressed" [15]. Consumers are durable or ephemeral, pull (client asks for a batch) or push; ack policies are none, all or explicit, with redelivery after `AckWait` up to `MaxDeliver`. Retention: `LimitsPolicy` ("Retention based on the various limits... MaxMsgs, MaxBytes, MaxAge, and MaxMsgsPerSubject"), `WorkQueuePolicy` ("Each message can be consumed only once"), `InterestPolicy` (deleted once "ack'ed by all consumers filtering on the subject") [13]. Replicas go up to 5; each stream replica *and each consumer replica* is its own Raft group [10]. A mirror "can source its messages from exactly one stream"; sources pull "from one or more streams concurrently" [13]. KV and Object Store are built on streams; the object store chunks large objects [15].

Topology: clusters; gateways "enable connecting one or more clusters together into a full mesh" (superclusters) and forward only subjects with interest [14]; leaf nodes "extend an existing NATS system of any size", are "useful in IoT and edge scenarios" and "do not need to be reachable themselves" [14].

### Redis Streams and Pub/Sub (and Valkey)

A stream is an append-only radix tree keyed by `<ms>-<seq>` IDs: `XADD` appends, `XRANGE` reads by ID, `XREAD BLOCK` tails. Consumer groups (`XREADGROUP`) keep a Pending Entries List (PEL) of "messages that were delivered to some consumer of the consumer group, but are yet to be acknowledged" [16]. `XACK` clears an entry; `XPENDING`/`XCLAIM`/`XAUTOCLAIM` take over entries from dead consumers, "otherwise the server will leave the messages pending forever" [16]. `MAXLEN ~` trims approximately [16]. Redis 8.2–8.8 added group-aware deletion, `XREADGROUP ... CLAIM`, idempotent production and `XNACK` [16][22].

Persistence is the standard Redis model: RDB snapshots (lose "the latest minutes of data" on crash) or AOF with `everysec` (lose up to one second) or `always` ("very very slow, very safe") [17]. The streams docs warn: "AOF must be used with a strong fsync policy if persistence of messages is important", replication is asynchronous, and failover "may promote a replica that lacks some data" [16]. A stream is one key, so it lives on one shard and one thread. Pub/Sub "exhibits at-most-once message delivery semantics... the message is forever lost" if the subscriber is down [18]. Valkey is the Linux Foundation fork of the last BSD Redis, "an open source (BSD) high-performance key/value datastore", latest 9.1.2 [20].

### NSQ

Go, shared-nothing. `nsqd` nodes "share nothing"; messages stay "primarily in memory" and overflow to disk; `nsqlookupd` is an eventually consistent discovery service whose nodes "do not coordinate"; `nsqadmin` is the UI [24]. Topics fan out to channels; channels are competing-consumer queues.

### Apache Iggy

Rust, "a persistent message streaming platform" over QUIC, TCP, WebSocket and HTTP [27]. Since 0.6.0 it is thread-per-core on `compio`/io_uring with "partitions assigned to CPU-pinned shards" and a preallocated memory pool [28]. Clustering uses Viewstamped Replication, is "disabled by default" [27], and shipped in 0.9.0 (2026-09-18) [28]. Both durability policies default to `replicated`, meaning "VSR quorum commit without waiting for stable storage"; `persisted` is stronger [27]. Incubator early 2025, top-level project August 2026 [28].

### Fluvio

Rust, from InfinyOn. A Streaming Controller "manages the SPU life cycle and optimizes the distribution of data streams"; Stream Processing Units store partitions as immutable ordered logs with leader/follower replication: "The leader receives the data from producers and forwards a copy to followers" [30]. SmartModules are Rust-compiled WebAssembly filters/maps/aggregates run inside the SPU [31]. Targets "edge networks and IOT devices", including Raspberry Pi [30]. Apache-2.0, ~5.3k stars [31].

### Memphis.dev

A Go broker with UI, schema management and dead-letter features, "simple as RabbitMQ, robust as Apache Kafka", $5.5M seed in June 2023 [33]. It was widely described as a modified NATS JetStream; I could not confirm that from the current README, which no longer mentions NATS [32].

### ZeroMQ / nanomsg / NNG

Brokerless socket libraries (PUB/SUB, REQ/REP, PUSH/PULL over TCP/IPC/inproc) with no persistence: "persistence, retransmission, and higher-level delivery guarantees must be implemented at the application layer" [34].

## Strengths

- **One static binary.** NATS, NSQ, Iggy and Fluvio each ship one. Even a durability-sceptical reviewer credits NATS with "a single binary, sub-second startup, modest resource needs, and a clean Go API" [12]; NSQ is "super easy to run. A few command line params" [25].
- **Subject hierarchy and wildcards.** `*` and `>` give routing, tenancy and filtering without topic sprawl; JetStream consumers filter the same way [13][14].
- **Edge and IoT.** Leaf nodes and superclusters keep traffic local and forward only interested subjects [14]; Fluvio targets Raspberry-Pi-class devices [30].
- **Latency.** Core NATS and ZeroMQ are the floor; Iggy reports P9999 of 6.51 ms vs 34 ms after its io_uring rewrite (vendor numbers) [28].
- **Batteries included or already installed.** JetStream bundles KV, object store, request/reply and auth; Redis Streams ride on infrastructure most teams already run [23].
- **Developer experience.** Redis commands learn in an afternoon; Iggy ships SDKs for Rust, C#, Java, Python, Node and Go [27].

## Pain points

| System | Pain | Evidence (quote + link + date) | Severity 1-5 | Root cause |
|---|---|---|---|---|
| NATS JetStream | Acks before fsync; default flush every 2 minutes | "NATS calls fsync... only once every two minutes, but acknowledges messages immediately"; a simulated power failure lost "131,418 of 930,005 messages"; Jepsen asked NATS to "change the default value of fsync to always, or clearly document these hazards" (docs updated, default unchanged). https://jepsen.io/analyses/nats-2.12.1 (2025-12-08) [6]. HN: "I wouldn't trust a product that doesn't default to safest options" (rdtsc) [7] | 5 | Performance-first default that breaks durable-before-ack |
| NATS JetStream | Corrupted files on a minority of nodes lose writes and split-brain | "679,153 of 1,367,069 acknowledged writes were lost" after bitflips on a minority; snapshot corruption made nodes delete a stream; issues #7549/#7556/#7567 unresolved [6] (2025-12-08) | 4 | Recovery trusts local state over quorum |
| NATS JetStream | Crashes alone deleted whole streams (2.10.20–2.10.22) | "process crashes alone could cause the total loss of a JetStream stream"; fixed in 2.10.23 [6] | 5 (fixed) | State-recovery bug |
| NATS JetStream | Loss on quorum loss; issue went stale | "We are experiencing message loss... whenever quorum is lost"; reply: "Could you update to the latest server? 2.10.23?"; stale Feb 2025, paid support suggested. https://github.com/nats-io/nats-server/issues/6236 (2024-12-10) [8] | 3 | Catch-up edge cases; thin free support |
| NATS JetStream | Work-queue + R3 + max-deliver dropped messages | "11 out of 20 messages are missing... neither ack'ed nor TTL'ed"; fixed in PR #7845. https://github.com/nats-io/nats-server/issues/7817 (2026-02-11) [9] | 3 (fixed) | Retention/redelivery interaction |
| NATS JetStream | Raft group per stream *and* per consumer | "Each stream replica and consumer replica is a separate Raft group"; "Servers with 1,000+ HA assets on non-NVMe storage are prime candidates for IPQ backpressure"; advice: consolidate streams [10]. HN: "Jetstream is even more limited than most Kafkas on number of streams" (shikhar, 2025-08-23) [11] | 4 | Consensus per object; meta-leader serializes API calls |
| NATS JetStream | 1 MB default message cap; docs gaps; soft exactly-once | "Jetstream's maximum message size of 1MB is a showstopper" (atombender, 2025-09-01) [11]; Jepsen skipped membership tests because docs were "incorrect and incomplete" [6]; exactly-once is message-ID dedupe within a time window plus double-ack (not verified at scale here) | 3 | Defaults and docs lag the server |
| NATS (governance) | 2025 Synadia/CNCF relicensing scare | Synadia sought "full control of the nats.io domain name and GitHub repository" and a BSL relicense (2025-03/04) [2]; HN: "Really unsure about the viability of NATS as an open source product if Synadia stops contributing... making me reconsider our architecture change" (d4mr, 2025-04-24) [4]; Zaitsev: "it could shake people's trust in... foundation-governed open source projects" (2025-04-30) [5]. Outcome: Synadia "decided not to fork the NATS Server under the Business Source License"; trademarks to the Linux Foundation; Apache-2.0 continues (2025-05) [1][3] | 3 (resolved) | Single vendor (~97% of commits) with weak foundation trademark control |
| Redis Streams | Memory-bound, no tiering; trimming not ack-aware | "all messages that fall under that range will be deleted, whether they were processed, acknowledged, read or not" (Arcjet, 2024-03-26) [23]; one stream = one key = one shard's RAM | 4 | In-memory structure, not a disk log |
| Redis Streams | PEL never expires; memory leak | "a pending entry that nobody acknowledges never goes away... continues to consume memory" (2026-05-26) [22]; issue #8635 open since 2021-03-10 [21] | 4 | No server-side reclaim until 8.4 |
| Redis Streams | Persistence and failover lose data | "AOF must be used with a strong fsync policy"; failover "may promote a replica that lacks some data" [16]; RDB: "lose the latest minutes of data"; `always` is "very very slow" [17] | 4 | Async replication, cache-oriented persistence |
| Redis Streams | DIY reliable-consumer loop | "Each application reimplements the same orchestration logic — often subtly wrong" [22] | 3 (improving) | Primitives instead of a consumer abstraction |
| Redis Pub/Sub | At-most-once only | "the message is forever lost" if the subscriber is down [18] | 2 | By design |
| Redis (licensing) | BSD → RSALv2/SSPL (2024-03) → AGPLv3 (2025-05); Valkey fork | "the change hurt our relationship with the Redis community. SSPL is not truly open source" (2025-05-01) [19]; Valkey "essentially a drop-in replacement for redis... worked without a hitch"; "developers who migrated to Valkey may not return" [36] | 3 | Vendor vs hyperscaler economics |
| NSQ | No ordering, no replication, in-memory by default | "You cannot rely on the order of messages"; "There is no built in replication"; "messages can be delivered multiple times" [24]; "the lack of true persistence and ordering means it's not something I can use" (atombender, 2015-01-26) [25]; "NSQ has no replication support" (arunoda, 2015-03-16) [26] | 4 | Shared-nothing nsqd |
| Memphis.dev | Abandoned by its company | "Memphis.dev is no longer supported officially by the Superstream team (formerly Memphis.dev)... and was released to the public" [32]; pivot to Kafka cost optimisation after a $5.5M seed [33] | 5 | Startup pivot; no independent community |
| Apache Iggy | Immature; clustering just shipped; weak default durability | "Iggy is basically Kafka written in Rust that has yet to reach the distributed, clustered, fault tolerant stage" (genxy, HN, ~2026-09) [29]; default `replicated` = "VSR quorum commit without waiting for stable storage" [27]; no transactions, no compaction, custom protocol, Kafka gateway "not yet part of a release" [27][28]; SoftwareMill: "For every Kafka workload today, no" (2026-09-02) [28] | 3 | Young project, small team |
| Fluvio | Vendor-dependent, thin independent evidence | README notes a "temporary transition to a fluvio-community hosted fluvio build and release" [31]; no independent production reviews found; 20–38x latency claims unverified | 3 | Single-company project |
| ZeroMQ / nanomsg | No persistence; uneven maintenance | nanomsg "only in sustaining mode"; "NNG is the only library under active development" (2025-08-11) [34] | 2 | Library, not a broker |

## Why people choose them and when they regret it

**Why they choose them.** Kafka's weight: Arcjet called Kafka "famously difficult to host and manage, which as a startup isn't desirable" and replaced it with Redis Streams at roughly $1k/year versus six-figure managed quotes [23]. Already installed: Redis is in most stacks; NATS covers pub/sub, request/reply, queues, KV and objects in one binary. Fit: "NATS/Jetstream is amazing if it fits your use case and you don't need extreme scalability" (atombender) [11]; "For our needs NATS Jetstream sounds like the exactly perfect fit... Kafka I already used and rejected" (pdimitar) [11].

**When they regret it.**

- *Durability discovered late.* After Jepsen: "NATS is fantastic for ephemeral messaging, but some of this report is really concerning" (johncolanduoni) [7]; a security-pipeline team concluded "the durability evidence as it stands today disqualifies NATS JetStream from the core path" and kept Kafka (2026-05-25) [12]; one user "ended up implementing from scratch something with the same functionality" (williamstein) [7]. Even a fan would be "seriously anxious if I had TBs of stream data I couldn't afford to lose" [11].
- *Object count, not byte count.* One-stream-per-device designs hit the Raft-per-asset wall and are told to consolidate [10][11].
- *Redis memory maths.* Streams that outlive RAM force trimming that drops unprocessed work, or PEL growth that eats the instance [21][22][23]; Arcjet calls its trimmer "not an exact science" and "a stopgap" [23].
- *Vendor risk.* Memphis users were orphaned [32]; NATS users spent April 2025 reconsidering [4]; Redis users forked to Valkey [36].
- *Ecosystem gap.* No Kafka Connect equivalent; Iggy and Fluvio speak custom protocols and Iggy's Kafka gateway is unreleased [27].

## Cost

- **Software:** free to self-host (Apache-2.0 for NATS, Iggy, Fluvio; BSD for Valkey; MIT for NSQ; Redis 8 under AGPLv3/RSALv2/SSPLv1 [19]).
- **Footprint:** a single node of any of these runs on a $5–20/month VM. Cost drivers differ: Redis Streams pay for RAM (every retained byte and pending entry is in memory); JetStream pays for fast disk. Synadia budgets "2,000-5,000 groups" per NVMe server versus "500-1,000" on cloud SSD and advises against network-attached HDD [10]. Three replicas across AZs is "the minimum required if you want fault-tolerance" [11], so budget 3x.
- **Managed NATS (Synadia Cloud):** Personal free (10 connections, 5 GiB, no HA); Starter $49/mo (100 connections, 2 HA streams, 1 GiB HA); Pro $199/mo (1,000 connections, 10 HA streams, 10 GiB HA); Enterprise custom; add-ons such as HA stream $5/mo and HA storage $0.90/GiB/mo [35]. Search snippets also listed a self-run Kubernetes bundle at $1,250/mo (3 nodes) and an AWS Marketplace listing at $75,000/year; not fetched, so unverified.
- **Managed Redis:** third-party roundups put Redis Cloud Essentials near $5/mo for 250 MB and Pro at $0.014/hour with a $200/mo minimum; unverified against redis.io.
- **Hidden cost:** building what the broker lacks. Arcjet wrote a "Janitor" trimmer plus monitoring [23]; Redis users pre-8.4 wrote reclaim loops [22]; NATS users tune `sync_interval`, placement tags and stream consolidation themselves [6][10].

## Design lessons for a new platform

- **Durable before ack, by default.** Fsync (or a quorum of fsynced replicas) before acknowledging; make lazy mode an explicit opt-in with its loss window stated in the config reference. Use group commit so the safe path is also the fast path, avoiding both NATS's 2-minute default and Redis's "very very slow" `always`.
- **Never trust one replica's local files.** Checksum segments and snapshots, treat corruption as "I know nothing", rebuild from quorum. A bit flip on one node must not become 49% loss or split-brain.
- **No consensus group per logical object.** Multiplex many streams and consumers onto a few replicated shard logs so one-stream-per-tenant is a cheap namespace, and keep metadata off a single serialized meta-leader.
- **Disk-first log, memory as cache, tiering to object storage.** Retention is never bounded by RAM; trimming is ack-aware and never deletes pending work by default.
- **Reliable consumption is a server feature.** Server-tracked positions, automatic reclaim of stalled deliveries, max-deliver, dead-letter routing and consumer TTLs built in, so nobody stitches `XPENDING`/`XCLAIM`/`XREADGROUP` by hand.
- **Ordering per key and replication are table stakes**; NSQ shows the cost of omitting them.
- **Durable idempotency.** Dedupe producer IDs against a persisted table with explicit retention, not only a sliding window.
- **Keep what works:** one static binary, subject hierarchy with `*`/`>` and server-side filtering, built-in KV and request/reply, leaf-node edge bridging, official SDKs in the big six languages at launch.
- **No 1 MB wall.** Configurable limits plus transparent chunking for large payloads.
- **Ecosystem bridge early.** A Kafka-protocol gateway or first-class connectors; its absence is why Iggy, Fluvio and NATS lose evaluations to Kafka and Redpanda.
- **Governance that cannot be clawed back.** Apache-2.0 with trademark, domain and repos foundation-held from day one and more than one maintaining organisation; NATS, Redis and Memphis each cost users trust differently.
- **Publish the failure matrix.** For every durability and replication knob, state the loss window and tested faults; commission a Jepsen-style test before claiming "self-healing" or "linearizable".

## Sources

1. CNCF, "CNCF and Synadia align on securing the future of the NATS.io project", 2025-05-01. https://www.cncf.io/blog/2025/05/01/cncf-and-synadia-align-on-securing-the-future-of-the-nats-io-project/
2. The Stack, "'We want it back' - Synadia tries to claw NATS back from a shocked CNCF", 2025-04-24. https://www.thestack.technology/we-want-it-back-synadia-tries-to-claw-nats-back-from-a-shocked-cncf/
3. Synadia, "Synadia and the NATS project", 2025-05-13. https://www.synadia.com/blog/nats-server-next-steps
4. Hacker News thread on "Protecting NATS and the integrity of open source", 2025-04-24. https://news.ycombinator.com/item?id=43787122
5. Peter Zaitsev, "NATS goes Nuts", 2025-04-30. https://peterzaitsev.com/nats_goes_nuts/
6. Kyle Kingsbury, "Jepsen: NATS 2.12.1", 2025-12-08. https://jepsen.io/analyses/nats-2.12.1
7. Hacker News, "Jepsen: NATS 2.12.1", December 2025. https://news.ycombinator.com/item?id=46196105
8. nats-io/nats-server issue #6236, 2024-12-10. https://github.com/nats-io/nats-server/issues/6236
9. nats-io/nats-server issue #7817, 2026-02-11. https://github.com/nats-io/nats-server/issues/7817
10. Synadia Insights, "NATS Raft IPQ Backpressure", undated, accessed 2026-10-19. https://www.synadia.com/insights/checks/nats-raft-ipq-backpressure
11. Hacker News, "Why was Apache Kafka created?", 2025-08-23 to 2025-09-03. https://news.ycombinator.com/item?id=44999442
12. Security Data Works, "NATS JetStream: lightweight Kafka alternative, disqualified by durability", 2026-05-25. https://securitydataworks.com/writing/pipelines/nats-jetstream/
13. NATS docs, "Streams" (raw), accessed 2026-10-19. https://raw.githubusercontent.com/nats-io/nats.docs/master/nats-concepts/jetstream/streams.md
14. NATS docs, "Subjects", "Leaf Nodes", "Gateways" (raw), accessed 2026-10-19. https://raw.githubusercontent.com/nats-io/nats.docs/master/nats-concepts/subjects.md ; .../running-a-nats-service/configuration/leafnodes/README.md ; .../gateways/README.md
15. NATS docs, "JetStream", accessed 2026-10-19. https://docs.nats.io/nats-concepts/jetstream
16. Redis docs, "Redis Streams", accessed 2026-10-19. https://redis.io/docs/latest/develop/data-types/streams/
17. Redis docs, "Redis persistence", accessed 2026-10-19. https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
18. Redis docs, "Redis Pub/sub", accessed 2026-10-19. https://redis.io/docs/latest/develop/pubsub/
19. Rowan Trollope, "Redis is now available under the AGPLv3 open source license", 2025-05-01. https://redis.io/blog/agplv3/
20. Valkey project site, accessed 2026-10-19. https://valkey.io/
21. redis/redis issue #8635, opened 2021-03-10, backlogged 2025-07-23. https://github.com/redis/redis/issues/8635
22. Sergey Georgiev, "Single-shot reliable consumers with XREADGROUP CLAIM in Redis 8.4", 2026-05-26. https://redis.io/blog/single-shot-reliable-consumers-with-xreadgroup-claim-in-redis-84/
23. Danni Popova (Arcjet), "Replacing Kafka with Redis Streams", 2024-03-26. https://arcjet.com/blog/replacing-kafka-with-redis-streams
24. NSQ docs, "Features & Guarantees", accessed 2026-10-19. https://nsq.io/overview/features_and_guarantees.html
25. Hacker News, "NSQ – A realtime distributed messaging platform", 2015-01-26. https://news.ycombinator.com/item?id=8947354
26. Hacker News, "Adventures in message queues", 2015-03-16. https://news.ycombinator.com/item?id=9209516
27. apache/iggy README, accessed 2026-10-19. https://github.com/apache/iggy
28. SoftwareMill, "Apache Kafka vs Apache Iggy: Same Log, Different Engine", 2026-09-02 (updated 2026-09-18). https://softwaremill.com/apache-kafka-vs-apache-iggy-same-log-different-engine/
29. Hacker News, "Apache Iggy... graduates to an Apache TLP", ~2026-09. https://news.ycombinator.com/item?id=49510540
30. Fluvio docs, "Architecture overview", accessed 2026-10-19. https://www.fluvio.io/docs/fluvio/concepts/architecture/overview
31. infinyon/fluvio README, accessed 2026-10-19. https://github.com/infinyon/fluvio
32. superstreamlabs/memphis README (discontinuation notice), accessed 2026-10-19. https://github.com/memphisdev/memphis
33. TechCrunch, "Memphis.dev looks to simplify event streaming...", 2023-06-28. https://techcrunch.com/2023/06/28/memphis-dev-looks-to-simplify-event-streaming-with-open-source-and-saas-products/
34. La Corte, Rashid, Dan, "Performance Evaluation of Brokerless Messaging Libraries", arXiv:2508.07934, 2025-08-11. https://arxiv.org/html/2508.07934v1
35. Synadia Cloud pricing, accessed 2026-10-19. https://docs.synadia.com/cloud/pricing
36. The Register forums, "Redis 'returns' to open source with AGPL license" (reader comments), 2025-05-01. https://forums.theregister.com/forum/all/2025/05/01/redis_returns_to_open_source/
