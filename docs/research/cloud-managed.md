# Cloud managed messaging

Research date: 2026-10-19. Scope: Amazon SQS, SNS, Kinesis Data Streams, EventBridge; Google Cloud Pub/Sub (and the Pub/Sub Lite retirement); Azure Service Bus, Event Hubs, Event Grid; a short note on Cloudflare Queues and the new Cloudflare K2. Prices are US regions; figures a vendor page would not render are marked "unverified" with a dated secondary source. Reddit could not be fetched from this environment, so community sentiment comes from Hacker News (Algolia API), Microsoft Q&A, DEV Community, and engineering blogs. No quotes are invented; each is linked and dated.

## Architecture and semantics

### Amazon SQS (standard and FIFO)
A queue with a lease model: `ReceiveMessage` hides a message for a visibility timeout (default 30 s, max 12 h); `DeleteMessage` is the ack. Retention 60 s to 14 days. Max payload became 1 MiB in August 2025 (was 256 KiB); larger payloads use the S3 extended client, up to 2 GB [15][11]. Standard queues are at-least-once, best-effort ordered, "nearly unlimited" throughput. FIFO queues order per `MessageGroupId` with a 5-minute deduplication window [13]. FIFO defaults to 300 TPS per API action (3,000 msg/s with batches of 10); high-throughput mode reaches 70,000 TPS in us-east-1 only if work is spread across many group IDs, since one group is processed one message at a time [12][11]. Dead-letter queues via redrive policy; no replay once deleted. July 2025 "fair queues" let `MessageGroupId` on a standard queue act as a tenant key so one noisy tenant does not raise everyone's dwell time [14].

### Amazon SNS
Push fan-out to SQS, Lambda, HTTP, Firehose, email, SMS. Attribute filtering is free; payload filtering is billed per KB scanned [27]. Messages 256 KiB default, 1 MiB per topic on request. FIFO topics: 300 msg/s per group, 3,000 msg/s per topic by default; the account publish quota is 30,000 msg/s in us-east-1 but 300 msg/s in "all other" regions [26]. Archive and replay exist only for FIFO topics.

### Amazon Kinesis Data Streams
A partitioned log. Each shard takes 1 MB/s or 1,000 records/s of writes and serves 2 MB/s of reads shared by all `GetRecords` consumers, capped at 5 `GetRecords` calls/s per shard [2]. Enhanced fan-out (EFO) gives each registered consumer its own 2 MB/s per shard; 20 consumers per stream (50 in On-demand Advantage). Records up to 10 MiB; retention 24 h to 365 days; ordering per shard; replay by sequence number or timestamp. Modes: Provisioned (you pick shards, split/merge by hand); On-demand Standard (sized to double the prior 30-day peak, throttles if traffic exceeds 2x within 15 minutes, and "does not detect and isolate hash keys that are driving a higher portion of incoming traffic to a particular shard" [3]); On-demand Advantage (account-level, 25 MiB/s minimum, no per-stream fee) [4]. No per-record ack, DLQ, or visibility timeout; a bad record blocks its shard unless the consumer skips it [10].

### Amazon EventBridge
A rule-based router: JSON events (256 KB on the classic bus) match patterns and go to 20+ target types. At-least-once, no ordering, latency "about half a second" [29]. Retries up to 24 hours, then DLQ if configured. No retention unless Archive is enabled; replay is unordered and delayed. In September 2026 AWS added an "enhanced" bus priced per GB with built-in retention [28].

### Google Cloud Pub/Sub (and Pub/Sub Lite)
Topics with independent subscriptions (pull, StreamingPull, push, export). Messages up to 10 MB; ack deadline 10-600 s; unacked messages kept 7 days by default; topic retention up to 31 days; `seek` replays to a time or snapshot (snapshots live at most 7 days) [33][36]. Ordering is opt-in per key and regional: 1 MB/s publish per key, it "decreases publish availability and increases end-to-end message delivery latency", and "redeliveries of a message trigger redelivery of all subsequent messages for that key, even acknowledged ones" [34]. Exactly-once is pull-only, same-region, excludes publisher duplicates, and has "significantly higher publish-to-subscribe latency" [35]. Dead-letter topics, attribute filters. Pub/Sub Lite (zonal, partitioned, cheaper) is deprecated: closed to new customers 2024-09-24 and, per the live doc, "will be turned down" on 2027-01-31 (earlier cached copies said 2026-03-18); Google points to Managed Service for Apache Kafka or standard Pub/Sub [32].

### Azure Service Bus
A classic broker: queues, topics with SQL/correlation filters, sessions (ordered per session ID), peek-lock with renewal, dead-lettering, scheduling, transactions, duplicate detection. Basic and Standard are multitenant: 256 KB messages, 1,000 operations/s per namespace, credit-based throttling where filter evaluations also consume credits. Premium is dedicated "messaging units": 1 MB messages (100 MB over AMQP), no fixed ops/s cap, and the only tier with private endpoints and geo-replication [41].

### Azure Event Hubs
Partitioned log with Kafka-protocol support. Standard: 1 MB events, 32 partitions, 20 consumer groups, 7-day retention, 1 MB/s in / 2 MB/s out per throughput unit, max 40 TUs, `ServerBusyException` over capacity. Premium: processing units, 100 partitions, 90 days. Dedicated: capacity units, 1,024 partitions, 20 MB events [48].

### Azure Event Grid
Push (Basic) or pull/MQTT (Standard). 1 MB events, 5,000 events/s per topic, billed per 64 KB chunk. At-least-once, unordered; retries up to 24 hours or 30 attempts, then dead-letter to blob storage; 1-day retention [50][51].

### Cloudflare Queues (and K2)
Queues: 128 KB messages, 5,000 msg/s and 25 GB backlog per queue, 14-day retention, 100 retries, at-least-once, $0.40 per million operations with 64 KB chunking, no egress fees [53]. On 2026-10-01 Cloudflare launched K2: an R2-backed ordered log with consumer groups and 5-minute batch leases, about 1 s p99 produce latency, anticipated $0.04/GB produced, $0.04/GB consumed, $0.02/GB-month retained; Kafka clients are a roadmap item [54].

## Strengths

- **Zero ops at modest scale.** Atlassian: "This zero ops service served us well up to 20-30 billion events" per day [5]. On Pub/Sub: "You just need to specify a topic and subscription, and then you don't really have to care about resources or scaling at all" [40].
- **Pay per use with real free tiers.** SQS $0.40 per million requests with 1M free; Pub/Sub $40/TiB with 10 GiB free; EventBridge $1 per million events. Pub/Sub is "stupidly cheap in comparison to any other similar product" [40].
- **IAM, encryption, and audit built in.** Resource policies, KMS, CloudTrail/Azure Monitor; no broker credentials to rotate.
- **Elasticity without capacity planning** in SQS standard and Pub/Sub; 2x burst headroom in Kinesis on-demand and Event Hubs auto-inflate.
- **Native glue**: function triggers, SNS to SQS fan-out, EventBridge targets, Kafka protocol on Event Hubs.

## Pain points

| Service | Pain | Evidence (quote + link + date) | Severity 1-5 | Root cause |
|---|---|---|---|---|
| Kinesis | Shard quotas and throttling at scale | peaks "needed thousands of shards"; "Kinesis throttling during spikes affected producers" — [Atlassian](https://www.atlassian.com/blog/how-we-build/scaling-streamhub-transitioning-from-kinesis-to-kafka-for-145-billion-daily-events), 2026-07-28 | 5 | Capacity is a user-visible unit with fixed per-shard limits |
| Kinesis | Hot keys cannot be fixed by scaling | On-demand "does not detect and isolate hash keys... If you are using highly uneven partition keys you may continue to receive write exceptions" — [AWS docs](https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html), accessed 2026-10-19 | 4 | Hash-range partitioning, no key-level rebalancing |
| Kinesis | Cost grows linearly; EFO and retention are premiums | "Enhanced Fan Out (EFO) carries a steep premium" — [Atlassian](https://www.atlassian.com/blog/how-we-build/scaling-streamhub-transitioning-from-kinesis-to-kafka-for-145-billion-daily-events), 2026-07-28. Knock: "approximately $112k/year between shard-hours and DynamoDB tables for checkpointing" — [WarpStream](https://www.warpstream.com/blog/knock-kinesis-to-warpstream-migration), 2026-09-21 | 4 | Per-shard-hour and per-consumer-shard-hour billing; checkpoints in DynamoDB |
| Kinesis | No queue semantics; poison pills | "A single malformed record can prevent processing on an entire shard" — [AWS Compute Blog](https://aws.amazon.com/blogs/compute/new-aws-lambda-controls-for-stream-processing-and-asynchronous-invocations/), 2019-11-25. "Kinesis doesn't support transaction write across multiple topics" — [Kong](https://konghq.com/blog/from-kinesis-to-kafka), 2023-03-24 | 3 | Per-shard cursor; no per-record ack or DLQ |
| SQS | FIFO ceiling and per-group serialization | "Requests over 300 TPS get the ThrottlingException error even if messages in the queue are available" — [AWS docs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/troubleshooting-fifo-throttling-issues.html), accessed 2026-10-19 | 3 | One partition per group; scale requires key spreading |
| SQS | No replay, no fan-out from one queue | "When a message is consumed from SQS, it's deleted. There's no log to replay" — [DEV](https://dev.to/rakno/why-i-stopped-using-sqs-and-built-a-kafka-system-from-scratch-43m5), 2026-05-01. "the semantics of a log offer replayability whereas SQS does not" — [HN rubiquity](https://news.ycombinator.com/item?id=37038585), 2023-08-07 | 4 | Delete-on-ack model; fan-out needs SNS in front |
| SQS | Request billing surprises | "I turned on sqs data events into cloudtrail... my run rate went to about $4k per day in cloudtrail+sqs usage... I'd racked up $25k" — [HN jcims](https://news.ycombinator.com/item?id=49950525), 2026-10-04. "The most common cause for a high number of SQS requests are empty receives" — [AWS re:Post](https://repost.aws/knowledge-center/sqs-high-charges) | 3 | Per-request billing; idle polling still costs |
| SQS | Semantics leak into code; exits overrun | "Managed-queue exits overrun because four SQS primitives leaked into application code and nobody costed the rewrite" — [stribog](https://stribog.com/blog/sqs-kafka-queue-semantics-managed-queue-exit), 2026-10-08 | 3 | Visibility lease, delete-as-ack, redrive counter, duplicate contract are proprietary |
| AWS (all) | No faithful local story | "It's very similar to AWS but not exactly the same, so you hit all kinds of annoying edges and subtle bugs" — [HN discomrobertul8](https://news.ycombinator.com/item?id=45604722), 2025-10-16. "the LocalStack auth token change broke a lot of CI pipelines" — [HN edmondx](https://news.ycombinator.com/item?id=47790534), 2026-04-16 | 3 | No first-party emulator; third-party emulators drift (LocalStack disables SQS's 60 s purge/recreate delays by default [55]) |
| AWS (all) | Lock-in through the glue | "it's all the auxiliary stuff that's locked in, the IAM, Cognito, CloudFormation, EventBridge, etc... Good luck digging out of that hole" — [HN chickensong](https://news.ycombinator.com/item?id=47772299), 2026-04-14 | 3 | Proprietary APIs plus proprietary triggers |
| EventBridge | No ordering, 24 h retry, ~0.5 s latency, pricier than SNS+SQS | "Typical latency is about half a second" — [AWS FAQ](https://aws.amazon.com/eventbridge/faqs/). Same design: SNS+SQS "$1603.14" vs EventBridge "$2893.56" — [DEV](https://dev.to/aws-builders/should-we-consider-migrate-to-amazon-eventbridge-from-amazon-sns-sqs--4dgi), 2022-02-05 | 3 | Router, not a store; $1/M per 64 KB |
| Pub/Sub | Duplicates and nack loops | "Duplicated messages, shitty DLQ implementation... infinite resends of messages (because they nacked message on error)" — [HN likis](https://news.ycombinator.com/item?id=39274025), 2024-02-06. | 3 | At-least-once default; deadline expiry redelivers |
| Pub/Sub | Ordering is narrow and fragile | "limited to 1 MBps" per key; "Redeliveries of a message trigger redelivery of all subsequent messages for that key, even acknowledged ones" — [Google docs](https://docs.cloud.google.com/pubsub/docs/ordering), accessed 2026-10-19. | 3 | Ordering bolted onto an unordered core |
| Pub/Sub | Fan-out and small messages multiply the bill | "500 million 120-byte messages bill as 500 GB, not 60 GB"; 1 TiB to 4 subscriptions meters as 5 TiB — [c3x.dev](https://c3x.dev/blog/gcp-pub-sub-cost/), 2026-09-21 | 3 | 1 KB minimum; charged per delivery |
| Pub/Sub Lite | Product retired | "Pub/Sub Lite is deprecated. Effective January 31, 2027, Pub/Sub Lite will be turned down" — [Google docs](https://docs.cloud.google.com/pubsub/docs/pubsub-lite-documentation), accessed 2026-10-19 | 4 | Low-cost tier abandoned for managed Kafka |
| Service Bus | Premium is a cliff; idle namespaces bill | Two namespaces used ~1 hour "consumed 450 usd already"; Premium is "charged per hour billing (fixed pricing) and not based on the usage" — [Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/1349802/2-service-bus-which-i-have-not-used-them-cost-arou), 2023-08-21. "Standard tier we pay roughly 11€/month, with the Premium tier it would be over 700€/month" — [burgyn.online](https://blog.burgyn.online/2024/03/24/azure-service-bus-how-we-did-not-use-topic-filters), 2024-03-24 | 4 | Dedicated units, no scale-to-zero |
| Service Bus | Standard throttles by credits, filters included | 56 filtered subscribers at 20 changes/s "would need 1,160 credits" vs 1,000 allowed — [burgyn.online](https://blog.burgyn.online/2024/03/24/azure-service-bus-how-we-did-not-use-topic-filters), 2024-03-24. | 3 | Multitenant credit throttling; tier packaging |
| Event Hubs | Idle Premium costs ~$739/month | "1.027$ per hour per processing unit. For one processing unit the monthly price is around 739.44$" — [Microsoft Q&A](https://learn.microsoft.com/en-in/answers/questions/1632941/cost-of-azure-premium-event-hubs), 2024-03-26 | 3 | Reserved units; partitions cannot shrink |
| Event Grid | No ordering; events dropped without DLQ | "Event Grid doesn't guarantee order"; "If you don't configure dead-lettering for an endpoint, Event Grid drops events" — [Microsoft docs](https://learn.microsoft.com/en-us/azure/event-grid/delivery-and-retry), 2026-02-09 | 3 | Push router, 1-day retention |

## Why people move

**Off cloud queues to Kafka-style logs.** Every migration post says the workload was a log all along. Atlassian needed 7-day history, many consumers, and 5x growth; retention beyond 24 h "required costly extended retention add ons", so they moved to MSK [5]. Knock forecast a drop from $112k/year to about $25k on WarpStream after moving five topics and ~20 consumers in a month [6]. Kong moved to MSK once "it would be more cost effective to run a Kafka cluster ourselves" and because developers "depended on an external AWS service for testing" [7]. On HN: "once you pop an item off the queue... it's gone... with Kafka, you can replay messages by resetting the offset" [24].

**Off cloud queues to Postgres.** For small and mid-size apps, `SKIP LOCKED` wins on transactional enqueue and local testing: "If enqueueing a job must be atomic with the row that caused it, Postgres wins outright" [22]; "My bar for 'Is there a reason we can't just do this all in Postgres?' is much, much higher than it was a decade ago" [23].

**Toward cloud queues.** The counter-pressure is operations and semantics: "If you need a queue, great, go get RabbitMQ, ZMQ, Redis, SQS, named pipes, pretty anything but Kafka" [25]. Kafka "replaces not a transport but guarantees your business logic already depends on silently" (per-message retries, DLQ, leases), and a stalled handler loses "every partition it held" on rebalance [21].

## Cost

List prices (US East, accessed 2026-10-19 unless noted):

- **SQS**: $0.40/M requests standard, $0.50 FIFO, +$0.10 for fair-queue requests; 1M free; each 64 KB chunk is a request, so a 1 MiB message is 16 requests [16]. An idle Lambda-polled queue costs pennies; the danger is sprawl and feedback loops like the $25k CloudTrail incident [18].
- **Kinesis**: provisioned $0.015/shard-hour (~$11/shard/month) plus $0.014 per million 25 KB PUT units, extended retention $0.020/shard-hour, EFO $0.015/consumer-shard-hour plus $0.013/GB. On-demand Standard $0.040/stream-hour, $0.08/GB in, $0.04/GB out; AWS's example is $918/month for 1,000 records/s at 3 KB with one consumer. Advantage: $0.032/GB in, $0.016/GB out, no stream fee, 25 MiB/s minimum (about $100/day) [1][4]. Provisioned is cheaper above about 5% average utilization [9], but idle shards bill every hour.
- **EventBridge**: $1.00/M custom events per 64 KB; archive $0.10/GB plus $0.023/GB-month; replays re-billed at $1/M; Pipes $0.40/M. Enhanced bus: $0.18/GB in, $0.05/GB per subscription out, $0.08/GB stored; AWS's example is $2,317/month for 1,000 events/s at 2 KB with two subscriptions [28].
- **Pub/Sub**: $40/TiB metered on publish and on each delivery, 10 GiB/month free, 1 KB minimum per message, retained/snapshot storage about $0.27/GiB-month, cross-region at egress rates (official page could not be scraped; per c3x.dev, 2026-09-21) [37].
- **Service Bus**: Basic $0.05/M operations; Standard $0.0135/hour base (~$10/month) with 13M operations included, then $0.80/M to 100M, $0.50/M to 2.5B, $0.20/M above; Premium $0.928/MU-hour, about $677/month per MU (unverified on the dynamic official page; per Azure FinOps Essentials, 2024-09-23) [43].
- **Event Hubs**: Premium $1.027/PU-hour, about $739/month idle [49]; Standard TU rates could not be verified.
- **Event Grid**: 100,000 free operations/month; the commonly cited $0.60/M Basic rate could not be verified [52].
- **Cloudflare**: Queues $0.40/M operations, 1M/month included, no egress [53]; K2 beta $0.04/GB produced and consumed, $0.02/GB-month [54].

Bill-shock patterns: idle Premium units ($450 in under a month for two unused Service Bus namespaces [44]; ~$739/month per idle Event Hubs PU [49]); Kinesis shard sprawl and EFO ($112k/year at Knock [6]; ~$50k/month plus ~$30k CloudWatch at a Cribl prospect [8]); per-delivery fan-out and 1 KB rounding on Pub/Sub (8x markup on 120-byte messages [37]); SQS request feedback loops ($4k/day [18]).

## Design lessons for a new platform

- **Keep the queue ergonomics people depend on**: per-message ack, visibility lease with extension, delay, DLQ with a redrive counter, content filtering. These are the primitives that make SQS exits expensive [21] and that Cloudflare split K2 away from Queues to drop [54].
- **Store everything as a log underneath** so replay, independent consumer groups, and long retention are first-class rather than an SNS-in-front or Archive add-on. "There's no log to replay" is the top reason teams leave SQS [20][24].
- **Never expose shards or units as a capacity or billing primitive.** Users should not compute `ceiling(max(in/1024, out/2048))` [3] or pay for idle shards; partition internally, rebalance hot keys automatically, and bill on bytes and retention as K2 and On-demand Advantage do.
- **Make per-key ordering cheap and robust**: no 1 MB/s per-key cap, no 300 msg/s per group, no "redeliver everything after a failure" cascade [34]. Serialize per key, parallelize across keys, and park a poisoned key without stalling its neighbors.
- **Deduplicate at the producer (idempotency key) and at the consumer (durable ack)** so exactly-once is not regional, pull-only, or a latency penalty [35], and the default mode does not surface duplicates [19].
- **Charge for bytes and storage, not requests**: no 64 KB or 1 KB rounding, no empty-receive charges, no fan-out multiplier. Offer a hard spend cap and scale to zero; the Service Bus and Event Hubs complaints are entirely about idle fixed units [44][49].
- **Ship a first-party, byte-identical local binary** with production limits and timing; emulator drift and license changes are a recurring complaint [56][57], and Kong listed local testing as a migration reason [7].
- **Large messages built in** (10 MB inline, claim-check to object storage beyond), **per-message observability** (queryable state, per-key lag, trace IDs), and **tenancy as a core primitive** (tenant key, per-tenant quotas, noisy-neighbor isolation; SQS fair queues show the demand [14]).
- **Open protocol and client** (Kafka-compatible or a documented wire protocol) so exits are a config change [31][21]; one tier, no "Premium cliff" for private networking or large messages [47].
- **Cross-region as a feature**: ordered, replicated topics with one API; every incumbent degrades there (Pub/Sub ordering and exactly-once are single-region, Service Bus geo-replication is Premium-only).

## Sources

1. AWS, Kinesis Data Streams pricing — https://aws.amazon.com/kinesis/data-streams/pricing/ (accessed 2026-10-19)
2. AWS, Kinesis quotas and limits — https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html (accessed 2026-10-19)
3. AWS, Choose the right mode to stream in — https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html (accessed 2026-10-19)
4. AWS Big Data Blog, Kinesis On-demand Advantage saves 60% — https://aws.amazon.com/blogs/big-data/kinesis-on-demand-advantage-saves-60-on-streaming-costs (2026-03-09)
5. Atlassian, Scaling StreamHub: Kinesis to Kafka for 145 billion daily events — https://www.atlassian.com/blog/how-we-build/scaling-streamhub-transitioning-from-kinesis-to-kafka-for-145-billion-daily-events (2026-07-28)
6. WarpStream, How Knock saved $87K a year migrating off Kinesis — https://www.warpstream.com/blog/knock-kinesis-to-warpstream-migration (2026-09-21; vendor-authored, "after" figure is a forecast)
7. Kong, From Kinesis to Kafka — https://konghq.com/blog/from-kinesis-to-kafka (2023-03-24)
8. Cribl, Replacing Kinesis Data Streams with Cribl Stream — https://cribl.io/blog/replacing-kinesis-data-streams-with-cribl-stream-a-real-world-cost-saving/ (edited 2025-09-29; vendor-authored)
9. Caylent/Trek10, Kinesis on-demand vs provisioned cost comparison — https://caylent.com/blog/amazon-kinesis-data-streams-on-demand-vs.-provisioned-billing-mode-cost-comparison (2023-09-21)
10. AWS Compute Blog, New Lambda controls for stream processing — https://aws.amazon.com/blogs/compute/new-aws-lambda-controls-for-stream-processing-and-asynchronous-invocations/ (2019-11-25)
11. AWS, SQS message quotas — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html (accessed 2026-10-19)
12. AWS, Troubleshoot FIFO throttling — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/troubleshooting-fifo-throttling-issues.html (accessed 2026-10-19)
13. AWS, Exactly-once processing in SQS — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html (accessed 2026-10-19)
14. AWS, SQS fair queues — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues.html; announcement https://aws.amazon.com/about-aws/whats-new/2025/07/amazon-sqs-introduces-fair/ (2025-07)
15. AWS, SQS maximum payload 1 MiB — https://aws.amazon.com/about-aws/whats-new/2025/08/amazon-sqs-max-payload-size-1mib (2025-08-04)
16. AWS SQS pricing — https://aws.amazon.com/sqs/pricing/ (prices not rendered); figures from Cloudburn — https://cloudburn.io/blog/amazon-sqs-pricing (2026-02-21, updated 2026-10-12)
17. AWS re:Post, Understand SQS billing and reduce costs — https://repost.aws/knowledge-center/sqs-high-charges (accessed 2026-10-19)
18. HN comment by jcims on hard budget caps — https://news.ycombinator.com/item?id=49950525 (2026-10-04)
19. HN comment by nz on SQS vs Pub/Sub — https://news.ycombinator.com/item?id=50026231 (2026-10-09)
20. DEV, Why I stopped using SQS and built a Kafka system — https://dev.to/rakno/why-i-stopped-using-sqs-and-built-a-kafka-system-from-scratch-43m5 (2026-05-01, edited 2026-09-23)
21. stribog, Leaving SQS: Kafka is not a drop-in queue replacement — https://stribog.com/blog/sqs-kafka-queue-semantics-managed-queue-exit (2026-10-08)
22. DEV, Postgres as a job queue vs Redis vs SQS — https://dev.to/libme/postgres-as-a-job-queue-vs-redis-vs-sqs-when-does-just-use-your-database-stop-working-1f24 (Sep 2; year not shown)
23. Ask HN: Why do message queue-based architectures seem less popular now? — https://news.ycombinator.com/item?id=40723302 (2024-06-18)
24. HN comment by Jemaclus, go-to message queue in 2025 — https://news.ycombinator.com/item?id=44006992 (2025-05-16)
25. HN comment by mrweasel, Why was Apache Kafka created? — https://news.ycombinator.com/item?id=44998395 (2025-08-23)
26. AWS, SNS endpoints and quotas — https://docs.aws.amazon.com/general/latest/gr/sns.html (accessed 2026-10-19)
27. AWS SNS pricing — https://aws.amazon.com/sns/pricing/ (accessed 2026-10-19)
28. AWS EventBridge pricing — https://aws.amazon.com/eventbridge/pricing/ (accessed 2026-10-19)
29. AWS EventBridge FAQs — https://aws.amazon.com/eventbridge/faqs/ (accessed 2026-10-19)
30. DEV, Should we migrate to EventBridge from SNS+SQS? — https://dev.to/aws-builders/should-we-consider-migrate-to-amazon-eventbridge-from-amazon-sns-sqs--4dgi (2022-02-05)
31. HN comment by chickensong on AWS lock-in — https://news.ycombinator.com/item?id=47772299 (2026-04-14)
32. Google, Pub/Sub Lite documentation (deprecation notice) — https://docs.cloud.google.com/pubsub/docs/pubsub-lite-documentation (accessed 2026-10-19)
33. Google, Pub/Sub quotas and limits — https://docs.cloud.google.com/pubsub/quotas (accessed 2026-10-19)
34. Google, Order messages — https://docs.cloud.google.com/pubsub/docs/ordering (accessed 2026-10-19)
35. Google, Exactly-once delivery — https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery (accessed 2026-10-19)
36. Google, Replay and purge messages (seek) — https://docs.cloud.google.com/pubsub/docs/replay-overview (accessed 2026-10-19)
37. c3x.dev, GCP Pub/Sub cost — https://c3x.dev/blog/gcp-pub-sub-cost/ (2026-09-21); official page https://cloud.google.com/pubsub/pricing could not be scraped
38. DEV, The redelivery trap: transactional inbox for Pub/Sub — https://dev.to/asbrodova/the-redelivery-trap-building-a-safe-transactional-inbox-for-google-pubsub-36ei (Oct 12; year inferred as 2026)
39. DEV, Addressing Pub/Sub ordering key limitations — https://dev.to/einfochips/ensuring-message-order-in-distributed-systems-addressing-pubsub-ordering-key-limitations-with-5f0o (2025-11-27)
40. HN comment by likis on Pub/Sub gotchas — https://news.ycombinator.com/item?id=39274025 (2024-02-06)
41. Microsoft, Service Bus quotas and limits — https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quotas (2026-10-13)
42. Microsoft, Service Bus pricing — https://azure.microsoft.com/en-us/pricing/details/service-bus/ (prices rendered dynamically; accessed 2026-10-19)
43. Azure FinOps Essentials, Optimizing Service Bus tiers — https://azure-finops-essentials.mindbyte.nl/p/azure-service-bus-tier-optimization (2024-09-23)
44. Microsoft Q&A, 2 service bus which I have not used cost around 450 — https://learn.microsoft.com/en-us/answers/questions/1349802/2-service-bus-which-i-have-not-used-them-cost-arou (2023-08-21)
45. burgyn.online, Azure Service Bus: how we didn't use topic filters — https://blog.burgyn.online/2024/03/24/azure-service-bus-how-we-did-not-use-topic-filters (2024-03-24)
46. caleta.io, Service Bus: when Standard beats Premium — https://caleta.io/blog/service-bus-standard-vs-premium-cost/ (2026-04-07)
47. HN comment by Amasuriel on Azure premium SKUs — https://news.ycombinator.com/item?id=36277878 (2023-06-11); sfeldman, What you pay is what you get — https://weblogs.asp.net/sfeldman/what-you-pay-is-what-you-get/ (2018-07-03)
48. Microsoft, Event Hubs quotas and limits — https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas (2026-02-05)
49. Microsoft Q&A, Cost of Azure Premium Event Hubs — https://learn.microsoft.com/en-in/answers/questions/1632941/cost-of-azure-premium-event-hubs (2024-03-26)
50. Microsoft, Event Grid quotas and limits — https://learn.microsoft.com/en-us/azure/event-grid/quotas-limits (2026-05-07)
51. Microsoft, Event Grid delivery and retry — https://learn.microsoft.com/en-us/azure/event-grid/delivery-and-retry (2026-02-09)
52. Microsoft, Event Grid pricing — https://azure.microsoft.com/en-us/pricing/details/event-grid/ (prices rendered dynamically; accessed 2026-10-19)
53. Cloudflare, Queues limits — https://developers.cloudflare.com/queues/platform/limits/ and pricing — https://developers.cloudflare.com/queues/platform/pricing/ (accessed 2026-10-19)
54. Cloudflare, Announcing Cloudflare K2: serverless event streams — https://blog.cloudflare.com/cloudflare-k2-streams/ (2026-10-01); HN thread https://news.ycombinator.com/item?id=49923798 (2026-10-01)
55. LocalStack, SQS service docs — https://docs.localstack.cloud/aws/services/sqs/ (accessed 2026-10-19)
56. HN comments by discomrobertul8 and ljm on LocalStack — https://news.ycombinator.com/item?id=45604722 and https://news.ycombinator.com/item?id=45605605 (2025-10-16)
57. HN comment by edmondx on LocalStack and mocked-service drift — https://news.ycombinator.com/item?id=47790534 (2026-04-16)
