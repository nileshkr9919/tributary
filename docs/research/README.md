# Messaging platform research (2026-10-19)

Ten reports, about 34,000 words and roughly 340 dated sources, written to decide what this
project should become. Each report has the same shape: architecture, strengths, a pain-point
table with quotes and links, why people leave, cost, design lessons, sources.

| Report | Covers |
| --- | --- |
| [kafka.md](kafka.md) | Apache Kafka, KRaft, KIP-848, share groups (KIP-932), tiered storage, Confluent, MSK |
| [pulsar.md](pulsar.md) | Apache Pulsar, BookKeeper, Oxia, StreamNative |
| [classic-brokers.md](classic-brokers.md) | RabbitMQ (classic, quorum, streams), ActiveMQ Classic and Artemis, IBM MQ, ZeroMQ |
| [lightweight-systems.md](lightweight-systems.md) | NATS JetStream, Redis Streams, Valkey, NSQ, Iggy, Fluvio, Memphis |
| [cloud-managed.md](cloud-managed.md) | SQS, SNS, Kinesis, EventBridge, Google Pub/Sub, Azure Service Bus, Event Hubs, Event Grid, Cloudflare Queues |
| [kafka-compatible-new-wave.md](kafka-compatible-new-wave.md) | Redpanda, WarpStream, AutoMQ, Bufstream, KIP-1150 diskless topics, S2 |
| [db-queues-and-cross-cutting-pains.md](db-queues-and-cross-cutting-pains.md) | Postgres queues, River, Oban, Solid Queue, BullMQ, Sidekiq, Temporal; pains common to every platform |
| [ai-inference-traffic.md](ai-inference-traffic.md) | LLM traffic: token rate limits, 429s, priority and fairness, batch APIs, token streams, cost, fallbacks (requirements I1 to I16) |
| [ai-agents.md](ai-agents.md) | Agents: durable execution, human-in-the-loop, tool idempotency, supersede and cancel, A2A and MCP, safety (requirements A1 to A20) |
| [ai-data-pipelines.md](ai-data-pipelines.md) | AI data: RAG and embedding pipelines, lakehouse tables, eval logs, EU AI Act, deletes, PII, gating (requirements D1 to D18) |

## Method and limits

- **Reddit could not be read.** reddit.com blocked the fetcher and search returned no subreddit
  threads, in every one of the ten runs. Community sentiment comes from Hacker News, GitHub
  issues and discussions, mailing lists, the Confluent forum analysis by Conduktor, and
  practitioner blogs instead. Each report says this in its own method note.
- **Vendor sources are marked** where a claim comes from a company selling an alternative.
- **Spot checks.** Two surprising claims were verified directly: the Jepsen NATS 2.12.1 figures
  (fsync every two minutes, 131,418 of 930,005 messages lost) and CVE-2026-67235 in RabbitMQ.
- A few cloud prices render only with JavaScript and are marked unverified in
  [cloud-managed.md](cloud-managed.md).

## The ten pains that recur across every family

1. **Two systems.** Logs (Kafka, Kinesis) cannot ack, retry, delay or dead-letter one message;
   queues (SQS, RabbitMQ, Postgres) cannot replay or fan out. Teams run both.
2. **Operational weight.** Kafka, Pulsar and clustered RabbitMQ "need a dedicated team"; NATS and
   Postgres win on "one binary".
3. **Acknowledged data gets lost.** RabbitMQ under partitions (Jepsen), NATS lazy fsync (Jepsen
   2025), Redis persistence windows, single-AZ write-ahead logs in S3-native Kafka designs.
4. **Partitions are a permanent guess** that also caps consumer parallelism; per-key ordering and
   parallelism fight each other everywhere.
5. **Dead letters and poison messages** have no owner, no reason recorded and no replay path.
6. **Observability.** "Where is my message" has no answer; depth is reported but message age is not.
7. **Exactly-once confusion.** Duplicates still reach the database; idempotency is left to consumers.
8. **Cost opacity.** Cross-AZ traffic is 70 to 90 percent of a cloud Kafka bill; shard and unit
   billing (Kinesis, Azure Premium) charges for idle capacity.
9. **Add-on sprawl.** Schema registry, connectors, delayed delivery and tiered storage are separate
   systems, plugins or paid tiers.
10. **Trust and governance.** Relicensing (Redis, Redpanda BSL, the NATS scare), acquisitions
    (WarpStream, Bufstream), and benchmark wars.

The design that answers these is in
[../superpowers/specs/2026-10-19-unified-messaging-platform-design.md](../superpowers/specs/2026-10-19-unified-messaging-platform-design.md).
