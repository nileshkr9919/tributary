# AI data pipelines and governance

## Method and limits

Research date: 2026-10-19. Web search plus direct fetches of engineering blogs, docs, arXiv, law-firm notes, Hacker News and vendor posts; Reddit could not be fetched. Every quote was read on the cited page; where only a paraphrase could be confirmed, I paraphrase. Vendor sources are marked **[vendor]**.

Limits: community sentiment on vendor AI features is thin (no Hacker News launch threads found), so "hype vs reality" rests on one practitioner blog [12] and release notes. AI Act dates come from law-firm summaries, not the Official Journal. No public cross-tenant RAG leak report turned up; that evidence is design guidance.

## Challenges

### 1. Real-time RAG and embedding pipelines

**What goes wrong.** The usual pipeline is CDC, chunking, an embedding API, then an upsert into the vector store. Four things break.

- *Deletes get lost.* Upsert-only jobs never remove vectors for deleted rows. Streamkap's guide lists "Delete handling" as a production check: "Deletes in the source DB remove vectors from the store" [3]. Even when deletes arrive, vector stores often "only mark the record as deleted, leaving the embedding on disk physically unchanged" [4].
- *Model changes force full backfills.* A new embedding model means re-embedding the whole corpus into a new index while live writes continue. "Without dual writes, the new index is stale the instant you finish backfilling it" [1]. And "Embedding APIs are governed by tokens-per-minute quotas, and 40 billion tokens is a lot of minutes"; a 100-million-chunk corpus takes "more than five days of continuous embedding" [1].
- *Rate limits are per provider, not per consumer.* "Embedding APIs have rate limits", so "batching is essential" [3]. Each consumer instance backs off on its own, so the limit is never shared.
- *Wasted work.* "Not every column change requires re-embedding" [3]. A status-flag update still costs an embedding call unless someone filters it out.

Per-document ordering also matters: two updates racing through parallel workers can let the older embedding win. The guides rely on idempotent upserts, which fix duplicates, not reordering.

**How teams solve it today.** Debezium or a vendor CDC tool into Kafka, a worker pool with hand-written batching and 429 backoff, content hashes to skip unchanged text, and a versioned embedding column with blue-green indexes: "model upgrades become a blue-green deployment for embeddings" [2].

**What a message platform could do.** Per-key ordering with latest-wins coalescing, first-class deletes, a shared rate budget across all consumers of one subscription, a backfill subscription that runs beside the live one, and delivery filtered on changed fields.

### 2. Streaming into lakehouses for AI

**What goes wrong.** Teams want one copy usable as both a stream and an Iceberg or Delta table for training and analytics. Confluent Tableflow, Redpanda Iceberg Topics, WarpStream Tableflow, AutoMQ Table Topic and Aiven all offer this, but most keep two physical copies. A 2026-10-16 survey says Redpanda, Confluent and WarpStream write "a broker-managed second copy", and that Redpanda "does not backfill an existing topic when you enable Iceberg" **[vendor]** [7]. Upsert tables carry sharp edges: in Tableflow, "the Kafka message key and partition number form the composite primary key", so "If the same key appears in two partitions, it creates two separate rows in the materialized table"; and "Upsert tables have a maximum limit of 30 billion unique keys" **[vendor]** [8]. Schema mismatch is the practitioner complaint: a Hacker News commenter who hand-built Protobuf-to-Iceberg in Flink wrote that "things like schema evolution are a nightmare" (2025-07-31) [9]. Streaming writes also produce many small files, which need compaction [7].

**How teams solve it today.** The Kafka Connect Iceberg sink or Flink jobs, a separate compaction service, and manual tombstones for deletes [8].

**What a message platform could do.** Make "table view" a property of the storage class (spec 6.7), driven by the planned registry (7.1). Row identity must not depend on shard number, because shards split from M4.

### 3. Event logs for evaluation and feedback

**What goes wrong.** Teams are told to "Log everything. Tag every request/response with a `workload_id`" [15]. But traces are large, and "storing 100% of a high-traffic app can cost more than the LLM calls themselves" [13]. Random sampling loses the rare failures: "Errors are rare and high-value; you can't debug an outage from a 5% sample of it" [13]. Feedback is sparse: "Less than 1% of production interactions yield explicit feedback signals" [15]. Replaying traffic against a new model needs full context: "You need to capture and replay full session context, which substantially increases storage requirements", and "replaying the final message in isolation changes the semantic context" [14]. Arize notes that shadowed agents must not run real side effects, so tool calls have to be stubbed or replayed from recorded responses **[vendor]** [27]. NVIDIA's flywheel blueprint admits "Routing production traffic directly into fine-tuning without PII removal" **[vendor]** [16].

**How teams solve it today.** An observability SaaS or Elasticsearch [16] for traces, head or tail sampling in the OTel collector, and batch exports to build eval sets. OpenTelemetry GenAI instrumentation captures no message content by default (`NO_CONTENT`); capture is opt-in, and content can be uploaded to external storage via a hook [24].

**What a message platform could do.** Key-consistent sampling, so a whole session is kept or dropped together, with "always keep" rules for error headers. Claim-check storage for large prompt bodies. A shadow-replay profile that cannot cause side effects.

### 4. Feature and context freshness

**What goes wrong.** Batch-fed context goes stale. Vendors now sell a "context layer": Confluent's Real-Time Context Engine (GA, with retrieval metrics added in the 2026-08-18 update) **[vendor]** [11], Materialize as "the live context layer for agents and apps" **[vendor]** [26], and RisingWave's MCP server **[vendor]** [26]. Freshness is also about meaning, not only latency. Atlan: "A streaming platform confirms an event arrived; it says nothing about whether the field definition, schema version, or access…" policy is still current (2026-07-20) **[vendor]** [23].

**How teams solve it today.** Compacted topics materialized into Redis, a feature store or a streaming database; per-pipeline lag dashboards.

**What a message platform could do.** Report end-to-end freshness per subscription and per key ("index applied offset N for document D"). Serve latest-by-key reads from compacted topics, so small context lookups need no extra store.

### 5. Governance and compliance

**EU AI Act.** Article 12(1): "High-risk AI systems shall technically allow for the automatic recording of events (logs) over the lifetime of the system" [17]. Under Articles 19 and 26(6), providers and deployers must keep the logs under their control for at least six months, unless other law sets a different period [20]. The Digital Omnibus deferred the high-risk obligations: "High-risk obligations for stand-alone Annex III systems are deferred to 2 December 2027" and, "for AI embedded in regulated products under Annex I, to 2 August 2028" (Gibson Dunn, 2026-05-27) [18]. Law-firm summaries report that Regulation (EU) 2026/1744 was published on 2026-07-24 and entered into force on 2026-07-27 [19].

**GDPR deletion.** A person's data now sits in source tables, the event log, embeddings, eval sets, fine-tuning sets and caches. With vector stores, "you have a problem that 'delete by ID' does not solve", and "Whether a tombstoned record satisfies Article 17 is genuinely contested" (2026-04-20) [5]. OWASP: "Embeddings are not anonymized data" and "Deletion must be explicitly propagated" [6].

**PII before models.** "The right place to redact streaming data isn't downstream", because "Once a single SSN reaches one downstream system, it's everywhere" (2026-03-11) **[vendor]** [22].

**How teams solve it today.** Connect transforms or Flink jobs for masking, a separate audit store, manual deletion runbooks and manual lineage.

**What a message platform could do.** Data-class labels on schema fields, with retention and redaction per class. An access audit log. Provenance headers. Crypto-shredding that covers every copy: tiered segments, table views and claim-check blobs.

### 6. Security of AI data flows

**What goes wrong.** Documents and events become prompt text. Knowledge-base poisoning "persists until the index is purged and affects all users querying a poisoned topic", and "Swapping the model does not sanitize the index" **[vendor]** (2026-06-19) [21]. The recommended fix is at ingestion: "Secure your ingestion pipeline before data reaches the vector index", with failing documents "excluded from ingestion or quarantined pending review" [21]. OWASP adds "Implement staging or review steps between ingestion and availability in the vector store" [6]. On tenancy: "Chunks from tenant A must never be retrieved by queries from tenant B", and a failed access check should "return nothing, not a filtered subset" [6].

**How teams solve it today.** Scanner services, per-tenant vector namespaces, app-side filters.

**What a message platform could do.** A gated topic, where messages become visible to downstream subscriptions only after a scanner subscription approves them. A server-enforced tenant tag bound to the producer's token. Field-level projection, so a consumer only receives the fields its token allows.

### 7. What streaming vendors ship for AI, and what users say

- **Confluent Intelligence** **[vendor]**: Real-Time Context Engine (GA), managed MCP server, agent skills, forecasting models in early access; the 2026-08-18 post gives Streaming Agents only one line [11].
- **Redpanda Agentic Data Plane** **[vendor]** (2025-10-28): "a single, governed API to discover, query, act on, and revoke access to data", with "immutable audit trails with configurable retention" on the roadmap [10]. Redpanda Connect has embedding processors and vector-store outputs [25].
- **Materialize, RisingWave** **[vendor]**: incrementally maintained views exposed to agents over MCP [26].
- **StreamNative, Aiven, Estuary**: no AI-specific primary sources found, apart from Aiven's single-copy Iceberg design [7].

**Sentiment.** The clearest practitioner voice is the 2025 year-in-review on 2minutestreaming (2025-12-30). It calls a vendor's "AI Agent Meal Planner with MongoDB, Kafka and Flink" demo "The most ridiculous thing I've seen in 2025", and quotes "the last thing AI agents need is Kafka…" [12]. The same review says "Iceberg and the Lakehouse is everywhere" [12]. My reading: lakehouse and CDC-to-vector plumbing solve real problems; "streaming agents" is mostly positioning, while deletes, backfills, rate limits and audit remain unsolved.

## Pain table

| Pain | Evidence (quote + link + date) | Severity 1-5 | Current workaround |
|---|---|---|---|
| Deletes do not reach vector stores, or only soft-delete | "only mark the record as deleted, leaving the embedding on disk physically unchanged" [4] (2026-06-16); "Deletion must be explicitly propagated" [6] | 5 | Custom delete handlers in CDC workers; periodic reconciliation |
| Re-embedding backfill when the model changes | "Without dual writes, the new index is stale the instant you finish backfilling it" [1] (2026-07-05) | 4 | Blue-green indexes, dual-write code, checkpointed batch jobs [2] |
| Embedding API rate limits | "Embedding APIs are governed by tokens-per-minute quotas, and 40 billion tokens is a lot of minutes" [1] (2026-07-05) | 4 | Per-worker backoff on 429, batching [3] |
| Unneeded re-embedding on metadata-only changes | "Not every column change requires re-embedding" [3] (2025-05-22) [vendor] | 3 | Content hashes, column lists in worker code |
| Stream-to-table keeps two copies; no backfill | Redpanda "does not backfill an existing topic when you enable Iceberg" [7] (2026-10-16) [vendor] | 3 | Pay for both copies; Connect sink from earliest offset |
| Upsert-table key identity tied to partition | "If the same key appears in two partitions, it creates two separate rows" [8] (accessed 2026-10-19) [vendor] | 3 | Never add partitions; manual tombstones |
| Schema evolution stream to table | "things like schema evolution are a nightmare" [9] (2025-07-31) | 3 | Hand-written Flink jobs |
| Trace storage cost vs losing failures | "storing 100% of a high-traffic app can cost more than the LLM calls themselves" [13] (2026-06-13) | 4 | Tail sampling in collectors; SaaS observability |
| Replay needs full context and no side effects | "You need to capture and replay full session context" [14] (2026-04-19) | 3 | Bespoke shadow harnesses; stubbed tools |
| PII flowing into models and training sets | "Once a single SSN reaches one downstream system, it's everywhere" [22] (2026-03-11) [vendor] | 5 | Connect/Flink masking; external redaction services |
| Erasure across embeddings, logs, datasets | "you have a problem that 'delete by ID' does not solve" [5] (2026-04-20) | 5 | Manual runbooks; avoid fine-tuning on personal data |
| AI Act logging and six-month retention | "automatic recording of events (logs) over the lifetime of the system" [17]; deadlines moved to 2027-12-02 / 2028-08-02 [18] (2026-05-27) | 4 | Separate audit stores; vendor governance layers [10] |
| Poisoned content in ingestion | poisoning "persists until the index is purged" [21] (2026-06-19) [vendor] | 4 | Scanner stages; quarantine queues |
| Cross-tenant context leakage | "Chunks from tenant A must never be retrieved by queries from tenant B" [6] | 4 | Per-tenant namespaces; app-side filters |
| Context freshness is unmeasured | "it says nothing about whether the field definition, schema version, or access…" [23] (2026-07-20) [vendor] | 3 | Per-pipeline lag dashboards |

## Requirements for Tributary

- **D1 Latest-wins coalescing per key** (subscription setting). When behind, deliver only the newest record per key and ack the skipped ones; deletes are never coalesced away. Test: v1..v5 produced while paused yields one delivery (v5).
- **D2 First-class delete records** (topic setting plus core). A keyed delete marker reaches every subscription in key order and is applied to compacted, tiered, object and table copies. Test: after compaction, no storage class returns the key.
- **D3 Shared rate budget** (subscription setting). One token bucket (records, bytes or a header unit such as `x-tokens`) shared by all members, optionally namespace-wide; a 429 with retry-after pauses the whole subscription without spending attempts. Test: 10 members never exceed N units/minute combined.
- **D4 Backfill profile** (profile). A second subscription from offset 0 or a time, at lower priority with its own budget, beside the live one; reports progress, ETA and "caught up". Test: with live traffic at its limit, backfill uses only spare budget.
- **D5 Changed-field filter** (subscription setting, needs registry and before-images from the outbox/CDC relay). Deliver only when a named field changed. Test: a status-only update skips the embedding subscription.
- **D6 Table view per topic** (topic setting on storage classes, M4+). Expose sealed data as an Iceberg table (append or upsert) with registry-driven schema evolution, deletes, compaction and backfill; row identity is the key, not the shard. Test: after a shard split, one row per key.
- **D7 Key-consistent sampling** (subscription setting). Deterministic fraction by key hash so whole sessions are kept or dropped, plus always-deliver header rules (`error=true`). Test: same key, same decision across restarts.
- **D8 Claim-check with separate retention** (topic setting). Large payloads go to object storage with their own, shorter retention, so prompts expire while the event survives. Test: after blob expiry a fetch returns metadata and an "expired" marker.
- **D9 Shadow-replay profile** (profile plus token scope). Replays a time range with a `shadow` header; its token cannot produce to non-shadow topics and webhooks only reach allowlisted endpoints. Test: a shadow-token produce to a production topic is rejected.
- **D10 Data-class labels and per-class retention** (topic setting via schema annotations). Fields tagged `pii`, `secret`, `user_content` get per-class retention or masking. Test: after the class retention, tagged fields read back masked.
- **D11 Field-level projection by token scope** (subscription setting enforced server-side). Consumers receive only fields their token allows; failures are closed. Test: a token without `pii` never receives tagged bytes, including via replay and DLQ.
- **D12 Access audit log** (new primitive: system topic). Immutable record of who produced, consumed, replayed, requeued or reconfigured, by topic and offset range; default retention at least 6 months; exportable. Test: every consume session appears with token, ranges and time.
- **D13 Verifiable erasure across copies** (extends M5 crypto-shredding). Shredding covers local, tiered, object, table and claim-check copies and returns a receipt listing storage classes and subscriptions that received the subject's records. Test: afterwards no read path returns plaintext.
- **D14 Provenance headers** (core, broker-managed). Source topic, offset, schema version, producer and outbox origin survive relay and requeue; queryable by key. Test: a relayed record keeps its origin header.
- **D15 Gated topics** (new primitive). Records reach normal subscriptions only after a scanner subscription approves them; rejects go to quarantine. Test: an unapproved record is never dispatched.
- **D16 Server-enforced tenant tag** (topic setting plus token binding). Tenant ID comes from the producer's token; tenant-scoped subscriptions get only their tenant's records. Test: a spoofed tenant header is rejected.
- **D17 Freshness metrics per key** (observability). End-to-end age-at-ack histogram per subscription, plus last-acked-offset lookup per key. Test: the lookup matches consumer acks.
- **D18 Latest-by-key read on compacted topics** (topic setting). Get-by-key returns the latest value for small context lookups. Test: it equals the newest committed record.

## Sources

1. Tian Pan, "Retiring an Embedding Model: Re-indexing Millions of Vectors Without Taking Search Down", 2026-07-05. https://tianpan.co/blog/2026/07/05/retiring-an-embedding-model-reindex-without-downtime
2. A. Obernesser (dbi services), "RAG Series: Embedding versioning with pgvector", 2026-02-22. https://www.dbi-services.com/blog/rag-series-embedding-versioning-with-pgvector-why-event-driven-architecture-is-a-precondition-to-ai-data-workflows/
3. Streamkap **[vendor]**, "Building a Real-Time Embedding Pipeline with CDC and Vector Stores", 2025-05-22. https://streamkap.com/resources-and-guides/streaming-embeddings-pipeline
4. Chakraborttii et al., "Ghost Vectors: Soft-Deleted Embeddings Remain Reconstructible in HNSW Vector Databases", arXiv 2606.18497, 2026-06-16. https://arxiv.org/abs/2606.18497
5. Tian Pan, "GDPR's Deletion Problem: Why Your LLM Memory Store Is a Legal Liability", 2026-04-20. https://tianpan.co/blog/2026/04/20/gdpr-llm-memory-erasure-vector-database
6. OWASP, "RAG Security Cheat Sheet", undated, accessed 2026-10-19. https://cheatsheetseries.owasp.org/cheatsheets/RAG_Security_Cheat_Sheet.html
7. Conduktor **[vendor]**, S. Derosiaux, "Kafka to Iceberg: Topics as Tables", 2026-10-16. https://www.conduktor.io/glossary/kafka-to-iceberg-topics
8. Confluent **[vendor]**, "Write Modes with Tableflow", docs, accessed 2026-10-19. https://docs.confluent.io/cloud/current/topics/tableflow/concepts/write-modes.html
9. Hacker News, "Stream Kafka Topic to the Iceberg Tables with Zero-ETL", 2025-07-31. https://news.ycombinator.com/item?id=44744834
10. Redpanda **[vendor]**, "Introducing the Agentic Data Plane", 2025-10-28. https://www.redpanda.com/blog/agentic-data-plane
11. Confluent **[vendor]**, "New in Confluent Cloud and WarpStream: Evolving the Data Streaming Platform for AI, Scale, and Control", 2026-08-18. https://www.confluent.io/blog/2026-q3-confluent-intelligence-ai-update/
12. S. Kozlovski, 2 Minute Streaming, "Kafka 2025 Wrapped", 2025-12-30. https://blog.2minutestreaming.com/p/apache-kafka-2025-recap
13. AI TL;DR, "Trace Sampling for LLM Apps", updated 2026-06-13. https://ai-tldr.dev/learn/production-llmops/observability-monitoring/trace-sampling-llm/
14. Tian Pan, "Shadow Traffic for AI Systems", 2026-04-19. https://tianpan.co/blog/2026/04/19/shadow-traffic-ai-systems-production-replay
15. Tian Pan, "Data Flywheels for LLM Applications", 2025-09-28. https://tianpan.co/blog/2025/09/28/data-flywheels-llm-applications
16. NVIDIA **[vendor]**, "data-flywheel" AI Blueprint README, accessed 2026-10-19. https://github.com/NVIDIA-AI-Blueprints/data-flywheel
17. EU AI Act, Article 12 (artificialintelligenceact.eu), accessed 2026-10-19. https://artificialintelligenceact.eu/article/12/
18. Gibson Dunn, "EU AI Act Omnibus Agreement: Postponed High-Risk Deadlines", 2026-05-27. https://www.gibsondunn.com/eu-ai-act-omnibus-agreement-postponed-high-risk-deadlines-and-other-key-changes/
19. Hunton, "EU Digital Omnibus on AI Enters into Force", 2026-07 (publication and in-force dates as reported; Official Journal not checked). https://hunton.sitepilot11.firmseek.com/privacy-and-cybersecurity-law-blog/eu-digital-omnibus-on-ai-enters-into-force
20. EU AI Act, Article 26 (artificialintelligenceact.eu), and Lyceum, "High-Risk AI Deadlines: When Does Article 26 Apply?", accessed 2026-10-19 (paraphrased). https://artificialintelligenceact.eu/article/26/ ; https://lyceum.technology/magazine/ai-act-article-26-deployer-logging-6-month/
21. Prediction Guard **[vendor]**, "RAG security: indirect prompt injection and knowledge base poisoning", 2026-06-19. https://predictionguard.com/blog/rag-security-indirect-prompt-injection-and-knowledge-base-poisoning
22. Philterd **[vendor]**, "Architecting Privacy in Kafka: Real-Time Redaction for Streaming Data", 2026-03-11. https://philterd.ai/blog/architecting-privacy-in-kafka-real-time-redaction-for-streaming-data/
23. Atlan **[vendor]**, "Real-Time Data for AI Agents", 2026-07-20. https://atlan.com/know/ai-agent/data-for-ai/real-time-data-for-ai-agents/
24. OpenTelemetry Python contrib, OpenAI instrumentation docs; Splunk **[vendor]** AI agent monitoring config (content capture defaults to `NO_CONTENT`), accessed 2026-10-19 (paraphrased). https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation-genai/openai.html ; https://help.splunk.com/en/splunk-observability-cloud/observability-for-ai/splunk-ai-agent-monitoring/configure-ai-agent-monitoring
25. Redpanda **[vendor]**, Redpanda Connect `openai_embeddings` processor docs and "New AI connectors and GPU runtime support", accessed 2026-10-19. https://docs.redpanda.com/connect/components/processors/openai_embeddings.md ; https://redpanda.com/blog/ai-connectors-gpu-runtime-support
26. Materialize **[vendor]** homepage ("The Live Context Layer for Agents and Apps"); RisingWave **[vendor]**, "MCP Streaming Database", accessed 2026-10-19. https://materialize.com/ ; https://risingwave.com/blog/mcp-streaming-database-connect-ai-agents-risingwave/
27. Arize **[vendor]**, "What Is Shadow Testing (LLMs / Agents)", accessed 2026-10-19 (paraphrased). https://arize.com/glossary/shadow-testing-llms-agents/
