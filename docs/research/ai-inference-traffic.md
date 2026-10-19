# AI inference traffic

Research date: 2026-10-19. Scope: the queueing and messaging problems that calls to large language models and other inference endpoints create for application teams in 2025-2026, and what Tributary could do about them natively.

## Method and limits

Sources are provider, gateway and inference-server documentation, GitHub issues, developer-forum threads and engineering blogs. **[V]** marks a vendor source (one selling the product discussed or a fix for the problem); vendor incident figures are treated as unverified. Reddit could not be fetched. Hacker News searches returned little on topic, so community sentiment comes mainly from GitHub issues and the OpenAI forum. Undated documentation is cited with the date read (2026-10-19). Quotes are copied from fetched pages; figures seen only in search summaries are paraphrased and marked.

## Challenges

### 1. Provider rate limits and admission control

**What goes wrong.** Providers limit several things at once. Anthropic measures "requests per minute (RPM), input tokens per minute (ITPM), and output tokens per minute (OTPM) for each model class", uses a token bucket, and warns that "a rate of 60 requests per minute (RPM) might be enforced as 1 request per second" [1]. Limits nest: organisation limits always apply, and workspaces can be given lower ones [1]. OpenAI applies limits at organisation and project level, counts requests and tokens separately, and counts failed requests against the per-minute budget (paraphrased from a search summary of [2]). On top of steady limits there are "acceleration limits ... if your organization has a sharp increase in usage" [1], so a queue that drains a backlog at full speed can be throttled even under its nominal limit. Bedrock users report token-per-day quotas stuck at zero [3].

The cost of a message is not known until the call finishes. Anthropic estimates input tokens at the start and "adjusted during the request", and counts output "in real time as output tokens are produced" [1]. Kong's gateway admits the same gap: "The cost for the AI Proxy or AI Proxy Advanced is only reflected during the next request" [4][V]. Counting messages is the wrong unit: one message can be 50 tokens or 200,000.

A 429 also means different things. Anthropic's spend-cap 429 has "no `retry-after` header. Retrying, including the SDK's automatic retries, fails until access resumes" [1]; treating it as retryable burns attempts and dead-letters good work.

**How teams solve it today.** A gateway in front of the provider: LiteLLM reserves "a percentage of your model's total TPM/RPM to specific priority levels" and has a separate priority queue for high traffic [5][V]; Envoy AI Gateway counts tokens per route and model using a Redis-backed global rate limit [6]; Portkey retries on chosen status codes but will "skip retries and return the error" if the provider asks for more than 60 seconds [7][V]. The queue behind the gateway knows nothing of these limits, so workers pull, hit 429 and retry blindly.

**What a message platform could do.** Meter dispatch in tokens, estimated from a header and corrected by usage reported on ack; let a consumer pause a subscription (or every subscription sharing one provider quota) for `retry_after` without spending an attempt.

### 2. Priority and fairness

**What goes wrong.** Interactive chat and bulk jobs share one quota or GPU pool. Red Hat's llm-d team describes the trade-off: teams "provision dedicated GPU pools to handle peak demand and isolate latency-sensitive workloads", which then sit idle, but sharing them means interactive requests compete with batch work [8]. In multi-tenant products one customer's bulk import raises everyone's latency; AWS notes that "Regular queues deliver older messages first, which increases message dwell time for all tenants" [9][V].

**How teams solve it today.** SQS fair queues (July 2025) use the message group ID as a tenant key; SQS tracks in-flight messages per tenant and "automatically adjusts message delivery order to prioritize messages belonging to quiet (non-noisy) tenants" [9][V]. Inside inference servers, vLLM started as "first-come-first-served scheduling based on the arrival time of requests"; priority mode landed in 2024, with tenant-level policy pushed above the engine [10]. llm-d now does that at the gateway: requests are classified by fairness ID and priority, queued centrally, and dispatched by priority band then round-robin across tenants, because "Round-robin prevents a high-volume tenant from taking every dispatch turn" [8]. Dispatch waits while the pool is saturated: "Admission determines when a request can advance. Scheduling determines where that admitted request should run" [8].

**What a message platform could do.** Put priority bands, per-tenant fair dispatch and saturation-gated admission in the broker's dispatch path, so consumers need no separate gateway queue.

### 3. Batch inference

**What goes wrong.** Batch APIs are half price but asynchronous with a long window. OpenAI's window is fixed at 24 hours; unfinished work is cancelled and completed work charged (paraphrased from [11][V]). Vertex AI offers 50 % off with a 24-hour turnaround target, and batch and cache discounts do not stack (paraphrased from [12][V]). Anthropic's Message Batches API gives "50% cost reduction" [26]. Anthropic limits the number of batch requests "in the processing queue at the same time" (200,000 to 500,000 depending on tier) and up to 100,000 per batch [1]. Bedrock wants JSONL files in S3 with a per-model minimum number of records [13][V]. Teams must pack requests into files within minimums and maxima, wait up to a day, then split results back to requests and retry failures.

**How teams solve it today.** Cron jobs or workflow engines drain a queue into a file, wait for a `batch.completed` webhook (the payload holds only the batch ID [14][V]), download results and fan them out. Result matching, partial failures and expiry are hand-written each time.

**What a message platform could do.** Accumulate messages into a batch by count, bytes or age; park the members under a long external lease; then ack, retry or dead-letter each member from the per-item results.

### 4. Long and variable latency, streaming and resumption

**What goes wrong.** Calls take seconds to many minutes; agent runs longer. OpenAI added background mode so long tasks run without holding a connection, with response data kept about 10 minutes for polling [15][V]; a forum thread from 2026-08-02 reports background calls "failing intermittently with no rhyme or reason" and stuck `in_progress` for 30+ minutes [16]. Short leases cause duplicate runs; long fixed leases delay recovery from dead workers.

Users watch tokens stream in. When the connection drops, the work does not stop: "The worker node is entirely unaware that this happens and keeps adding chunks to the stream", and without extra machinery "the stream will be completely dead, until you refresh the page" [17] (2025-12-15). The common fix buffers chunks in Redis keyed by message ID and replays on reconnect [17][18][V]. Ably points out the limit of that pattern: "The Redis stream is attached to a stream ID, not to a session that exists independently of the stream", and "A second device joining the same conversation has nothing to subscribe to" [19][V].

**What a message platform could do.** Lease extensions that carry a progress checkpoint; per-request output streams as keyed, short-retention topics that any number of readers attach to from an offset (the SSE `Last-Event-ID`); and cancellation so abandoned requests stop spending tokens.

### 5. Cost: duplicates, caching, attribution and caps

**What goes wrong.** Retries are billable. A forum post from 2026-09-22 states the core problem: "if my backend sends a request to an AI API and the request times out, I might retry it. But what if the first request was actually processed and I just didn't receive the response?" [20]. Yotta Labs: "The original provider call may still complete and consume tokens" [21][V]. Agent retries grow: "Context grows every attempt, so attempt seven costs several times attempt one", and "None of the above trips a rate limit" because each call is under quota [22][V]. An OpenClaw issue shows the reverse failure, where retrying the same overloaded provider for two hours left "The agent ... effectively silent despite having GPT-4o available" [23].

Prompt caching helps: Anthropic does not count cache reads toward ITPM for most models [1]. Semantic caches are less safe: published production hit rates "range from 20-45%, not 90-95%", and a bad answer once cached means "every similar future query gets that same bad answer" [24]. Token counts arrive only with the response, so per-tenant attribution and budgets lag by a request [4][V].

**How teams solve it today.** Own request-ID tables, gateway spend limits (Kong, LiteLLM, Cloudflare [25][V]) and spend-velocity alerts.

**What a message platform could do.** A stable operation ID across attempts; usage reported on ack and aggregated by tenant and feature; per-tenant budgets that pause dispatch; retry caps in cost, not only count.

### 6. Multimodal and large payloads

**What goes wrong.** Requests carry PDFs, images, audio and video. Anthropic allows 32 MB on the Messages API, 256 MB for Message Batches and 500 MB on the Files API, and notes "Bedrock limits requests to 20 MB, and Google Cloud limits requests to 30 MB" [26]. OpenAI files can be 512 MB [27][V]. Brokers cap messages far lower (SQS 1 MiB), so teams store payloads elsewhere, pass a pointer and own the cleanup.

**What a message platform could do.** A broker-managed claim check whose object lifetime follows retention and the slowest subscription that owes the message.

### 7. Fallback and routing

**What goes wrong.** Teams configure fallbacks, then find they do not fire. The OpenClaw issue (2026-03-17): "OpenClaw does not activate the configured model fallback chain ... This makes the fallback configuration unreliable for the most common real-world outage scenario (provider overload)" [23].

**How teams solve it today.** Azure spillover sends provisioned-throughput overflow to a standard deployment on `429`, some `400`s and `500`/`503`, but only within "the same Azure OpenAI resource" [28][V]. Portkey composes retries, weighted load balancing and fallbacks, triggering fallback only on listed status codes and exhausting retries on the primary first [7][V].

**What a message platform could do.** Route by failure reason: an `overloaded` nack or open circuit forwards the message to an alternate subscription (another provider's workers), keeping attempt history and recording the route.

## Pain table

| Pain | Evidence (quote + link + date) | Severity 1-5 | Current workaround |
| --- | --- | --- | --- |
| Count-based limits ignore token cost | "ITPM rate limits are estimated at the beginning of each request, and the estimate is adjusted during the request" [Anthropic, read 2026-10-19](https://platform.claude.com/docs/en/api/rate-limits) | 5 | Gateway token limiters (Envoy, LiteLLM, Kong) in front of a count-blind queue |
| 429s burn retries and attempts; spend-cap 429 never clears | "the response has no `retry-after` header. Retrying ... fails until access resumes" [Anthropic, read 2026-10-19](https://platform.claude.com/docs/en/api/rate-limits) | 4 | SDK retries plus hand-written error-code checks |
| Bulk work starves interactive traffic | "Round-robin prevents a high-volume tenant from taking every dispatch turn" [Red Hat, 2026-08-27](https://developers.redhat.com/articles/2026/08/27/llm-d-flow-control-priority-queuing-for-shared-gpu-inference) | 5 | Dedicated pools; llm-d flow control; LiteLLM reservations |
| Noisy tenant raises everyone's latency | "Regular queues deliver older messages first, which increases message dwell time for all tenants" [AWS, 2025-07-21](https://aws.amazon.com/blogs/compute/building-resilient-multi-tenant-systems-with-amazon-sqs-fair-queues) [V] | 4 | SQS fair queues; queue per tenant |
| Batch APIs need hand-built accumulate/park/split pipelines | "A batch request is considered part of the processing queue when it has yet to be successfully processed by the model" [Anthropic, read 2026-10-19](https://platform.claude.com/docs/en/api/rate-limits) | 3 | Cron or workflow engine, result-file parsing |
| Long calls hang or stall | "failing intermittently with no rhyme or reason" [OpenAI forum, 2026-08-02](https://community.openai.com/t/background-mode-requests-stuck-in-in-progress-for-30-minutes-responses-api/1388809) | 4 | Background mode plus polling; long leases with heartbeats |
| Token streams die on disconnect | "the stream will be completely dead, until you refresh the page" [Stardrift, 2025-12-15](https://stardrift.ai/blog/streaming-resumptions) | 4 | Redis streams keyed by message ID; Vercel resumable-stream |
| Resumption is per stream, not per session | "A second device joining the same conversation has nothing to subscribe to" [Ably, 2026-06-30](https://ably.com/blog/ai-chat-stream-resumption) [V] | 3 | Extra fan-out layer or hosted realtime service |
| Timeouts cause paid duplicate generations | "what if the first request was actually processed and I just didn't receive the response?" [OpenAI forum, 2026-09-22](https://community.openai.com/t/how-are-you-handling-idempotency-with-ai-api-requests/1399903) | 4 | Own request-ID table; no provider idempotency |
| Retry loops run up spend under quota | "None of the above trips a rate limit" [Requesty, 2026-07-22](https://www.requesty.ai/blog/how-to-cap-runaway-agent-spend-2026) [V] | 5 | Spend-velocity alerts; gateway budgets that lag a request |
| Semantic cache serves wrong answers | "every similar future query gets that same bad answer" [dev.to, Apr 5, year not shown](https://dev.to/gauravdagde/llm-semantic-caching-the-95-hit-rate-myth-and-what-production-data-actually-shows-8ga) | 2 | Exact-match cache, high thresholds, kill switch |
| Fallback chain does not fire on overload | "does not activate the configured model fallback chain" [OpenClaw #49079, 2026-03-17](https://github.com/openclaw/openclaw/issues/49079) | 4 | Gateway fallback configs (Portkey, Azure spillover) |
| Payloads exceed broker limits | "If you exceed these limits, you'll receive a 413 `request_too_large` error" (32 MB Messages, 500 MB Files) [Anthropic, read 2026-10-19](https://platform.claude.com/docs/en/api/overview) | 3 | Manual claim check via S3 or GCS, manual cleanup |

## Requirements for Tributary

- **I1. Token-weighted rate limits.** `rate_limit` can be set in cost units taken from a header (such as `x-est-tokens`), charged at dispatch and corrected by actual units reported on ack. Test: with mixed 100- and 5,000-unit messages, dispatched units stay within 10 % of a 10,000/min limit over 5 minutes. *Subscription setting.*
- **I2. Shared quota pools.** Named token buckets, nestable (organisation, project, key), shared by subscriptions across namespaces. Test: two subscriptions on one pool never exceed it together; a child never exceeds its parent. *New primitive.*
- **I3. Upstream throttle signal.** A nack with `throttled` and `retry_after` pauses the subscription or pool for that long and does not count an attempt. Test: 100 throttled nacks leave attempts at 0 and no dead letters. *Subscription setting (`failure.throttle`).*
- **I4. Hard-stop reason.** A `quota_exhausted` nack suspends the subscription or pool until resumed by an operator or at a set time, and alerts. Test: one such nack stops further deliveries. *Subscription setting.*
- **I5. Priority bands with ageing.** Dispatch serves higher priority-header bands first, with weights and ageing against starvation. Test: under saturation no low-priority message waits beyond its configured maximum. *Subscription setting; `interactive-vs-batch` profile.*
- **I6. Fair dispatch by tenant key.** `shared` dispatch round-robins (optionally weighted) across a tenant header, with a per-tenant in-flight cap. Test: beside a tenant with 1 million backlog, small tenants' dwell time stays within 2x of an idle system. *Subscription setting.*
- **I7. Saturation-gated admission.** Dispatch holds while a consumer-reported or polled saturation signal is above a threshold. Test: dispatch stops within 1 s of crossing it. *Subscription setting.*
- **I8. Progress-carrying leases.** A lease extension can attach a checkpoint (up to 4 KiB) delivered on redelivery with attempt count and prior holder. Test: kill a worker after a checkpoint; the next delivery has it. *Subscription setting (extends `lease`).*
- **I9. Operation ID across attempts.** Each message has a stable operation ID, distinct from delivery tokens, for use as an idempotency key. Test: all redeliveries show the same ID. *Built into the message model.*
- **I10. Per-request output streams.** A `token-stream` topic profile: one key per request, minutes of retention, readers attach by key from an offset, end-of-stream marker. Test: a reader reconnecting at offset 40 gets chunks 41 onward once; a second device sees the whole stream. *Topic profile plus key-scoped read (new primitive).*
- **I11. Cancellation.** Cancel by message ID removes an undelivered message or signals the lease holder. Test: the holder receives the signal within 1 s. *New primitive.*
- **I12. Batch accumulate and park.** A `provider-batch` profile groups messages by count, bytes or age, parks members under an external lease of up to 48 h, and accepts per-member ack, nack or dead-letter from any process holding the batch token (such as a webhook handler). Test: a 10,000-item batch with 37 failures yields 9,963 acks and 37 retries. *Profile plus external-completion primitive.*
- **I13. Usage reporting and attribution.** Ack carries usage (tokens in and out, cost), aggregated in metrics by subscription and configured headers. Test: per-tenant reported cost equals the metric. *Subscription setting; feeds section 7.5.*
- **I14. Budgets and cost-capped retries.** A tenant or pool budget pauses that tenant's dispatch when reached; `failure.max_cost` dead-letters a message whose summed cost across attempts passes the cap. Test: rising-cost retries are dead-lettered at the cap. *Subscription setting.*
- **I15. Broker-managed claim check.** Bodies above a topic threshold go to object storage behind a reference, deleted only after retention and once no subscription owes them. Test: a 100 MB message round-trips and its object is removed afterwards. *Topic setting.*
- **I16. Reason-based fallback routing.** `failure.on_reason` forwards on nack reasons (`overloaded`, repeated `throttled`, open circuit) to another subscription or topic, keeping operation ID and history and adding a `routed-from` header. Test: with the primary overloaded, messages reach the fallback within the configured delay. *Subscription setting.*

## Sources

1. Anthropic, "Rate limits", platform.claude.com/docs/en/api/rate-limits, read 2026-10-19 [V]
2. OpenAI Help Center, "Troubleshooting API rate limits and 429 errors", help.openai.com/en/articles/5955604, read via search 2026-10-19 (page returned 403 to direct fetch) [V]
3. AWS re:Post, "All Bedrock model quotas stuck at 0 tokens/day", repost.aws/questions/QUt6wvrkLHQwq6yq52nVADvA, 2026 (user reports, unverified)
4. Kong, "AI Rate Limiting Advanced plugin", developer.konghq.com/plugins/ai-rate-limiting-advanced/, read 2026-10-19 [V]
5. LiteLLM, "Dynamic rate limit / priority reservation", docs.litellm.ai/docs/proxy/dynamic_rate_limit, read 2026-10-19 (feature from v1.77.3) [V]
6. Envoy AI Gateway, "Usage-based rate limiting" and "Capabilities", aigateway.envoyproxy.io/docs/capabilities/, read 2026-10-19
7. Portkey, "Fallbacks" and "Retries", portkey.ai/docs/product/ai-gateway/fallbacks, read 2026-10-19 [V]
8. Griffith and Saini, Red Hat Developer, "llm-d flow control: Priority queuing for shared GPU inference", 2026-08-27; llm-d docs, llm-d.ai/docs/architecture/core/router/epp/flow-control
9. AWS Compute Blog, "Building resilient multi-tenant systems with Amazon SQS fair queues", 2025-07-21 [V]
10. vLLM, "[RFC]: Priority Scheduling", github.com/vllm-project/vllm/issues/6077, opened 2024-07-02, closed 2024-09-25
11. OpenAI Help Center, "Batch API FAQ", help.openai.com/en/articles/9197833, read via search 2026-10-19 [V]
12. Google Cloud, "Batch inference with Gemini", docs.cloud.google.com/vertex-ai/generative-ai/docs/multimodal/batch-prediction-gemini, read 2026-10-19 [V]
13. AWS, "Format and upload your batch inference data", docs.aws.amazon.com/bedrock/latest/userguide/batch-inference-data, read 2026-10-19 [V]
14. OpenAI, "Webhook events", developers.openai.com/api/docs/api-reference/webhook-events, read 2026-10-19 [V]
15. OpenAI, "Background mode", developers.openai.com/api/docs/guides/background, read 2026-10-19 [V]
16. OpenAI Developer Forum, "Background mode requests stuck in 'in_progress' for 30+ minutes", 2026-08-02
17. Stardrift, "Is resumable LLM streaming hard? No, it's just annoying.", stardrift.ai/blog/streaming-resumptions, 2025-12-15
18. Vercel, resumable-stream, github.com/vercel/resumable-stream, and AI SDK "Chatbot Resume Streams", read 2026-10-19 [V]
19. Ably, "AI chat stream resumption: when Redis is enough, and when you need durable sessions", ably.com/blog/ai-chat-stream-resumption, 2026-06-30 [V]
20. OpenAI Developer Forum, "How are you handling idempotency with AI API requests?", 2026-09-22
21. Yotta Labs, "Prevent duplicate LLM retry charges", yottalabs.ai/post/prevent-duplicate-llm-retry-charges, 2026-03-11 [V]
22. Requesty, "$1.8k before anyone noticed: how to cap runaway agent spend", requesty.ai/blog/how-to-cap-runaway-agent-spend-2026, updated 2026-07-22 [V]
23. OpenClaw, "HTTP 529 (Anthropic overloaded) does not trigger model fallback chain", github.com/openclaw/openclaw/issues/49079, 2026-03-17
24. Gaurav Dagde, "LLM Semantic Caching: The 95% Hit Rate Myth", dev.to, posted Apr 5 (year not shown)
25. Cloudflare, "AI Gateway features", developers.cloudflare.com/ai-gateway/features/, read 2026-10-19 [V]
26. Anthropic, "API overview" (request size limits), platform.claude.com/docs/en/api/overview, read 2026-10-19 [V]
27. OpenAI, "Upload file" API reference, developers.openai.com/api/reference, read via search 2026-10-19 [V]
28. Microsoft Learn, "Manage traffic with spillover for provisioned deployments", learn.microsoft.com/en-us/azure/foundry/openai/how-to/spillover-traffic-management, updated 2026-06-18 [V]
