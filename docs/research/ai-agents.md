# AI agents and multi-agent systems

Research date: 2026-10-19. Scope: production problems of AI agents that touch messaging, queues,
streams and durable execution, and what Tributary should do about them.

## Method and limits

- Web search and direct reads of vendor docs, engineering blogs, GitHub issues, protocol specs and
  Hacker News. reddit.com could not be read (fetch blocked), as in the other reports.
- **Vendor sources are marked [V].** Most writing on this topic comes from companies selling a
  durable-execution engine, a streaming platform or an agent runtime. Independent evidence is
  thinner here than for Kafka or RabbitMQ, and community complaints are mostly in GitHub issues
  and HN threads.
- Widely repeated incident stories without a primary source are marked unverified.
- Specs move fast (MCP 2026-07-28, A2A 1.0); statements hold as of the dates given.

## Challenges

### 1. Durable execution for agents

**What goes wrong.** An agent run is a loop of model calls and tool calls that can last minutes to
hours. Put it on a plain queue and a crash, deploy or lease timeout restarts it from zero: the
model calls are paid for again, tools that already ran (emails, payments, tickets) run again, and
because model output varies, the second run may take a different path. Anthropic's research
agent team wrote that "Agents are stateful and errors compound" and "restarts are expensive and
frustrating for users", so they built resumption from checkpoints and used "rainbow deployments
to avoid disrupting running agents" [1, 2025-06-13, V]. Inngest: "If your serverless function
times out, you lose everything" [2, 2026-02-19, V].

**How teams solve it.** Durable-execution engines journal each model call and tool call as a step
and, on recovery, re-run the code while returning saved results instead of calling again.
Temporal keeps LLM calls in activities; "The Workflow replays your agent's progress using the
recorded LLM decisions from the Event History" [4, 2025-11-12, V]. Lambda durable functions
(re:Invent 2025) re-run the handler from the start, skip completed steps, and AWS advises
explicit versions so "replays always happen with the same code with which the execution was
started" [6, 2026-02-06, V]. DBOS instead commits step output in the application's own Postgres [7].

**Complaints.**
- **History and payload size.** Every model call carries the growing conversation, so history
  grows quadratically. A Temporal user measured "model call inputs were 99.8% of the workflow
  history" and hit "Workflow history size exceeds limit"; past about 450k tokens the 2 MiB
  per-payload limit is hit first. External payload storage fixed it (50.48 MB to 0.12 MB), but it
  is "still marked experimental" [3, 2026-09-21]. A team running an SDLC agent on Temporal hit
  "about 2 MB" per payload and a gRPC ceiling "around 4 MB", and bounded loop iterations and used
  continue-as-new [23, Jul 17, year not shown].
- **Determinism and versioning.** Replay requires deterministic orchestration code; changing code
  under in-flight runs is hard. An HN user on DBOS: it "seems currently impossible to properly
  upgrade inflight workflow"; on Temporal: "a pretty hard vendor lock-in" and "too heavy for a lot
  of use-cases"; on Inngest: "the typescript API is a bit clunky" [7, 2025-08-09].
- **Idempotency is still on you.** An HN commenter on Temporal: "you do have to work hard and
  make everything idempotent" [25, quoted 2026-06-22].

**What a message platform could do natively.** Not replay code. It can keep a run's work item
alive for hours (long, extendable leases), hand the consumer a small progress marker on
redelivery so it can skip finished work, offload large payloads by reference, and let a nack say
"do not retry" so retries happen in one layer only.

### 2. Human-in-the-loop and long waits

**What goes wrong.** Approvals take hours or days. Holding a worker or lease that long is not viable.

**How teams solve it.** Every engine now has a wait primitive with a timeout: Lambda
`waitForCallback` can "suspend execution for up to one year ... such as human-in-the-loop
processes" [6, V]; Cloudflare Workflows `step.waitForEvent` takes a type and a timeout of 1 second
to 365 days and fails the instance on timeout unless caught [13, 2025-04-07, V]; Trigger.dev
waitpoint tokens release the compute slot, take a timeout, accept an idempotency key, and can be
completed by anyone holding the token URL [12, V]; Temporal uses signals and timers, and a team
reports a workflow can "pause and wait for an external approval, with a timeout that escalates if
nobody responds" [23]. A2A models the same thing as `TASK_STATE_INPUT_REQUIRED` and
`TASK_STATE_AUTH_REQUIRED` interrupted states [15].

**What a message platform could do.** The building blocks are correlation and timers: a durable
inbox keyed by correlation ID that lasts days (Tributary's `rpc-inbox` is in-memory, 5 minutes),
and a delayed "timeout" message that can be cancelled when the reply arrives. The run's state
stays in the consumer or the engine.

### 3. Tool-call side effects and idempotency

**What goes wrong.** "The payment runs again. The customer is charged twice"; retries happen at
the model layer, and "Nothing in this loop tracks whether the tool already executed its side
effects" [14, 2026-04-19]. 

**How teams solve it.** A stable idempotency key derived from the run and step, never from a
timestamp, passed to the provider that performs the effect; classify tools as read-only (retry
freely) or effectful (key required); sagas with compensations, which "must themselves be
idempotent" [14]. A2A pushes the problem to receivers: "Clients SHOULD process notifications
idempotently, as duplicate deliveries may occur" [15].

**What a message platform could do.** Give every delivery a stable effect key (message ID plus a
caller-supplied step name), send it as an `Idempotency-Key` header on webhooks, keep idempotency
windows long enough for multi-day runs, and keep shipping the inbox helper. Compensation logic is
application or engine territory.

### 4. Agent concurrency control

**What goes wrong.** Two runs on one conversation interleave; a user who says "cancel that"
mid-run gets two answers; one tenant's burst eats the LLM rate limit.

**How teams solve it.** Engines added per-key controls. Inngest singleton `mode: "cancel"`:
"Inngest cancels the active run and starts a run for the new event" when "The newest event should
replace older work", with limits: "A currently executing `step.run` finishes before cancellation
takes effect" and "During a rapid burst of matching events, cancel mode can skip some new runs"
[8, V]. Inngest flow control adds per-user concurrency keys, throttling and debounce, "critical
for AI services that are under heavy load" [9, updated 2026-01-07, V]. Hatchet offers
`GROUP_ROUND_ROBIN` fairness by key and `CANCEL_IN_PROGRESS` so the newest run wins [10, V].
Restate serialises writes per session key with virtual objects [11, V].

**What a message platform could do.** This is the strongest fit. Tributary already has per-key
ordering (one message per key in flight). Adding a per-key policy for new arrivals (queue,
supersede, coalesce), debounce, per-key concurrency above one, and round-robin across keys covers
most of it as subscription settings.

### 5. Multi-agent coordination

**What goes wrong.** Synchronous agent-to-agent calls block: "the entire system can be blocked
while waiting for a single subagent" [1]. Protocol transports are HTTP-first and weak on
delivery. A2A 1.0 binds to JSON-RPC, gRPC and HTTP+JSON with SSE streaming and webhook push;
reconnecting clients "MAY not receive all status update messages", streams "MUST NOT be
considered a reliable delivery mechanism for critical information", and webhook senders need only
"attempt delivery at least once" and "MAY implement retry logic" [15, v1.0]. MCP's 2026-07-28
spec "officially retired the `initialize`/`initialized` exchange along with the `Mcp-Session-Id`
header" and moved change notifications to "a single `subscriptions/listen` stream"; applications
that need state should "mint an explicit handle from a tool and have the model pass it back"
[16, 2026-07-28].

**How event streams are positioned.** Confluent Streaming Agents run agents inside Flink
pipelines; the pitch is fresh context and replay of agent decisions from the Kafka log, though
it "requires organizations to embrace stream processing as a primary infrastructure approach"
[18, 2025-08-21]. Redpanda's Agentic Data Plane is governance-first: "Agents need centralized
governance, enforceable guardrails, and the ability to explain when things go wrong" [19,
2025-10-28, V]. Solace Agent Mesh routes agent-to-agent traffic over Solace topics (Enterprise GA
2025-12-02) [24, V]. I found no official A2A or MCP binding for Kafka or NATS.

**What works vs hype.** Real: async fan-out to sub-agents, durable buffering of callbacks, and
replaying history to debug. Unproven: "streaming platform as agent brain"; I found no independent
case study of it beating HTTP plus a durable engine.

**What a message platform could do.** Be the reliable mailbox behind HTTP protocols: accept A2A
push webhooks and MCP-triggered work with dedupe, and deliver them with the guarantees the
protocols lack. It should not invent its own agent protocol.

### 6. Agent memory and state

**What goes wrong.** Conversation history is the state, and it is append-only and keyed by
session, which looks like a log. But engines store it as workflow history (size limits, item 1)
and frameworks store it as checkpoints in a database.

**How teams solve it.** LangGraph checkpointers support replay from a past checkpoint (later
nodes run again, so model calls "can return different results") and forking by updating state at
a checkpoint, which "does not roll back a thread" [20, V]. Restate keeps it in per-session state [11, V].

**What a message platform could do.** Read one key's history in order (the per-key index already
exists), seek a subscription to replay one conversation into a debug consumer, and copy a key's
history to a new key to fork. 

### 7. Observability and audit

**What goes wrong.** A run crosses model calls, tools, queues and other agents; traces break at
each async hop. Standard attributes are unsettled: as of July 2026 "no GenAI-specific span, event,
metric, or attribute ... is marked Stable", the conventions moved to a separate repository with
"no releases or tags yet", and agent span shapes changed in April 2026 [17, 2026-07-17]. Anthropic
reports that "Adding full production tracing let us diagnose why agents failed" and that
"multi-agent systems use about 15× more tokens than chats" [1], so cost per run matters.

**What a message platform could do.** Carry W3C trace context untouched, emit messaging spans
with links, record per-message delivery history (who received it, when, attempt, outcome) as an
audit trail, and aggregate consumer-reported cost by key or label. It should not adopt GenAI
attribute names until they are stable.

### 8. Safety and control

**What goes wrong.** Loops that stay under rate limits keep spending: agents "repeat the same step
all night" and "run up an API bill" [21, 2026-10-12, V]. Multi-agent ping-pong between two agents
is the classic case; stories of five-figure bills circulate but are unverified.

**How teams solve it.** Per-run cost ceilings and step limits [21, V], gateway spend caps and
hand-built kill switches.

**What a message platform could do.** It sees every hop between agents, so it can count them: a
hop counter on causally produced messages with a maximum, per-key message quotas per window that
park a runaway key, and an instant pause for a subscription, key or tenant label that also
revokes in-flight leases.

## Pain table

| Pain | Evidence (quote + link + date) | Severity 1-5 | Current workaround |
| --- | --- | --- | --- |
| Restart from zero repeats model and tool calls | "restarts are expensive and frustrating for users" [1](https://www.anthropic.com/engineering/multi-agent-research-system) 2025-06-13 [V] | 5 | Durable-execution engine; hand-rolled checkpoints |
| Duplicate side effects on retry | "The payment runs again. The customer is charged twice" [14](https://tianpan.co/blog/2026/04/19/idempotency-agentic-tool-calling-saga-deduplication) 2026-04-19 | 5 | Stable idempotency keys; sagas |
| History/payload limits in engines | "model call inputs were 99.8% of the workflow history" [3](https://github.com/temporalio/sdk-python/issues/1890) 2026-09-21 | 4 | External payload storage (preview), continue-as-new, context trimming |
| Engine weight, lock-in, versioning in-flight runs | "a pretty hard vendor lock-in"; "seems currently impossible to properly upgrade inflight workflow" [7](https://news.ycombinator.com/item?id=44842954) 2025-08-09 | 3 | Rainbow deploys, pinned versions, patching APIs |
| Superseded runs on new user message | "A currently executing `step.run` finishes before cancellation takes effect" [8](https://www.inngest.com/docs/guides/singleton) accessed 2026-10-19 [V] | 4 | Singleton cancel, `CANCEL_IN_PROGRESS`, check-between-steps |
| Tenant bursts exhaust LLM limits | "AI APIs are rate-limited. Scale isn't infinite." [9](https://www.inngest.com/blog/ai-in-production-managing-capacity-with-flow-control) updated 2026-01-07 [V] | 4 | Per-key concurrency, throttle, round-robin fairness |
| Long human waits | Lambda can "suspend execution for up to one year ... human-in-the-loop" [6](https://aws.amazon.com/blogs/compute/building-fault-tolerant-long-running-application-with-aws-lambda-durable-functions) 2026-02-06 [V] | 3 | Engine wait/signal primitives with timeouts |
| Lossy agent-protocol notifications | Streams "MUST NOT be considered a reliable delivery mechanism for critical information" [15](https://a2a-protocol.org/latest/specification/) v1.0, accessed 2026-10-19 | 4 | Polling `GetTask`, idempotent webhook receivers |
| Blocking synchronous sub-agents | "the entire system can be blocked while waiting for a single subagent" [1](https://www.anthropic.com/engineering/multi-agent-research-system) 2025-06-13 [V] | 3 | Async fan-out via queues or engine child runs |
| No stable GenAI telemetry schema | "no GenAI-specific span, event, metric, or attribute ... is marked Stable" [17](https://john-hodge.com/blog/opentelemetry-genai-semantic-conventions/) 2026-07-17 | 3 | Vendor attributes, pin SDK versions |
| Runaway loops and spend | Agents "repeat the same step all night" and "run up an API bill" [21](https://dev.to/vijaym2k6/your-ai-agent-needs-a-brake-pedal-stopping-loops-and-runaway-cost-at-runtime-4jc4) 2026-10-12 [V] | 4 | Per-run budgets, step limits, gateway caps |
| Stateful MCP sessions vs load balancers | Spec "retired the `initialize`/`initialized` exchange along with the `Mcp-Session-Id` header" [16](https://blog.modelcontextprotocol.io/posts/2026-07-28/) 2026-07-28 | 2 (now fixed in spec) | Sticky sessions until SDKs adopt 2026-07-28 |

## Where a message platform should stop

Tributary should own **the unit of work and the wire between agents**: one message, one lease,
retries, per-key exclusivity, cancellation, timers, correlation, fair dispatch, quotas, tracing and
audit of each delivery. It should not own **the code path inside a run**.

Belongs in a workflow engine (Temporal, Restate, DBOS, Inngest, Lambda durable functions), not
Tributary:
- journaling each step and replaying code deterministically;
- workflow versioning and patching of in-flight runs;
- compensation and saga orchestration;
- run-scoped state beyond a small progress marker;
- conversation memory, summarisation, retrieval.

Rule of thumb: a run that fits in one lease and can resume from a progress marker needs only
Tributary; many effectful steps or multi-day stateful waits need an engine.

**Integration point.** Tributary sits on both sides of the engine: (1) a subscription starts runs
(webhook or push into the engine's start API, with the message ID as the run ID so redelivery is
idempotent); (2) replies, approvals and A2A/MCP callbacks arrive on topics and are delivered to
the waiting run by correlation key as signals; (3) runs emit step and audit events to topics for
replay, analytics and other agents. 

## Requirements for Tributary

Each requirement names where it fits and a test.

- **A1. Supersede (subscription setting `per_key.on_arrival: queue | supersede`).** A new message
  for a key whose previous message is in flight makes that delivery's next extend or ack return
  `superseded` (push clients get a cancel notice); the new message dispatches when the old lease
  ends. Test: first holder sees `superseded` within one ack-flush interval.
- **A2. Per-key debounce and coalesce (subscription setting `per_key.debounce`, `coalesce: last |
  batch`).** Deliver after a quiet period, either the last message or all pending as one batch.
  Test: 5 messages in 1 s with 2 s debounce yield one delivery.
- **A3. Per-key concurrency and fairness (subscription settings `per_key.max_in_flight`,
  `fairness: round-robin-by <header>`).** Test: a tenant with 10,000 queued messages delays
  another tenant's first message by at most one dispatch round.
- **A4. Cancel by message ID or key (new small primitive).** Pending deliveries are removed and
  in-flight holders see `cancelled`; reason recorded. Test: no redelivery after cancel.
- **A5. Long leases with progress marker (subscription setting).** `lease.max_total` up to topic
  retention; extend may carry up to 64 KiB of `progress`, returned on redelivery. Test: crash
  after extend with progress P; redelivery carries P.
- **A6. Durable correlation inbox (topic profile `durable-inbox`).** Replicated, retention up to
  30 days, for approvals and request-reply that survive restarts. Test: reply after broker
  restart reaches the waiter.
- **A7. Cancellable delayed messages (topic setting).** Cancel a delayed message by ID before it
  is due, for approval timeouts. Test: cancelled timeout never delivers.
- **A8. Stable effect keys (client and webhook behaviour).** Clients expose `effect_key(step)` =
  hash(message ID, step); webhooks send `Idempotency-Key` equal to the message ID on every
  attempt. Test: three attempts carry the same header.
- **A9. Do-not-retry nack (subscription behaviour).** `nack(retry=false, reason)` applies
  `on_exhausted` at once. Test: one nack dead-letters with the reason.
- **A10. Large payload offload (topic setting `large_payload: reject | offload`, M4).** Oversized
  payloads go to object storage and are resolved by clients transparently. Test: 20 MiB message
  round-trips on a 1 MiB topic.
- **A11. Read and replay one key (new read API, subscription seek-by-key).** Uses the per-key
  index. Test: replaying key K redelivers only K's messages.
- **A12. Fork a key (new operation).** Copy a key's records up to an offset to a new key with
  `trib-forked-from`. Test: fork equals the source prefix.
- **A13. Trace pass-through (platform behaviour).** Keep `traceparent`, `tracestate`, `baggage`
  byte for byte, emit linked OTel messaging spans, no GenAI attributes until Stable. Test: header
  bytes unchanged end to end.
- **A14. Delivery audit (topic setting `audit: deliveries`).** Every attempt, consumer identity,
  outcome and reason, queryable by message ID. Test: three retries show three entries.
- **A15. Hop limit (topic setting `max_hops`).** Clients set `trib-hops` and `trib-causation-id`
  when producing inside a handler; over-limit messages are refused or dead-lettered. Test: an
  A-B-A loop stops at the limit.
- **A16. Per-key quota (subscription setting `per_key.quota`, `on_exceed: hold | dead-letter`).**
  Test: with quota 200/hour, the 201st message for a key is held; other keys flow.
- **A17. Kill switch (new operation).** Pause a subscription, key or header value (tenant, agent)
  cluster-wide within 1 s; in-flight extends return `paused`. Test: no deliveries after the pause
  is acknowledged.
- **A18. Cost annotations (subscription behaviour, metric).** Ack carries small numeric
  annotations (tokens, cents) summed per key and header label. Test: cost by tenant is reported.
- **A19. Callback ingest (new primitive, M3).** HTTPS endpoint that verifies HMAC or JWT, dedupes
  by a configured header and produces to a topic; fits A2A push notifications. Test: a duplicate
  POST is stored once.
- **A20. `agent-session` profile (profile).** Per-key ordering, supersede, 5 min lease extendable
  to 6 h, progress markers, 3 attempts, `on_exhausted: hold`, per-key quota. Test: validates
  against all compatibility rules.

## Sources

[V] marks vendor sources or sources from a company selling a related product.

1. Anthropic, "How we built our multi-agent research system", 2025-06-13 [V]. https://www.anthropic.com/engineering/multi-agent-research-system
2. Inngest, "Durable Execution: The Key to Harnessing AI Agents in Production", 2026-02-19 [V]. https://www.inngest.com/blog/durable-execution-key-to-harnessing-ai-agents
3. temporalio/sdk-python issue #1890, opened 2026-09-21, maintainer reply 2026-09-29. https://github.com/temporalio/sdk-python/issues/1890
4. Temporal, "Of course you can build dynamic AI agents with Temporal", 2025-11-12 [V]. https://temporal.io/blog/of-course-you-can-build-dynamic-ai-agents-with-temporal
5. Temporal docs, payload and gRPC message size limits, accessed 2026-10-19 [V]. https://docs.temporal.io/troubleshooting/blob-size-limit-error
6. AWS Compute Blog, Lambda durable functions, 2026-02-06 [V]. https://aws.amazon.com/blogs/compute/building-fault-tolerant-long-running-application-with-aws-lambda-durable-functions
7. Hacker News, "Build durable workflows with Postgres" thread, 2025-08-09. https://news.ycombinator.com/item?id=44842954
8. Inngest docs, Singleton functions, accessed 2026-10-19 [V]. https://www.inngest.com/docs/guides/singleton
9. Inngest, "AI in production: Managing capacity with flow control", 2024-04-18, updated 2026-01-07 [V]. https://www.inngest.com/blog/ai-in-production-managing-capacity-with-flow-control
10. Hatchet docs, concurrency control, accessed 2026-10-19 [V]. https://docs.hatchet.run/home/concurrency
11. Restate docs and blog, AI agents and sessions, accessed 2026-10-19 [V]. https://docs.restate.dev/tour/ai-agents ; https://restate.dev/blog/agentic-workflows-are-just-code-treat-them-that-way
12. Trigger.dev, waitpoints changelog and docs, accessed 2026-10-19 [V]. https://trigger.dev/changelog/waitpoints
13. Cloudflare, Workflows GA changelog, 2025-04-07 [V]. https://developers.cloudflare.com/changelog/post/2025-04-07-workflows-ga/
14. Tian Pan, "The Idempotency Problem in Agentic Tool Calling", 2026-04-19. https://tianpan.co/blog/2026/04/19/idempotency-agentic-tool-calling-saga-deduplication
15. A2A Protocol Specification v1.0.0, accessed 2026-10-19. https://a2a-protocol.org/latest/specification/
16. MCP blog, "The 2026-07-28 Specification", 2026-07-28. https://blog.modelcontextprotocol.io/posts/2026-07-28/
17. John Hodge, "The state of the OpenTelemetry GenAI semantic conventions (July 2026)", 2026-07-17. https://john-hodge.com/blog/opentelemetry-genai-semantic-conventions/
18. diginomica, "Confluent tackles enterprise AI agent complexity with Streaming Agents platform", 2025-08-21 (publisher partners with Confluent). https://diginomica.com/confluent-tackles-enterprise-ai-agent-complexity-streaming-agents-platform
19. Redpanda, "Introducing the Agentic Data Plane", 2025-10-28 [V]. https://www.redpanda.com/blog/agentic-data-plane ; competitor view: AutoMQ [V], https://www.automq.com/blog/redpanda-agentic-data-plane-kafka-strategy
20. LangChain docs, LangGraph time travel and checkpointers, accessed 2026-10-19 [V]. https://docs.langchain.com/oss/python/langgraph/use-time-travel
21. DEV Community, "Your AI agent needs a brake pedal", 2026-10-12 (year inferred from page) [V]. https://dev.to/vijaym2k6/your-ai-agent-needs-a-brake-pedal-stopping-loops-and-runaway-cost-at-runtime-4jc4
22. MCP stateless change, Google Developers Blog [V]. https://developers.googleblog.com/scaling-ai-agent-infrastructure-with-the-mcp-stateless-updates/
23. DEV Community (AWS Community Builders), "Orchestrating Our Agents SDLC on Temporal: The Hard Parts Nobody Warns You About", Jul 17 (year not shown). https://dev.to/aws-builders/orchestrating-our-agents-sdlc-on-temporal-the-hard-parts-nobody-warns-you-about-1mgc
24. Solace, Agent Mesh Enterprise GA press release, 2025-12-02 [V]. https://www.aap.com.au/aapreleases/cision20251202ae35325
25. Workflow Builder, "Reliability is the new model selection", 2026-06-22 [V], quoting HN. https://www.workflowbuilder.io/blog/reliability-is-the-new-model-selection
