# ObzenFlow examples

Every example in this directory is a runnable reference application. They are ordered below as a learning path, so each tier builds on concepts introduced in the previous one. If you prefer to jump straight into a working end-to-end demo, start with the quickstart below and circle back to the structured path afterward.

## Quickstart: HTTP ingestion + joins + stateful (interactive)

This is the most end-to-end demo in the repo: push events over HTTP, enrich with a join, materialise state, and observe the system via `/metrics`.

Run:

```bash
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090
```

Create reference data (accounts):

```bash
curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.account","data":{"account_id":"acct-1","owner":"Alice","initial_balance_cents":1000}}'
curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.account","data":{"account_id":"acct-2","owner":"Bob","initial_balance_cents":0}}'
```

Post stream data (transactions):

```bash
curl -XPOST http://127.0.0.1:9090/api/bank/tx/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.tx","data":{"account_id":"acct-1","delta_cents":250,"note":"paycheck"}}'
curl -XPOST http://127.0.0.1:9090/api/bank/tx/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.tx","data":{"account_id":"acct-1","delta_cents":-99,"note":"coffee"}}'
```

Observe:
- Metrics: `curl http://127.0.0.1:9090/metrics`
- Topology: `curl http://127.0.0.1:9090/api/topology`

Code: `examples/http_ingestion_piggy_bank_demo.rs`

The structured learning path begins below for readers who want to build up concepts incrementally.

---

## How to run examples

Most examples run as:

```bash
cargo run -p obzenflow --example <name>
```

Some examples require feature flags:
- `--features obzenflow_infra/warp-server` for `--server` mode and HTTP endpoints.
- `--features http-pull` for HTTP pull sources (enables the default reqwest-based client).

---

## Learning path

### Tier 1: Your first pipeline

These examples introduce the core building blocks of an ObzenFlow pipeline. You will learn how to declare a flow with `flow!`, wire up typed sources and sinks, and apply simple transforms. By the end of this tier you should be comfortable reading and writing a minimal source-to-transform-to-sink pipeline.

- **`char_transform`** — A minimal typed pipeline that takes characters, maps them through a transform, and reduces the results. You will learn the `flow!`, `source!`, `transform!`, and `sink!` macros alongside `MapTyped` and `ReduceTyped`.
  Run: `cargo run -p obzenflow --example char_transform`
  Code: `examples/char_transform.rs`

- **`typed_source_demo`** — Walks through every way to create a typed source, including `from_iter`, `from_item_fn`, fallible sources, and async sources. You will learn how `FiniteSourceTyped` and `AsyncFiniteSourceTyped` work and when to pick each.
  Run: `cargo run -p obzenflow --example typed_source_demo`
  Code: `examples/typed_source_demo.rs`

- **`typed_sink_demo`** — Shows how typed sinks enforce strict type checking at the boundary of a pipeline and how mismatched types are gracefully skipped. You will learn `SinkTyped`, fallible sinks, and the strict-by-default contract.
  Run: `cargo run -p obzenflow --example typed_sink_demo`
  Code: `examples/typed_sink_demo.rs`

> **Sidebar: infinite sources and graceful shutdown.**
> The examples above all use finite sources that terminate naturally. Real pipelines often run indefinitely. `typed_infinite_source_demo` shows how to create a long-running source and shut it down cleanly with ObzenFlow's graceful shutdown mechanism.
> Run: `cargo run -p obzenflow --example typed_infinite_source_demo`
> Code: `examples/typed_infinite_source_demo.rs`

### Tier 2: Stateful processing

Once you can build a basic pipeline, the next step is managing state. These examples introduce accumulators, emission strategies, and bounded-memory patterns. You will learn how ObzenFlow stages can accumulate values across events and choose when and how to emit results.

- **`stateful_counter_demo`** — The lowest-level look at stateful processing. You will learn the `StatefulHandler` trait directly, including its `accumulate`, `initial_state`, and `drain` methods.
  Run: `cargo run -p obzenflow --example stateful_counter_demo`
  Code: `examples/stateful_counter_demo.rs`

- **`web_analytics_pipeline`** — Applies typed accumulators (`GroupByTyped`, `ReduceTyped`) to a realistic web analytics scenario with multiple emission strategies and a fan-out topology. You will learn how to group, reduce, and emit results under different policies.
  Run: `cargo run -p obzenflow --example web_analytics_pipeline`
  Code: `examples/web_analytics_pipeline.rs`

- **`top_n_leaderboard`** — A bounded-memory accumulator that keeps only the top N items. You will learn `TopNTyped` and see how ObzenFlow can cap memory usage while still producing correct results.
  Run: `cargo run -p obzenflow --example top_n_leaderboard`
  Code: `examples/top_n_leaderboard.rs`

- **`ecommerce_top_products`** — Builds on the Top-N concept with `TopNByTyped` to rank products by aggregated scores. Also introduces rate-limit middleware at the stage level.
  Run: `cargo run -p obzenflow --example ecommerce_top_products`
  Code: `examples/ecommerce_top_products.rs`

### Tier 3: Joins and topology

Stream-table joins let you enrich events with reference data, and topology patterns let you split and merge data flows. These examples cover inner joins, left joins, strict joins, chained multi-way joins, and fan-in/fan-out/diamond topologies.

- **`flight_delays_simple`** — Your first join. Enriches a stream of flight delay events with carrier reference data using `InnerJoinBuilder`. You will learn how stream-table joins work and how the reference side is populated.
  Run: `cargo run -p obzenflow --example flight_delays_simple`
  Code: `examples/flight_delays_simple.rs`

- **`csv_demo_support_sla`** — Joins and transforms on offline CSV fixtures, writing results to a CSV sink. You will learn how to wire up file-based sources and sinks for batch-style processing.
  Run: `cargo run -p obzenflow --example csv_demo_support_sla`
  Code: `examples/csv_demo_support_sla/flow.rs`

- **`topology_patterns_demo`** — Demonstrates fan-in, fan-out, and diamond topologies with independent journal readers. You will learn how to split a stream to multiple consumers and merge multiple streams into one.
  Run: `cargo run -p obzenflow --example topology_patterns_demo`
  Code: `examples/topology_patterns_demo.rs`

- **`product_catalog_enrichment`** — The most complex join example. Chains inner, left, and strict joins across multiple reference catalogs to produce a fully enriched product view. You will learn multi-way join composition and the differences between join modes.
  Run: `cargo run -p obzenflow --example product_catalog_enrichment`
  Code: `examples/product_catalog_enrichment/flow.rs`

### Tier 4: Middleware, metrics, and resilience

Production pipelines need rate limiting, metrics, and fault tolerance. These examples show how to layer operational concerns onto a pipeline without changing its core logic.

- **`flow_middleware_config`** — Shows how middleware is inherited from the flow level and overridden at the stage level. You will learn the middleware configuration hierarchy using rate limiting as the running example.
  Run: `cargo run -p obzenflow --example flow_middleware_config --features obzenflow_infra/warp-server -- --server`
  Code: `examples/flow_middleware_config.rs`

- **`prometheus_1k_demo`** — Integrates Prometheus `/metrics` into a pipeline with error routing and typed counting. You will learn how to expose and observe pipeline metrics.
  Run: `cargo run -p obzenflow --example prometheus_1k_demo --features obzenflow_infra/warp-server -- --server`
  Code: `examples/prometheus_1k_demo.rs`

- **`prometheus_100k_demo`** — A higher-volume variant intended for scraping and graphing. You will learn how rate limiting and `run_blocking` interact with metrics under sustained load.
  Run: `cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server`
  Code: `examples/prometheus_100k_demo.rs`

- **`payment_gateway_resilience`** — Wraps a payment processing pipeline with a circuit breaker and typed fallback. You will learn the difference between strict contracts and breaker-aware contracts, and how ObzenFlow's error taxonomy routes failures.
  Run: `cargo run -p obzenflow --example payment_gateway_resilience`
  Optional (live `/metrics`): `cargo run -p obzenflow --example payment_gateway_resilience --features obzenflow_infra/warp-server -- --server`
  Code: `examples/payment_gateway_resilience/flow.rs`

### Tier 5: External integrations

These examples connect ObzenFlow to the outside world through HTTP push ingestion, HTTP pull/polling, and LLM-backed transforms.

- **`http_ingestion_piggy_bank_demo`** — HTTP push ingestion with live joins, stateful snapshots, and `/metrics`. This is the same demo covered in the quickstart above. You will learn how to accept events over HTTP and wire them into a typed pipeline.
  Run: `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
  Code: `examples/http_ingestion_piggy_bank_demo.rs`

- **`hn_ingestion_demo`** — Polls the Hacker News API on an interval, decodes and transforms stories, and prints them to the console. Defaults to a local mock server so you can run it offline. You will learn how HTTP pull sources work and how to test them without network access.
  Run: `cargo run -p obzenflow --example hn_ingestion_demo --features http-pull`
  Optional (real HN): `HN_LIVE=1 cargo run -p obzenflow --example hn_ingestion_demo --features http-pull`
  Code: `examples/hn_ingestion_demo/flow.rs`

- **`hn_ai_digest_demo`** — The most advanced example in the repo. Pulls stories from HN, groups and budgets them by token count, and feeds them to an LLM (via Rig) to produce a markdown digest. You will learn how to integrate AI/LLM transforms into a typed pipeline.
  Run (mock HN + Ollama): `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai-rig"`
  Optional (real HN): `HN_LIVE=1 cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai-rig"`
  Optional (OpenAI): `HN_AI_PROVIDER=openai OPENAI_API_KEY=... cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai-rig"`
  Ollama setup (macOS): `brew install ollama`, then start the server with `ollama serve` (or open the Ollama desktop app), then `ollama pull llama3.1:8b`
  Requirements: Ollama running locally (default `http://localhost:11434`) or a configured OpenAI/OpenAI-compatible endpoint. This example preflights the provider at startup and fails fast if it cannot connect or the model is not available.
  Third-party terms note: ObzenFlow only provides a client-side integration (via `rig-core`). It does not redistribute Ollama, model weights, or hosted LLM services. You are responsible for complying with any third-party licences/terms (including model weight licences and hosted-provider ToS/usage limits). When using a hosted provider, your prompts and story text will be sent to that provider.
  Useful env vars: `HN_AI_PROVIDER=ollama|openai`, `HN_AI_MODEL=...`, `HN_AI_INTERESTS="rust, ai, security"`, `HN_AI_GROUP_BUDGET_TOKENS=...`, `HN_AI_GROUP_MAX_STORIES=...`, `OLLAMA_BASE_URL=...`, `OPENAI_BASE_URL=...`
  Code: `examples/hn_ai_digest_demo/flow.rs`

---

## Deprecated aliases

- **`flight_delays`** — Deprecated alias for `flight_delays_simple`. Use `flight_delays_simple` instead.
