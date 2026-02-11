# ObzenFlow examples

The `examples/` directory contains runnable “reference applications” for ObzenFlow. Most examples are meant to be read like small apps: domain types + a `flow! { ... }` definition + a `FlowApplication` runner.

## Quickstart: HTTP ingestion + joins + stateful (interactive)

This is the most end-to-end demo in the repo: push events over HTTP, enrich with a join, materialize state, and observe the system via `/metrics`.

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

## How to run examples

Most examples run as:

```bash
cargo run -p obzenflow --example <name>
```

Some examples require features:
- `--features obzenflow_infra/warp-server` for `--server` mode and HTTP endpoints.
- `--features http-pull` for HTTP pull sources (enables the default reqwest-based client).

## Curated demos (recommended order)

### Reference applications

- **`http_ingestion_piggy_bank_demo`**: HTTP ingestion (push) + join + stateful snapshots + `/metrics`.  
  Run: `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`  
  Code: `examples/http_ingestion_piggy_bank_demo.rs`

- **`product_catalog_enrichment`**: reference catalogs + chained joins + stateful summary (offline, fixtures).  
  Run: `cargo run -p obzenflow --example product_catalog_enrichment`  
  Code: `examples/product_catalog_enrichment/flow.rs`

- **`payment_gateway_resilience`**: circuit breaker + typed fallback + error taxonomy + “strict vs breaker-aware” behavior.  
  Run: `cargo run -p obzenflow --example payment_gateway_resilience`  
  Optional (live `/metrics`): `cargo run -p obzenflow --example payment_gateway_resilience --features obzenflow_infra/warp-server -- --server`  
  Code: `examples/payment_gateway_resilience/flow.rs`

- **`csv_demo_support_sla`**: join + transform on offline CSV fixtures, writes a CSV sink.  
  Run: `cargo run -p obzenflow --example csv_demo_support_sla`  
  Code: `examples/csv_demo_support_sla/flow.rs`

- **`hn_ingestion_demo`**: HTTP pull (poll) source + decode/transform + console sink (defaults to a local mock server).  
  Run: `cargo run -p obzenflow --example hn_ingestion_demo --features http-pull`  
  Optional (real HN): `HN_LIVE=1 cargo run -p obzenflow --example hn_ingestion_demo --features http-pull`  
  Code: `examples/hn_ingestion_demo/flow.rs`

- **`hn_ai_digest_demo`**: HN HTTP pull + typed accumulation + Rig-backed `ChatTransform` to generate a markdown digest (defaults to mock HN + Ollama).  
  Run (mock HN + Ollama): `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai-rig"`  
  Optional (real HN): `HN_LIVE=1 cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai-rig"`  
  Optional (OpenAI): `HN_AI_PROVIDER=openai OPENAI_API_KEY=... cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai-rig"`  
  Ollama setup (macOS): `brew install ollama`, then start the server with `ollama serve` (or open the Ollama desktop app), then `ollama pull llama3.1:8b`  
  Requirements: Ollama running locally (default `http://localhost:11434`) *or* a configured OpenAI/OpenAI-compatible endpoint. This example preflights the provider at startup and fails fast if it can’t connect or the model isn’t available.  
  Third-party terms note: ObzenFlow only provides a client-side integration (via `rig-core`). It does not redistribute Ollama, model weights, or hosted LLM services. You are responsible for complying with any third-party licenses/terms (including model weight licenses and hosted-provider ToS/usage limits). When using a hosted provider, your prompts and story text will be sent to that provider.  
  Useful env vars: `HN_AI_PROVIDER=ollama|openai`, `HN_AI_MODEL=...`, `HN_AI_INTERESTS="rust, ai, security"`, `OLLAMA_BASE_URL=...`, `OPENAI_BASE_URL=...`  
  Code: `examples/hn_ai_digest_demo/flow.rs`

### Middleware, monitoring, and topology patterns

- **`flow_middleware_config`**: flow-level vs stage-level middleware inheritance/override (rate limiting).  
  Run (observe `/metrics` while it runs): `cargo run -p obzenflow --example flow_middleware_config --features obzenflow_infra/warp-server -- --server`  
  Code: `examples/flow_middleware_config.rs`

- **`prometheus_1k_demo`**: Prometheus `/metrics` + fan-out + typed accumulation.  
  Run: `cargo run -p obzenflow --example prometheus_1k_demo --features obzenflow_infra/warp-server -- --server`  
  Code: `examples/prometheus_1k_demo.rs`

- **`prometheus_100k_demo`**: higher-volume Prometheus demo intended for scraping/graphs.  
  Run: `cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server`  
  Code: `examples/prometheus_100k_demo.rs`

- **`topology_patterns_demo`**: fan-in, fan-out, and diamond patterns with independent journal readers.  
  Run: `cargo run -p obzenflow --example topology_patterns_demo`  
  Code: `examples/topology_patterns_demo.rs`

### Typed helpers and building blocks

- **`typed_source_demo`**: typed finite + async sources (`FiniteSourceTyped`, `AsyncFiniteSourceTyped`).  
  Run: `cargo run -p obzenflow --example typed_source_demo`  
  Code: `examples/typed_source_demo.rs`

- **`typed_infinite_source_demo`**: typed infinite sources + graceful shutdown.  
  Run: `cargo run -p obzenflow --example typed_infinite_source_demo`  
  Code: `examples/typed_infinite_source_demo.rs`

- **`typed_sink_demo`**: typed sinks (`SinkTyped`, fallible sinks, strict-by-default type checking).  
  Run: `cargo run -p obzenflow --example typed_sink_demo`  
  Code: `examples/typed_sink_demo.rs`

- **`web_analytics_pipeline`**: typed stateful accumulators (`GroupByTyped`, `ReduceTyped`) across multiple emission strategies.  
  Run: `cargo run -p obzenflow --example web_analytics_pipeline`  
  Code: `examples/web_analytics_pipeline.rs`

- **`flight_delays_simple`**: typed join enrichment on a small “reference + stream” scenario.  
  Run: `cargo run -p obzenflow --example flight_delays_simple`  
  Code: `examples/flight_delays_simple.rs`

- **`char_transform`**: tiny “pure transform + typed reducer” pipeline.  
  Run: `cargo run -p obzenflow --example char_transform`  
  Code: `examples/char_transform.rs`

- **`stateful_counter_demo`**: `StatefulHandler` mechanics (accumulate/emit/drain).  
  Run: `cargo run -p obzenflow --example stateful_counter_demo`  
  Code: `examples/stateful_counter_demo.rs`

- **`top_n_leaderboard`** and **`ecommerce_top_products`**: bounded-memory Top-N stateful accumulators.  
  Run: `cargo run -p obzenflow --example top_n_leaderboard` / `cargo run -p obzenflow --example ecommerce_top_products`  
  Code: `examples/top_n_leaderboard.rs` / `examples/ecommerce_top_products.rs`
