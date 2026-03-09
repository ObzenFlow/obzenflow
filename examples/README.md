# ObzenFlow examples

This directory is a runnable example catalog.

If you want a guided walkthrough, start with the tutorials first:

- [Getting Started](https://obzenflow.dev/tutorials/getting-started/)
- [Model Bank Transactions as a Flow](https://obzenflow.dev/tutorials/model-bank-transactions/)
- [Run Live AI Inference from a Real Endpoint](https://obzenflow.dev/tutorials/live-ai-inference/)

The tutorials are the teaching surface. This README is the map of runnable examples once you want to explore beyond those walkthroughs.

## How to run examples

Most examples run as:

```bash
cargo run -p obzenflow --example <name>
```

Some examples require feature flags:

- `--features obzenflow_infra/warp-server` for `--server` mode and HTTP endpoints
- `--features http-pull` for HTTP pull sources
- `--features "http-pull ai"` for the AI digest example

## Start here

- **`char_transform`** — The smallest ObzenFlow example. It shows `FlowApplication`, `flow!`, typed stages, and reduction without extra infrastructure.
  Tutorial: [Getting Started](https://obzenflow.dev/tutorials/getting-started/)
  Run: `cargo run -p obzenflow --example char_transform`
  Code: `examples/char_transform.rs`

- **`http_ingestion_piggy_bank_demo`** — The canonical end-to-end service example: HTTP ingress, joins, stateful projection, and `/metrics` in one journal-backed flow.
  Tutorial: [Model Bank Transactions as a Flow](https://obzenflow.dev/tutorials/model-bank-transactions/)
  Run: `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
  Code: `examples/http_ingestion_piggy_bank_demo.rs`

- **`hn_ai_digest_demo`** — The canonical AI example: live HTTP pull, token budgeting, chunking, accumulation, and Rig-backed LLM inference with replayable evidence.
  Tutorial: [Run Live AI Inference from a Real Endpoint](https://obzenflow.dev/tutorials/live-ai-inference/)
  Run: `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai"`
  Code: `examples/hn_ai_digest_demo/flow.rs`

## Problem-focused examples

- **`product_catalog_enrichment`** — Multi-way enrichment across inner, left, and strict joins. Use this when you want the most realistic catalog-style dimension pipeline in the repo.
  Run: `cargo run -p obzenflow --example product_catalog_enrichment`
  Code: `examples/product_catalog_enrichment/flow.rs`

- **`csv_demo_support_sla`** — Offline CSV batch processing with typed joins, transforms, and CSV sink output. Good for ETL-style jobs that still need typed flows and replayable execution.
  Run: `cargo run -p obzenflow --example csv_demo_support_sla`
  Code: `examples/csv_demo_support_sla/flow.rs`

- **`payment_gateway_resilience`** — Circuit breakers, fallback behavior, and operator-facing resilience against unreliable dependencies. Use this when you care about runtime protections and failure semantics.
  Run: `cargo run -p obzenflow --example payment_gateway_resilience`
  Optional metrics mode: `cargo run -p obzenflow --example payment_gateway_resilience --features obzenflow_infra/warp-server -- --server`
  Code: `examples/payment_gateway_resilience/flow.rs`

- **`ecommerce_top_products`** — Bounded-memory ranked aggregation over event streams with stage-level rate limiting. Use this for a realistic Top-N-by-score pattern.
  Run: `cargo run -p obzenflow --example ecommerce_top_products`
  Code: `examples/ecommerce_top_products.rs`

- **`flight_delays_simple`** — The shortest path to understanding stream-table joins. Use this when you want one clean reference-enrichment example before moving on to larger join graphs.
  Run: `cargo run -p obzenflow --example flight_delays_simple`
  Code: `examples/flight_delays_simple.rs`

- **`top_n_leaderboard`** — Pure Top-N stateful accumulation with bounded memory and simple output. Use this when you want to understand ranked state without the extra business logic in the e-commerce example.
  Run: `cargo run -p obzenflow --example top_n_leaderboard`
  Code: `examples/top_n_leaderboard.rs`

## Reference shelf

- **`typed_source_demo`** — Typed source constructors and source-shape tradeoffs
- **`typed_sink_demo`** — Typed sink boundaries and mismatch behavior
- **`typed_infinite_source_demo`** — Long-running typed sources and graceful shutdown
- **`stateful_counter_demo`** — Lowest-level `StatefulHandler` example
- **`web_analytics_pipeline`** — Group/reduce stateful patterns with multiple emission strategies
- **`topology_patterns_demo`** — Fan-in, fan-out, and diamond topologies
- **`flow_middleware_config`** — Flow-level versus stage-level middleware inheritance
- **`prometheus_1k_demo`** — Smaller Prometheus `/metrics` demo
- **`prometheus_100k_demo`** — Higher-volume Prometheus variant
- **`hn_ingestion_demo`** — HTTP pull without the AI layer

## Deprecated aliases

- **`flight_delays`** — Deprecated alias for `flight_delays_simple`. Use `flight_delays_simple` instead.
