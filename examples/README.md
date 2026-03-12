# ObzenFlow examples

This directory is a runnable example catalog. Each example links to the [pillars](https://obzenflow.dev/pillars/) it demonstrates so you can navigate by capability.

## How to run examples

Most examples run as:

```bash
cargo run -p obzenflow --example <name>
```

Some examples require feature flags:

- `--features obzenflow_infra/warp-server` for `--server` mode and HTTP endpoints
- `--features http-pull` for HTTP pull sources
- `--features "http-pull ai"` for the AI digest example

## Canonical examples

These are the flagship examples and the best place to start. Each one has a companion [tutorial](https://obzenflow.dev/tutorials/) that walks through how to leverage key features of the framework in detail. The listings below link to both the tutorial and the [pillars](https://obzenflow.dev/pillars/) each example exercises so you can see which framework capabilities are in play.

- **`char_transform`** — The smallest ObzenFlow example. Despite its size, it is a proper stateful flow with accumulation, ordering, and typed reduction across stages.
  - Tutorial: [Getting Started](https://obzenflow.dev/tutorials/getting-started/)
  - Pillars: [Beautifully Expressive](https://obzenflow.dev/pillars/dsl/), [Composable Stage Primitives](https://obzenflow.dev/pillars/composable-stages/)
  - Run: `cargo run -p obzenflow --example char_transform`
  - Code: [`examples/char_transform.rs`](char_transform.rs)

- **`http_ingestion_piggy_bank_demo`** — The canonical end-to-end service example: HTTP ingress, joins, stateful projection, and `/metrics` in one journal-backed flow.
  - Tutorial: [Model Bank Transactions as a Flow](https://obzenflow.dev/tutorials/model-bank-transactions/)
  - Pillars: [Service-Like Streaming](https://obzenflow.dev/pillars/service-like-streaming/), [Composable Stage Primitives](https://obzenflow.dev/pillars/composable-stages/), [Replayability and Durability](https://obzenflow.dev/pillars/replay-auditability/), [Operational Zen](https://obzenflow.dev/pillars/no-platform-required/)
  - Run: `cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090`
  - Code: [`examples/http_ingestion_piggy_bank_demo/flow.rs`](http_ingestion_piggy_bank_demo/flow.rs)

- **`hn_ai_digest_demo`** — The canonical AI example: live HTTP pull, token budgeting, chunking, accumulation, and Rig-backed LLM inference with replayable evidence.
  - Tutorial: [Run Live AI Inference from a Real Endpoint](https://obzenflow.dev/tutorials/live-ai-inference/)
  - Pillars: [AI Execution Substrate](https://obzenflow.dev/pillars/ai-execution-substrate/), [Replayability and Durability](https://obzenflow.dev/pillars/replay-auditability/)
  - Run: `cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai"`
  - Code: [`examples/hn_ai_digest_demo/flow.rs`](hn_ai_digest_demo/flow.rs)

## Problem-focused examples

These examples don't have tutorials, but they demonstrate concrete framework concepts that the canonical examples don't cover in isolation: joins, CSV batch processing, resilience middleware, and ranked aggregation. Each one is a self-contained flow you can read and run without extra context.

- **`product_catalog_enrichment`** — Multi-way enrichment across inner, left, and strict joins. Use this when you want the most realistic catalog-style dimension pipeline in the repo.
  - Pillars: [Composable Stage Primitives](https://obzenflow.dev/pillars/composable-stages/)
  - Run: `cargo run -p obzenflow --example product_catalog_enrichment`
  - Code: [`examples/product_catalog_enrichment/flow.rs`](product_catalog_enrichment/flow.rs)

- **`csv_demo_support_sla`** — Offline CSV batch processing with typed joins, transforms, and CSV sink output. Good for ETL-style jobs that still need typed flows and replayable execution.
  - Pillars: [Composable Stage Primitives](https://obzenflow.dev/pillars/composable-stages/), [Replayability and Durability](https://obzenflow.dev/pillars/replay-auditability/)
  - Run: `cargo run -p obzenflow --example csv_demo_support_sla`
  - Code: [`examples/csv_demo_support_sla/flow.rs`](csv_demo_support_sla/flow.rs)

- **`payment_gateway_resilience`** — Circuit breakers, fallback behavior, and operator-facing resilience against unreliable dependencies. Use this when you care about runtime protections and failure semantics.
  - Pillars: [Production-Grade Primitives](https://obzenflow.dev/pillars/batteries-included/), [Correctness Guarantees](https://obzenflow.dev/pillars/correctness-guarantees/)
  - Run: `cargo run -p obzenflow --example payment_gateway_resilience`
  - Run with metrics: `cargo run -p obzenflow --example payment_gateway_resilience --features obzenflow_infra/warp-server -- --server`
  - Code: [`examples/payment_gateway_resilience/flow.rs`](payment_gateway_resilience/flow.rs)

- **`ecommerce_top_products`** — Bounded-memory ranked aggregation over event streams with stage-level rate limiting. Use this for a realistic Top-N-by-score pattern.
  - Pillars: [Composable Stage Primitives](https://obzenflow.dev/pillars/composable-stages/), [Production-Grade Primitives](https://obzenflow.dev/pillars/batteries-included/)
  - Run: `cargo run -p obzenflow --example ecommerce_top_products`
  - Code: [`examples/ecommerce_top_products.rs`](ecommerce_top_products.rs)

- **`flight_delays_simple`** — The shortest path to understanding stream-table joins. Use this when you want one clean reference-enrichment example before moving on to larger join graphs.
  - Pillars: [Composable Stage Primitives](https://obzenflow.dev/pillars/composable-stages/)
  - Run: `cargo run -p obzenflow --example flight_delays_simple`
  - Code: [`examples/flight_delays_simple/flow.rs`](flight_delays_simple/flow.rs)

## Reference shelf

More niche examples that target specific API surfaces or topology patterns. We use these to validate the developer experience as we add features, and they're useful if you want to explore a particular capability in isolation.

- **`web_analytics_pipeline`** — Group/reduce stateful patterns with multiple emission strategies
- **`topology_patterns_demo`** — Fan-in, fan-out, and diamond topologies
- **`flow_middleware_config`** — Flow-level versus stage-level middleware inheritance
- **`prometheus_100k_demo`** — High-volume Prometheus `/metrics` demo
