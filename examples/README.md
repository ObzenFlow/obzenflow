# ObzenFlow examples

Start with the [website tutorials](https://obzenflow.dev/tutorials/) for guided
walkthroughs. Example names below link to source; local run guides cover setup
and replay. A dash means there is no dedicated tutorial or run guide yet.

| Example (source) | Demonstrates | Tutorial or run guide |
| --- | --- | --- |
| [char_transform](char_transform.rs) | Stateful text transformation | [Getting started](https://obzenflow.dev/tutorials/getting-started/) |
| [char_transform_skeleton](char_transform_skeleton.rs) | Flow topology with placeholder handlers | [Getting started](https://obzenflow.dev/tutorials/getting-started/) |
| [http_ingestion_piggy_bank_demo](http_ingestion_piggy_bank_demo/flow.rs) | HTTP ingress, joins, and a checkbook projection | [Bank transactions](https://obzenflow.dev/tutorials/model-bank-transactions/) |
| [hn_ai_digest_demo](hn_ai_digest_demo/flow.rs) | HTTP input, token budgeting, and AI summarisation | [Live AI inference](https://obzenflow.dev/tutorials/live-ai-inference/) |
| [one_shot_inference_demo](one_shot_inference_demo/main.rs) | One bounded input and one model decision | — |
| [payment_gateway_resilience](payment_gateway_resilience/flow.rs) | Gateway effects with retries, circuit breaking, and rate limiting | [Run guide](payment_gateway_resilience/README.md) |
| [flash_sale_allocation](flash_sale_allocation/flow.rs) | Stateful stock reservation and cancellation | [Run guide](flash_sale_allocation/README.md) |
| [postgres_sink_payments](postgres_sink_payments/flow.rs) | Typed payments delivered through a PostgreSQL UPSERT | [Run guide](postgres_sink_payments/README.md) |
| [prometheus_demo](prometheus_demo/main.rs) | Metrics reporting, circuit breaking, and backpressure | [Run guide](prometheus_demo/README.md) |
| [product_catalog_enrichment](product_catalog_enrichment/flow.rs) | Multi-way inner, left, and strict joins | — |
| [flight_delays_simple](flight_delays_simple/flow.rs) | Stream-table reference enrichment | — |
| [csv_demo_support_sla](csv_demo_support_sla/flow.rs) | CSV input, typed joins, and CSV output | — |
| [ecommerce_top_products](ecommerce_top_products.rs) | Ranked aggregation with bounded memory | — |
| [stateful_accumulator_catalog](stateful_accumulator_catalog.rs) | Conflation, custom accumulators, and emission strategies | — |
| [stateful_emit_within_tumbling](stateful_emit_within_tumbling.rs) | Processing-time tumbling windows | — |
| [web_analytics_pipeline](web_analytics_pipeline.rs) | Grouping and reduction with different emission strategies | — |
| [topology_patterns_demo](topology_patterns_demo.rs) | Fan-in and routing for inputs of the same type | — |
| [multi_source_ingest_demo](multi_source_ingest_demo/flow.rs) | Aligning different input types before fan-in | — |
| [flow_middleware_config](flow_middleware_config/main.rs) | Per-source middleware configuration | — |

## Run

Run commands from the repository root. Most examples use:

```sh
cargo run -p obzenflow --example <name>
```

Follow the local run guides above for payments, flash-sale allocation, PostgreSQL,
and metrics. The bank, middleware, and AI examples use these commands:

```sh
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features prometheus,web-host

cargo run -p obzenflow --example flow_middleware_config --features prometheus,web-host

cargo run -p obzenflow --example one_shot_inference_demo --features ai -- \
  --config examples/one_shot_inference_demo/obzenflow.toml

cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- \
  --config examples/hn_ai_digest_demo/obzenflow.toml
```

Both AI configurations require Ollama running with `llama3.1:8b` available.
The digest uses a local mock news endpoint by default; prefix its command with
`HN_LIVE=1` to fetch real Hacker News stories. For PostgreSQL output, select
[the PostgreSQL config](hn_ai_digest_demo/obzenflow.postgres.toml) and supply a
connection as described in the [local PostgreSQL guide](../dev/postgres/README.md).

The bank example also includes an [authenticated control-plane config](http_ingestion_piggy_bank_demo/obzenflow.auth.toml);
see [managed web authentication](../crates/obzenflow_infra/src/web/README.md) for setup.

To replay a recorded run, add `--replay-from <archive> --verify` to the application
arguments, after Cargo's `--` separator. Use the archive path printed by the run
and keep the same configuration.
