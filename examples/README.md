# ObzenFlow examples

Start with the [website tutorials](https://obzenflow.dev/tutorials/) for guided
walkthroughs. Example names below link to source; local run guides cover setup
and replay. A dash means there is no dedicated tutorial or run guide yet.

| Example (source) | Demonstrates | Tutorial or run guide |
| --- | --- | --- |
| [char_transform](char_transform.rs) | Stateful text transformation | [Getting started](https://obzenflow.dev/tutorials/getting-started/) |
| [char_transform_skeleton](char_transform_skeleton.rs) | Flow topology with placeholder handlers | [Getting started](https://obzenflow.dev/tutorials/getting-started/) |
| [http_ingestion_piggy_bank_demo](http_ingestion_piggy_bank_demo/flow.rs) | HTTP ingress, ACH retries, fee rules, and a replay through a corrected rule | [Bank transactions](https://obzenflow.dev/tutorials/model-bank-transactions/), [run guide](http_ingestion_piggy_bank_demo/README.md) |
| [hn_ai_digest_demo](hn_ai_digest_demo/flow.rs) | HTTP input, AI summarization, and a resumed live run | [Live AI inference](https://obzenflow.dev/tutorials/live-ai-inference/), [run guide](hn_ai_digest_demo/README.md) |
| [one_shot_inference_demo](one_shot_inference_demo/main.rs) | One bounded input and one model decision | — |
| [payment_gateway_resilience](payment_gateway_resilience/flow.rs) | Gateway effects with retries, circuit breaking, and rate limiting | [Run guide](payment_gateway_resilience/README.md) |
| [flash_sale_allocation](flash_sale_allocation/flow.rs) | Stateful stock reservation and cancellation | [Run guide](flash_sale_allocation/README.md) |
| [aml_overnight_monitoring](aml_overnight_monitoring/flow.rs) | A recorded risk verdict per transaction, cases, and a resumed batch | [Run guide](aml_overnight_monitoring/README.md) |
| [prometheus_demo](prometheus_demo/main.rs) | Metrics reporting, circuit breaking, and backpressure | [Run guide](prometheus_demo/README.md) |
| [product_catalog_enrichment](product_catalog_enrichment/flow.rs) | Multi-way inner, left, and strict joins | — |
| [flight_delays_simple](flight_delays_simple/flow.rs) | Stream-table reference enrichment | — |
| [csv_demo_support_sla](csv_demo_support_sla/flow.rs) | CSV input, typed joins, and CSV output | — |
| [ecommerce_top_products](ecommerce_top_products.rs) | Ranked aggregation with bounded memory | — |

## Run

Run commands from the repository root. Most examples use:

```sh
cargo run -p obzenflow --example <name>
```

Examples with a run guide list their own commands. The one-shot inference
example needs Ollama running with `llama3.1:8b`:

```sh
cargo run -p obzenflow --example one_shot_inference_demo --features ai -- \
  --config examples/one_shot_inference_demo/obzenflow.toml
```

The bank example also includes an
[authenticated control-plane config](http_ingestion_piggy_bank_demo/obzenflow.auth.toml);
see [managed web authentication](../crates/obzenflow_infra/src/web/README.md) for setup.

To replay a recorded run, add `--replay-from <archive> --verify` to the application
arguments, after Cargo's `--` separator. Use the archive path printed by the run
and keep the same configuration, unless a run guide names another.
