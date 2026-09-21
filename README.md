# ObzenFlow

ObzenFlow is a durable execution framework written in Rust for building high-consequence systems. It combines the developer ergonomics of typed, graph-oriented stream processing, with durable event journals that drive execution forward, and does this without coupling your application to third-party infrastructure or databases. 

ObzenFlow's superpowers: 

- Process data through typed transforms, joins, and stateful stages using an ergonomic syntax
- Ingest live streaming data and batch data in the same flow
- Ships with advanced stage types for inline inference 
- Separates effectful operations from deterministic logic for replay-safe integrations 
- Replay and verify completed flows
- Trace results through recorded history for auditability and reconstruction
- Resume interrupted work from a last durable frontier
- Built-in resilience patterns like circuit breakers and rate limiters 
- Optionally serve metrics via Prometheus right out of the box 

ObzenFlow ships as a single binary. Out of the box there's no separate platform, broker cluster, or database to operate. 

Status: **pre-1.0**. APIs are still evolving and may change between releases.

## How does ObzenFlow provide durability without coupling? 

Each stage in a flow maintains an append-only output journal. Each output journal contains event-sourced facts that serve as the input tape for downstream stages. Together, journals compose to form a graph of durable processing, which preserves the history needed to reconstruct a flow's execution. 

ObzenFlow ships with disk-backed journals, plus in-memory journals for testing. Journal backends are pluggable, so you can change how journals are stored without changing processing logic. More journal backends (including object storage) are coming soon.

## Build an application

A flow defines its stages, journals, and connections. Running a flow is simple: hand the blueprint to `FlowApplication`, which materializes the blueprint and sets up metrics, endpoints, and other operational integrations, and then runs the flow. 

```rust,ignore
use obzenflow::prelude::*;
use obzenflow::journal::disk_journals;
use obzenflow::middleware::rate_limit;

fn build_flow() -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let my_source = build_source();
        let my_transform = build_transform();
        let my_sink = build_sink();

        Ok(flow! {
            name: "my_flow",
            journals: disk_journals("target/logs".into()),

            stages: {
                input = source!(InputEvent => my_source with [rate_limit(100.0)]);
                enrich = transform!(InputEvent -> OutputEvent => my_transform);
                output = sink!(OutputEvent => my_sink);
            },

            topology: {
                input |> enrich;
                enrich |> output;
            }
        })
    })
}

FlowApplication::run(build_flow()).await?;
```

No features are enabled by default. The [examples catalog](https://github.com/obzenflow/obzenflow/blob/main/examples/README.md) lists runnable flows and the feature flags and configuration they need, including HTTP services, AI inference, PostgreSQL, and metrics.

## Run and replay a flow

Clone the repository, then run the payment gateway example. It authorizes orders through a simulated unreliable gateway, with a declared effect and a circuit breaker:

```bash
cargo run -p obzenflow --example payment_gateway_resilience
```

The completion footer prints the archive path and a replay command. Copy that command and add `--verify`, or replace `<run_id>` below with the recorded run's ID:

```bash
cargo run -p obzenflow --example payment_gateway_resilience -- \
    --replay-from target/payment-gateway-logs/flows/<run_id> --verify
```

Replay uses the archived inputs and committed effect outcomes without calling the gateway again. A matching replay prints `output matched the original run, 0 differences`.

## Documentation

- [What is ObzenFlow?](https://obzenflow.dev/product/what-is-obzenflow/) describes the guarantees and intended uses.
- [How ObzenFlow Works](https://obzenflow.dev/product/how-obzenflow-works/) explains flow declarations, effects, journals, and run modes.
- [Tutorials](https://obzenflow.dev/tutorials/) walk through building flows, modelling bank transactions, and running live AI inference.
- [Philosophy](https://obzenflow.dev/philosophy/) explains the design principles.

## License

Dual-licensed under [MIT](https://github.com/obzenflow/obzenflow/blob/main/LICENSE-MIT) OR [Apache-2.0](https://github.com/obzenflow/obzenflow/blob/main/LICENSE-APACHE).
