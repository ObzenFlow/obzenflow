# ObzenFlow

ObzenFlow is a durable execution framework written in Rust intended for high-consequence systems. A high-consequence system must process data reliably and account precisely for how each result was produced. 

ObzenFlow combines the developer ergonomics of typed, graph-oriented stream processing, with durable event journals that drive execution forward. It does this without coupling your application to third-party infrastructure or databases. 

How does ObzenFlow provide durability without coupling? 

Each stage in a flow maintains an append-only output journal. Each output journal contains event-sourced facts that serve as the input tape for downstream stages. Together, journals compose to form a graph of durable processing, which preserves the history needed to reconstruct a flow's execution. Journals are pluggable to avoid coupling. We ship with disk-based journals out of the box, plus memory-based journals for testing, and more journal backend adapters are coming soon. 

ObzenFlow's superpowers: 

- Process data through typed transforms, joins, and stateful stages using an ergonomic syntax
- Ships with advanced stage types for inline inference (Ollama ships now, other providers are coming soon) 
- Replay and verify completed flows
- Trace results back through their recorded history for auditability and reconstruction
- Resume interrupted work from a last durable frontier
- Built-in resilience patterns like circuit breakers and rate limiters 
- Separates effectful operations from deterministic logic for integrating durably with external services

ObzenFlow ships as a single binary with built-in disk back journals. (Storage is pluggable so you're free to build your own journal adapter, and other journal backends are coming soon). Out of the box there's no separate platform, broker cluster, or database to operate. 

Status: **pre-1.0**. APIs are still evolving and may change between releases.

## Run and replay a flow

From a clone of this repository, run the payment gateway example. It authorizes orders through a simulated unreliable gateway, with a declared effect and a circuit breaker:

```bash
cargo run -p obzenflow --example payment_gateway_resilience
```

The completion footer prints the archive path and a replay command. Copy that command and add `--verify`, or replace `<run_id>` below with the recorded run's ID:

```bash
cargo run -p obzenflow --example payment_gateway_resilience -- \
    --replay-from target/payment-gateway-logs/flows/<run_id> --verify
```

Replay uses the archived inputs and committed effect outcomes without calling the gateway again. A matching replay prints `output matched the original run, 0 differences`.

These commands use repository examples, which are not included in the published crate. ObzenFlow is a library; replay and verification run through your application's executable.

## Build an application

Applications depend on `obzenflow` and import capabilities from its public modules. Serde, Tokio, and other libraries used by your code remain explicit dependencies.

A flow defines its stages, journals, and connections, then runs through `FlowApplication`. In this sketch, the event types and `build_*` functions belong to your application:

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

Construct handlers inside `FlowDefinition::materialize`; stage declarations refer to those local bindings. The [character transformation example](https://github.com/obzenflow/obzenflow/blob/main/examples/char_transform.rs) has complete domain types and handlers you can run.

Use the prelude for common names and import specialised capabilities as needed:

| Module | Application use |
| --- | --- |
| `prelude` | Common flow macros, `FlowDefinition`, `FlowApplication`, handler errors, and schema traits/derives |
| `schema` | Typed payloads and output/effect fact carriers |
| `flow` | Flow definitions, stage declarations, backpressure settings, and construction errors |
| `stages::{sources, transforms, stateful, joins, sinks}` | Built-in constructors and custom handler contracts, grouped by stage family |
| `effects` | External operations and replay-safe bindings |
| `middleware` | Live-I/O policies and passive observers |
| `application` | Configuration, execution, replay verification, and `application::ingress` |
| `journal` | Journal construction, inspection, and export |
| `ai`, `env`, `error` | AI contracts, typed environment parsing, and shared handler errors |

No features are enabled by default. The [examples catalog](https://github.com/obzenflow/obzenflow/blob/main/examples/README.md) lists runnable flows and the feature flags and configuration they need, including HTTP services, AI inference, PostgreSQL, and metrics.

## Documentation

- [What is ObzenFlow?](https://obzenflow.dev/product/what-is-obzenflow/) describes the guarantees and intended uses.
- [How ObzenFlow Works](https://obzenflow.dev/product/how-obzenflow-works/) explains flow declarations, effects, journals, and run modes.
- [Tutorials](https://obzenflow.dev/tutorials/) walk through building flows, modelling bank transactions, and running live AI inference.
- [Philosophy](https://obzenflow.dev/philosophy/) explains the design principles.

## License

Dual-licensed under [MIT](https://github.com/obzenflow/obzenflow/blob/main/LICENSE-MIT) OR [Apache-2.0](https://github.com/obzenflow/obzenflow/blob/main/LICENSE-APACHE).
