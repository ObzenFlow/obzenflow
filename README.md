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

To get a feel for ObzenFlow's vocabulary, [Show Me the Code](https://obzenflow.dev/product/show-me-the-code/) walks through stages, effects, journals, replay, and resume using one payment flow.

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

## Observe and control a running application

The `obzenflow` package includes a CLI behind the `cli` feature. This capability
ships with the next framework release; from this checkout, install it with:

```bash
cargo install --path . --features cli --locked
```

After that release is published, install the same package with
`cargo install obzenflow --features cli --locked`. Library users do not need the
`cli` feature. `obzenflow --version` reports package, archive schema and record
JSONL versions.

Start the application in terminal A with its `web-host` feature enabled:

```bash
cargo run -p obzenflow --features web-host --example payment_gateway_resilience -- \
    --server --startup-mode manual --server-port 9090
```

In terminal B, request Play and observe its committed journals:

```bash
obzenflow start --server http://127.0.0.1:9090 --follow
```

The application prints its PID, run directory, and copyable SIGTERM/SIGKILL
commands. Ctrl-C in terminal A cancels execution. SIGTERM requests graceful
drain; SIGKILL stops abruptly. Ctrl-C in terminal B only detaches the viewer.
The viewer remains independent of the application and can inspect committed
records after it exits. A crash without settlement evidence leaves follow
pending until you detach.

Observe an existing archive without requesting execution:

```bash
obzenflow show /path/to/run
obzenflow show /path/to/run --follow --json
obzenflow show /path/to/run --detail
obzenflow journal inspect /path/to/run
obzenflow journal export-jsonl /path/to/run --output records.jsonl
obzenflow verify --baseline /path/to/original --candidate /path/to/replay
```

`show` reads fixed committed prefixes; `--follow` keeps reading until terminal
outcome, pipeline drain and this reader's coverage are all confirmed. Its exit
code is 0 for successful observation and 4 for an operational error; the recorded
pipeline outcome is reported separately. Verification retains codes 0 match,
1 divergence, 2 uncertified, 3 refused and 4 operational error.

`show --json` writes version-1 JSONL records containing `version`, `run`, `journal`,
`position`, `kind` and the canonical `record` envelope/payload. Diagnostics go to
stderr. Positions are append ordinals within each journal, without a global
ordering across journals. `journal export-jsonl` keeps its existing canonical
envelope/payload format. Schema 6.0 requires the manifest's pipeline writer
identity; older archives require a matching older framework/CLI.

`start --follow` requires the server's admitted disk archive to be accessible on
the client filesystem. It checks the archive and host identities before Play.
Bare `start` also works with ephemeral runs. Neither command retries an uncertain
Play request. For an authenticated host, set `OBZENFLOW_CONTROL_AUTHORIZATION`
to the complete Authorization header value expected by its control-plane policy.

Applications and future Studio/SSE consumers can use the same read service through
`obzenflow::journal::read::{open_disk_run, RunSnapshot, RunTail}`. They receive
concrete read-only handles; implementing readers is a journal backend concern.

## Documentation

Visit the [ObzenFlow website](https://obzenflow.dev/) for an introduction to the framework, its guarantees, and intended uses.

- [How ObzenFlow Works](https://obzenflow.dev/product/how-obzenflow-works/) explains flow declarations, effects, journals, and run modes.
- [Tutorials](https://obzenflow.dev/tutorials/) walk through building flows, modelling bank transactions, and running live AI inference.
- [Philosophy](https://obzenflow.dev/philosophy/) explains the design principles.

## License

Dual-licensed under [MIT](https://github.com/obzenflow/obzenflow/blob/main/LICENSE-MIT) OR [Apache-2.0](https://github.com/obzenflow/obzenflow/blob/main/LICENSE-APACHE).
