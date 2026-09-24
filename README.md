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
obzenflow show /path/to/run --verbose
obzenflow show /path/to/run --quiet
obzenflow show /path/to/run --follow --json
obzenflow show /path/to/run --detail
obzenflow journal inspect /path/to/run
obzenflow journal export-jsonl /path/to/run --output records.jsonl
obzenflow verify --baseline /path/to/original --candidate /path/to/replay
```

The default human view shows business facts, effects and deliveries with a
stage-kind heading, `output type ← stage name(input type)`, a compact vector
clock, and a formatted JSON payload underneath. The output appears once, on the left of
the arrow; the right side explains which stage and recorded input produced it.
`--verbose` also shows runtime, lifecycle, signal and system records.
`--explain` adds teaching notes. `--quiet` uses one line per selected
record without clocks or count tables; `--detail` adds complete envelopes and
payloads for those records and the full recorded run manifest. Both can be combined
with `--verbose`.

- The palette draws on [Event Storming](https://kevinwebber.ca/series/domain-modelling-in-practice/part-2/):
  source and transform facts are bold orange, including declines and cancellations.
  `STATEFUL` and `JOIN` outputs use green as a read-model cue; catalog joins are
  recorded as `JOIN`. This styling does not infer an unrecorded state snapshot or
  change the output's journal type. Effect and delivery evidence use softer pink,
  borrowing the external-system color. Runtime records stay gray.
  Each heading and its entire output expression share the same color.
  `SOURCE`, `TRANSFORM`, `EFFECTFUL TRANSFORM`, `STATEFUL`, `EFFECTFUL STATEFUL`
  and `JOIN` name the recorded stage kind for facts;
  `EFFECT` and `DELIVERY` distinguish execution evidence in plain text too.
  New runs record the descriptor's effectful capability, including stages that
  emit facts without invoking an effect. For archives without this metadata,
  the viewer recognizes effectful stages after observing their own effect
  provenance or execution evidence; until then it shows the broader stage kind.
  Color is automatic in terminals;
  pipes and `NO_COLOR` use plain text. Override with `--color always` or
  `--color never`.
- Each expression uses the recorded output type, the declared stage name and the
  recorded input types. The arrow describes provenance, not equality, assignment
  or a claim that the stage is a pure function. Only the clock changes color within a line.
  Event names are the recorded schema names; the CLI does not guess Rust types,
  source file locations, business rules, currencies or unrecorded state values.
  Sources use `output type ← stage name()` because they have no upstream event
  argument. Effect and delivery expressions also use their schema names;
  the operation and outcome remain in the payload fields.
- A clock key lists stages once. Each clock sits below the output expression,
  aligned at the left margin. Counters have no padding or added
  spaces and retain every digit. Only the recorded writer's digits are
  underlined in the same color as the row; other components remain gray. This identifies the writer,
  not which components changed: merging history can advance other counters too.
  Counters are per-writer logical history, not journal positions or business-event
  counts. Other writers remain explicitly named; their components are never
  silently discarded. Input references do not repeat their clocks.
- Recorded parent IDs resolve the input side. A bounded lookahead allows inputs
  from another journal to arrive, and adjacent facts with the same parents can
  appear together. Each emitted fact has its own stage heading, output expression,
  clock and payload. The output type, clock and payload describe the same recorded
  fact. A group does not
  assert an atomic transaction. Unavailable parents are labeled unresolved.
  Journal order is retained; display order across journals is not a global clock.
- `EFFECT` marks execution evidence; domain facts produced by an effect retain
  their stage-kind heading and fact styling. Failures expose their recorded outcome, including
  structured rejection causes. Replay provenance says **read from journal**;
  observation never executes an effect. `DELIVERY` rows distinguish success,
  failure, buffering and partial delivery.
- Payloads use JSON with braces at the left margin, one field per line, quoted
  strings and keys, and two-space indentation for nested objects and arrays.
  Numbers, booleans and null retain their types. The normal human view targets
  90 columns; `COLUMNS` can narrow it to 40–90 columns, and wider terminals do
  not stretch the output. Oversized string values end in `…` with a separate
  shortening note; field names and numbers are retained. `--detail` and `--json`
  preserve complete evidence. Quiet rows use a compact JSON preview and an
  explicit `… [--detail]` marker when the row exceeds the display width.
- At snapshot end, confirmed settlement or Ctrl-C, a grayscale footer ends with how
  many journal entries the CLI observed, the recorded run outcome and where reading
  stopped. A compact `run_manifest.json` summary first describes the run, followed by the
  referenced system journal and each stage's separate data and error journals.
  Journal counts include every observed entry, even hidden runtime entries; empty
  journals remain visible with zero observed entries in the inventory. Each journal
  with displayed entries then gets its own **Count / Event type / Author / Author type** table.
  Stage data journals show **Stage**, **Subscribes to**, **Writes to** (the exact
  journal filename), and **Subscribers**. Connections come from the manifest's
  forward stage topology; an em dash means no recorded connection in that direction.
  System and error journals retain their exact filename headings; error journals
  also identify their stage. Data and error journals stay separate. A short note
  below the tables explains journal ownership, control forwarding and EOF completion.
  The observation count and
  run outcome are the final lines. Columns fit each
  journal's contents, and author types use compact
  labels such as `FiniteSource`, `Sink` and `MetricsAggregator`.
  Counts are grouped by event type and recorded writer within each
  journal. Author means the event's original author, so forwarded entries retain their
  original source. Supervisor types and runtime names come from
  `system.supervisor.registered` records; every supervisor registers through its
  shared runner before its state machine executes. Stage names resolve through
  the manifest. A partial observation without a registration says `Not recorded`.
  Archive schema 7.0 rejects earlier journals; no older-archive inference is used.
  Counts include replayed evidence,
  with grouped facts counted individually. The manifest is recorded during flow
  construction; completion comes from the system journal. Detaching does not turn
  missing outcome evidence into a completed run. `--detail` prints the full manifest,
  including exact filenames.

This is an append-only teaching and demo interface. Journal search, storage and
analysis can develop independently in RustFS/Quickwit; the CLI has no full-screen
UI or search/indexing subsystem. For file or pipe experiments:

```bash
obzenflow show /path/to/run --json > records.ndjson
```

`show` reads fixed committed prefixes; `--follow` keeps reading until terminal
outcome, pipeline drain and this reader's coverage are all confirmed. Its exit
code is 0 for successful observation and 4 for an operational error; the recorded
pipeline outcome is reported separately. Verification retains codes 0 match,
1 divergence, 2 uncertified, 3 refused and 4 operational error.

`show --json` always emits all records, including runtime and system evidence.
It writes version-1 JSONL records containing `version`, `run`, `journal`,
`position`, `kind` and the canonical `record` envelope/payload. It adds no ANSI,
legend or human footer. Diagnostics and the structured exit summary go to stderr.
Positions are append ordinals within each journal, without a global
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
