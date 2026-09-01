# ObzenFlow

ObzenFlow is a durable execution runtime in Rust for high-consequence systems. Every stage of a flow writes what happened, including the results of outside actions, to an append-only journal. That record lets the runtime rebuild state, verify a replay against the original run, and resume interrupted work without re-firing a committed effect. Out of the box, ObzenFlow is a single binary with no platform, broker, cluster, or database to run.

Status: **pre-1.0**. APIs are still evolving and may change between releases.

Where to go next:

- [What is ObzenFlow?](https://obzenflow.dev/product/what-is-obzenflow/) covers the guarantees and the systems they are built for.
- [How ObzenFlow Works](https://obzenflow.dev/product/how-obzenflow-works/) covers the DSL, effects, journals, and run modes in detail.
- [Tutorials](https://obzenflow.dev/tutorials/) walk from a first flow to live AI inference.
- [Philosophy](https://obzenflow.dev/philosophy/) explains the design principles underneath.

## The shape of a flow

Every ObzenFlow application follows the same shape:

```rust,ignore
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
                input |> enrich |> output;
            }
        })
    })
}

FlowApplication::run(build_flow()).await?;
```

Builder-owned handlers are ordinary Rust locals inside the deferred materialiser;
stage rows reference those locals by name.

For runnable versions with real domain types and handlers, see the examples catalog in `examples/README.md`.

## Quickstart: durable execution in two commands

Prerequisites:
- Rust `1.93.0` (pinned in `rust-toolchain.toml`)

Run the payment gateway example, a flow that authorizes orders through an unreliable gateway behind a declared effect and a circuit breaker:

```bash
cargo run -p obzenflow --example payment_gateway_resilience
```

The completion footer prints the run's archive path and the exact replay command. Replay the finished run from its record with verification:

```bash
cargo run -p obzenflow --example payment_gateway_resilience -- \
    --replay-from target/payment-gateway-logs/flows/<run_id> --verify
```

Replay reads the archived inputs instead of polling the sources and substitutes committed effect outcomes instead of calling the gateway again. A certified match prints `output matched the original run, 0 differences`. That is the core of durable execution. The record of a run is sufficient to rebuild it, verify it, and continue it.

## More examples

The full catalog with grouped commands and code pointers is in `examples/README.md`. A few highlights:

```bash
# Framework overview: reference catalogs + joins + stateful summary
cargo run -p obzenflow --example product_catalog_enrichment

# End-to-end HTTP service: ingress, joins, projections, /metrics
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server

# Live AI inference
cargo run -p obzenflow --example one_shot_inference_demo --features ai -- \
    --config examples/one_shot_inference_demo/obzenflow.toml

# Chunked AI map-reduce over a live HTTP source
cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres"
```

No features are enabled by default. `--features obzenflow_infra/warp-server` enables the HTTP server and web endpoints, `--features http-pull` enables HTTP pull sources, and `--features postgres` enables the PostgreSQL sink. PostgreSQL applications accept an externally supplied `OBZENFLOW_POSTGRES_URL`; they do not depend on repository tooling to launch the backing service. The optional contributor harness uses `cargo xtask postgres up`, retains its loopback endpoint and named volume, and prints a password-free client profile through `cargo xtask postgres connection`. It uses explicit loopback-protected plaintext without generating local certificates. Its canonical disposable proof remains `cargo xtask postgres test`, which separately retains the real verified-TLS tests. Repository-only PostgreSQL orchestration is not included in the published crate. See the [repository development-service guide](https://github.com/obzenflow/obzenflow/blob/main/dev/postgres/README.md) for the local lifecycle and `crates/obzenflow_infra/README.md` for the remaining feature matrix. An optional Prometheus + Grafana monitoring stack lives in `monitoring/`.

## Project organization

ObzenFlow follows an onion architecture: `obzenflow_core` defines the business domain and ports (traits), and outer layers provide implementations, orchestration, wiring, and concrete integrations.

- `crates/obzenflow_core/README.md`: core domain types + stable interfaces (events, journals, contracts, middleware ports)
- `crates/obzenflow_derive/README.md`: derive macros for the authoring surface (`EffectOutcomeFacts` for effect outcome carriers)
- `crates/obzenflow_runtime/README.md`: stage execution + supervisors + runtime orchestration (the engine)
- `crates/obzenflow_dsl/README.md`: the `flow!` DSL and how it builds a runnable flow graph
- `crates/obzenflow_infra/README.md`: `FlowApplication` + journaling/web/HTTP implementations, mostly behind feature flags
- `crates/obzenflow_adapters/README.md`: middleware + concrete sources/sinks composed into flows
- `docs/type-and-effect-stage-grammar.md`: `uses` capability clauses, lexical typed bindings, and migration from the pre-132a surface

The root `obzenflow` crate is a convenience re-export layer for common sources/sinks (`src/sources.rs`, `src/sinks.rs`). The remaining workspace crates, `obzenflow_benchmarks` and `obzenflow_sketches`, are internal support crates outside the public surface.

## Project policies

- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Governance: `GOVERNANCE.md`
- Security: `SECURITY.md`
- Trademarks: `TRADEMARKS.md`

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
