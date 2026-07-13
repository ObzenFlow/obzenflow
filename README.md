# ObzenFlow

ObzenFlow is a high-performance event streaming and processing framework for Rust, built around **durable per-stage journals**, **typed events**, and an ergonomics-first DSL for composing pipelines.

Status: **pre-1.0**. APIs are still evolving and may change between releases.

## Principles

ObzenFlow is built around journal-first execution, wide-event observability, and evidence-based correctness. Every stage reads from upstream append-only journals and writes its outputs to its own journal, making the system’s journaled history both the execution substrate and the primary observability surface.

For the full design philosophy, see [obzenflow.dev/philosophy](https://obzenflow.dev/philosophy/).

Every ObzenFlow application follows the same shape:

```rust,ignore
FlowApplication::run(flow! {
    name: "my_pipeline",
    journals: disk_journals("target/logs".into()),
    middleware: [rate_limit(100.0)],

    stages: {
        input = source!(InputEvent => my_source);
        enrich = transform!(InputEvent -> OutputEvent => my_transform);
        output = sink!(OutputEvent => my_sink);
    },

    topology: {
        input |> enrich |> output;
    }
})
.await?;
```

For runnable versions with real domain types and handlers, see the examples catalog in `examples/README.md`.

## Quickstart: run a real end-to-end demo (HTTP ingestion)

Prerequisites:
- Rust `1.93.0` (pinned in `rust-toolchain.toml`)

Run with localhost-only defaults:

```bash
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server
```

The demo uses its checked-in startup config by default. To override the startup file explicitly, pass `-- --config <path>` after the Cargo arguments.

Recommended control-plane auth variant:

```bash
export OBZENFLOW_PIGGY_BANK_CONTROL_PLANE_AUTH='Bearer piggy-bank-demo-secret'
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --config examples/http_ingestion_piggy_bank_demo/obzenflow.auth.toml
```

In another terminal, post a couple of events:

```bash
curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.account_opened","data":{"account_id":"acct-1","owner":"Alice","initial_balance_cents":1000}}'
curl -XPOST http://127.0.0.1:9090/api/bank/tx/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.ledger_entry","data":{"account_id":"acct-1","kind":"Debit","amount_cents":99,"note":"coffee"}}'
```

Observe:
- Metrics (localhost default): `curl http://127.0.0.1:9090/metrics`
- Topology (localhost default): `curl http://127.0.0.1:9090/api/topology`
- Metrics (auth variant): `curl http://127.0.0.1:9090/metrics -H 'Authorization: Bearer piggy-bank-demo-secret'`
- Topology (auth variant): `curl http://127.0.0.1:9090/api/topology -H 'Authorization: Bearer piggy-bank-demo-secret'`

Code: `examples/http_ingestion_piggy_bank_demo/flow.rs`

## More examples

The full catalog with grouped commands and code pointers is in `examples/README.md`. A few highlights:

```bash
# Framework overview: reference catalogs + joins + stateful summary
cargo run -p obzenflow --example product_catalog_enrichment

# Resilience: circuit breaker + typed fallback + contracts
cargo run -p obzenflow --example payment_gateway_resilience

# Middleware inheritance/override (observe /metrics while it runs)
cargo run -p obzenflow --example flow_middleware_config
```

No features are enabled by default. `--features obzenflow_infra/warp-server` enables the HTTP server and web endpoints, and `--features http-pull` enables HTTP pull sources. See `crates/obzenflow_infra/README.md` for the full feature matrix.

An optional Prometheus + Grafana monitoring stack is available in `monitoring/` (see `monitoring/README.md`).

## Boundary-owned circuit-breaker retry

Retry is an optional recovery policy on the existing circuit breaker, not a
standalone middleware or handler-shell loop. Attach the breaker to an eligible
async source poll, declared effect, or sink delivery:

```rust,ignore
use obzenflow_adapters::middleware::CircuitBreakerBuilder;
use obzenflow_adapters::middleware::control::circuit_breaker::RetryLimits;
use std::time::Duration;

let dependency_breaker = CircuitBreakerBuilder::new(3)
    .with_retry_fixed(Duration::from_millis(250), 3)
    .with_retry_limits(RetryLimits {
        max_single_delay: Duration::from_secs(5),
        max_total_wall_time: Duration::from_secs(20),
    })
    .build();
```

`max_attempts` counts total physical executions, including the initial call.
Async sources must explicitly declare repeatability after error or cancellation;
effects and sinks use their existing typed safety declarations. Undeclared,
transactional, synchronous, non-idempotent, or nested-retry attachments fail at
materialisation. Strict replay performs no live retry calls or sleeps.

For `HttpPullSource`, boundary retry requires
`HttpRetryConfig::disabled()` plus the decoder's explicit
`retry_safe_requests()` proof. A client or SDK whose internal retry cannot be
disabled remains opaque inside one framework attempt; its request timeout must
therefore fit inside the outer boundary's total-wall deadline.

## Project organization

ObzenFlow follows an onion architecture: `obzenflow_core` defines the business domain and “ports” (traits), and outer layers provide implementations, orchestration, wiring, and concrete integrations.

Inner layers are intentionally generic (domain types + traits) and avoid I/O and runtime/framework integration. Outer layers provide concrete implementations (journals, web/HTTP, middleware/exporters) and wire them into runtime services via traits and composition.

- `crates/obzenflow_core/README.md`: core domain types + stable interfaces (events, journals, contracts, middleware ports)
- `crates/obzenflow_runtime/README.md`: stage execution + supervisors + runtime orchestration (the engine)
- `crates/obzenflow_dsl/README.md`: the `flow!` DSL and how it builds a runnable flow graph (including middleware resolution)
- `crates/obzenflow_infra/README.md`: `FlowApplication` + journaling/web/HTTP implementations + typed env parsing, mostly behind feature flags
- `crates/obzenflow_adapters/README.md`: middleware + concrete sources/sinks (connectors) intended to be composed into flows

The root `obzenflow` crate is a convenience re-export layer for common sources/sinks (`src/sources.rs`, `src/sinks.rs`).

## Project policies

- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Governance: `GOVERNANCE.md`
- Security: `SECURITY.md`
- Trademarks: `TRADEMARKS.md`

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
