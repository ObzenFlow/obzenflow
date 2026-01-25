# ObzenFlow

ObzenFlow is a high-performance event streaming and processing framework for Rust, built around **durable per-stage journals**, **typed events**, and an ergonomics-first DSL for composing pipelines.

Status: **pre-1.0**. APIs are still evolving and may change between releases.

## Principles

Most streaming frameworks are optimized for shipping bytes with minimal overhead. That’s useful, but as streaming systems are increasingly used for data intelligence (training small models, generating predictions, triggering automated actions), the “hard part” shifts.

In these systems, **correctness**, **observability**, and **provenance** matter more than raw throughput. You need to answer audits with evidence: what happened, why it happened, and whether you can reproduce it deterministically.

**ObzenFlow is a next generation streaming framework built around an event‑sourced model.** Every stage reads from upstream *append‑only journals*, and writes its outputs to its own journal. Observability is derived from the same journaled history that drives execution. If an event isn’t recorded, it isn’t part of the system’s truth, and the system's truth is the primary observability substrate. 

This is a powerful model for building dataflow systems that are performant and operationally correct.

- **Journal-first execution**: stage-local immutable journals are the data plane; stages communicate by writing and reading journals.
- **Wide events provide observability**: events carry all flow/stage context so you can compute end-to-end metrics and debug pipelines directly from the journals.
- **Evidence-based correctness**: failures are explicit and typed; optional contracts let you enforce and validate operational invariants such as *at-least-once delivery*.
- **Replayable iteration**: run once, then replay recorded inputs to debug, test new logic, and answer audit questions without re-ingesting.
- **Typed, evolvable events**: model events as Rust types with stable `EVENT_TYPE` and `SCHEMA_VERSION`.

To see what an ObzenFlow application looks like end-to-end, start with the runnable examples in `examples/` (catalog: `examples/README.md`). They include domain types, fixtures/mocks, and a `FlowApplication` entrypoint.

## Quickstart: run a real end-to-end demo (HTTP ingestion)

Prerequisites:
- Rust `1.93.0` (pinned in `rust-toolchain.toml`)

Run (starts the web server and `/metrics`):

```bash
cargo run -p obzenflow --example http_ingestion_piggy_bank_demo --features obzenflow_infra/warp-server -- --server --server-port 9090
```

In another terminal, post a couple of events:

```bash
curl -XPOST http://127.0.0.1:9090/api/bank/accounts/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.account","data":{"account_id":"acct-1","owner":"Alice","initial_balance_cents":1000}}'
curl -XPOST http://127.0.0.1:9090/api/bank/tx/events \
  -H 'content-type: application/json' \
  -d '{"event_type":"bank.tx","data":{"account_id":"acct-1","delta_cents":-99,"note":"coffee"}}'
```

Observe:
- Metrics: `curl http://127.0.0.1:9090/metrics`
- Topology: `curl http://127.0.0.1:9090/api/topology`

Code: `examples/http_ingestion_piggy_bank_demo.rs`

### Continue learning

- Quickstart (HTTP ingestion + join + stateful): `examples/http_ingestion_piggy_bank_demo.rs`
- Examples catalog (recommended runs + code pointers): `examples/README.md`
- Architecture docs (crate READMEs):
    - `crates/obzenflow_core/README.md` — business domain + core “ports” (traits)
    - `crates/obzenflow_runtime_services/README.md` — execution engine + stage orchestration
    - `crates/obzenflow_dsl_infra/README.md` — `flow!` DSL + middleware resolution
    - `crates/obzenflow_infra/README.md` — `FlowApplication` + feature-gated infrastructure implementations
    - `crates/obzenflow_adapters/README.md` — middleware + concrete sources/sinks
- Monitoring stack: `monitoring/README.md`

### More examples

Full catalog (grouped, with commands): `examples/README.md`

```bash
# “Framework overview” flow: reference catalogs + joins + stateful summary
cargo run -p obzenflow --example product_catalog_enrichment

# Resilience story: circuit breaker + typed fallback + contracts
cargo run -p obzenflow --example payment_gateway_resilience

# Middleware inheritance/override (observe /metrics while it runs)
cargo run -p obzenflow --example flow_middleware_config --features obzenflow_infra/warp-server -- --server

# HTTP pull source (defaults to local mock server)
cargo run -p obzenflow --example hn_ingestion_demo --features http-pull
```

### Features (quick notes)

- `--features obzenflow_infra/warp-server` enables HTTP server mode (`--server`) and web endpoints.
- `--features http-pull` enables HTTP pull sources (maps to `obzenflow_infra/reqwest-client`).
- Full feature matrix and injection points: `crates/obzenflow_infra/README.md`

### Monitoring (optional)

Start the local Prometheus + Grafana stack:

```bash
cd monitoring
./setup.sh
```

Details: `monitoring/README.md`

## Project organization

ObzenFlow follows an onion architecture: `obzenflow_core` defines the business domain and “ports” (traits), and outer layers provide implementations, orchestration, wiring, and concrete integrations.

Inner layers are intentionally generic (domain types + traits) and avoid I/O and runtime/framework integration. Outer layers provide concrete implementations (journals, web/HTTP, middleware/exporters) and wire them into runtime services via traits and composition.

- `crates/obzenflow_core/README.md`: core domain types + stable interfaces (events, journals, contracts, middleware ports)
- `crates/obzenflow_runtime_services/README.md`: stage execution + supervisors + runtime orchestration (the engine)
- `crates/obzenflow_dsl_infra/README.md`: the `flow!` DSL and how it builds a runnable flow graph (including middleware resolution)
- `crates/obzenflow_infra/README.md`: `FlowApplication` + journaling/web/HTTP implementations, mostly behind feature flags
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
