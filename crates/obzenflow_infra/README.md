# ObzenFlow Outer Infrastructure (`obzenflow_infra`)

This crate is an internal implementation detail of the ObzenFlow project. It provides the **outermost infrastructure layer**: concrete implementations for persistence, web/HTTP integration, and an application runner (`FlowApplication`) that turns a `flow! { ... }` definition into a managed process (CLI, observability, optional server, shutdown, replay flags).

Most users should depend on the top-level `obzenflow` crate, but application binaries typically use `obzenflow_infra::application::FlowApplication`.

## What this crate contains

`obzenflow_infra` provides concrete implementations for framework “ports”, and utilities for running flows:

- **Application runner**: `FlowApplication` (`src/application/flow_application.rs`)
- **Journals**: disk-backed and in-memory implementations of `obzenflow_core::journal::Journal` (`src/journal/`)
- **Journal factories + replay archive**: implementations of `obzenflow_runtime_services::journal::FlowJournalFactory` and `obzenflow_runtime_services::replay::ReplayArchive` (`src/journal/factory.rs`, `src/journal/disk/replay_archive.rs`)
- **Web server + HTTP endpoints**: a Warp-based `obzenflow_core::web::WebServer` and built-in endpoints (`src/web/`) behind a feature flag
- **Outbound HTTP client**: a Reqwest-based `obzenflow_core::http_client::HttpClient` behind a feature flag (`src/http_client/`)
- **Monitoring backend glue**: small re-exports for metrics exporters (actual exporters live in adapters)

## Features (compile-time integration switches)

`obzenflow_infra` uses Cargo features to compile optional integrations. **No default features are enabled**.

| Feature | Enables | Injected into | Notes |
|---|---|---|---|
| `warp-server` | Warp HTTP server + hosting endpoints | `obzenflow_core::web::WebServer` | Required for `FlowApplication --server` and for hosting extra endpoints (ingestion/UI). |
| `console` | `console-subscriber` / `tokio-console` support | `FlowApplicationBuilder::with_console_subscriber` | API exists regardless; without the feature it warns and becomes a no-op. For full data: compile with `RUSTFLAGS="--cfg tokio_unstable"`. |
| `reqwest-client` | Reqwest-backed outbound HTTP client | `obzenflow_core::http_client::HttpClient` | Enables `obzenflow_infra::http_client::default_http_client()`. |
| `prometheus` | Convenience re-export of `PrometheusExporter` | N/A (export surface only) | Metrics exporters are implemented in `obzenflow_adapters`; DSL selects exporter via env vars. |

Examples:

- Enable server mode: `cargo run -p obzenflow --features obzenflow_infra/warp-server -- --server`
- Enable tokio-console: `cargo run -p obzenflow --features obzenflow_infra/console -- ...`
- Enable outbound HTTP client: `cargo run -p obzenflow --features obzenflow_infra/reqwest-client -- ...`

## Architecture (maintainers)

ObzenFlow follows an onion architecture. Inner crates define business-domain abstractions, and outer crates provide implementations and wire them together via traits and composition.

**Layer:** Infrastructure (outermost). Depends on `obzenflow_dsl_infra`, `obzenflow_adapters`, `obzenflow_runtime_services`, and `obzenflow_core`.

### Layering overview

`obzenflow_infra` is the **outermost** workspace crate: it depends on the inner ObzenFlow crates and provides concrete implementations + app wiring.

For the exact workspace crate dependency relationships, inspect the workspace `Cargo.toml` and the relevant crate `Cargo.toml`.

### How implementations get “injected inward”

Infra doesn’t "reach into" core to "do things". Infra implements core/runtime traits and then **outer layers wire those implementations into runtime services**.

Concrete injection points:

- `disk_journals(PathBuf)` / `memory_journals()` (`src/journal/factory.rs`) produce a per-flow factory that implements `FlowJournalFactory`. The DSL (`flow!`) uses it to allocate journals and write `run_manifest.json`.
- `WarpServer` (feature `warp-server`) implements `obzenflow_core::web::WebServer` and hosts `HttpEndpoint` trait objects produced by infra and other crates.
- `ReqwestHttpClient` (feature `reqwest-client`) implements `obzenflow_core::http_client::HttpClient`.
- `DiskReplayArchive` implements `obzenflow_runtime_services::replay::ReplayArchive` and is injected into runtime via `FlowJournalFactory::replay_archive_from_env`.

## Running flows: `FlowApplication`

`FlowApplication` is the recommended way to run flows in binaries. It:

- Initializes tracing (and optionally tokio-console)
- Parses CLI flags (`FlowConfig`) for server/replay options
- Awaits the `FlowDefinition` returned by `flow! { ... }` to build a `FlowHandle`
- Starts the HTTP server when `--server` is enabled (feature-gated)
- Runs the flow and manages graceful shutdown

Entry points:

- `FlowApplication::run(flow).await` (best default)
- `FlowApplication::builder().run_async(flow).await` (custom log level, extra endpoints, hooks)
- `FlowApplication::builder().run_blocking(flow)` (no `#[tokio::main]`, infra builds the runtime)

Relevant CLI flags (`src/application/config.rs`):

- `--server`, `--server-port`
- `--startup-mode=auto|manual` (server mode)
- `--cors-mode=allow-any-origin|allow-list|same-origin` and `--cors-allow-origin ...`
- `--replay-from <run_dir>`, `--allow-incomplete-archive`

Feature interactions:

- Without `warp-server`, `--server` logs a warning and continues without hosting HTTP endpoints.
- If you register **extra endpoints** (e.g., ingestion endpoints), `FlowApplication` requires a metrics exporter to be present (it will error if metrics are disabled).
- `with_console_subscriber()` is always callable; it only takes effect with the `console` feature.

## Journals

This crate provides the standard persistence backends used by the DSL and runtime services.

### Disk journals

`DiskJournal<T>` (`src/journal/disk/disk_journal.rs`) is an append-only, file-backed implementation of `obzenflow_core::journal::Journal<T>`.

Key properties:

- **Causal metadata**: assigns vector clocks (`VectorClock`) per writer and updates them from the parent envelope.
- **Framed log lines**: records are written as `<len>:<crc>:<json>\n` to avoid torn-read corruption; legacy JSON-only lines are still accepted for backwards compatibility.
- **Reader/writer coordination**: a shared per-path `RwLock` prevents readers from parsing partial writes across multiple `DiskJournal` instances pointing to the same file.
- **Cursor reader**: `DiskJournalReader<T>` implements `JournalReader<T>` with O(1) sequential reads.

### Memory journals

`MemoryJournal<T>` (`src/journal/memory/memory_journal.rs`) is a thread-safe in-memory `Journal<T>` used in tests and benchmarks. Its `JournalReader<T>` has tail-like semantics: when it reaches the end it returns `Ok(None)` but will observe newly appended events in later calls.

### Factories used by the DSL

The DSL expects a per-flow factory implementing `obzenflow_runtime_services::journal::FlowJournalFactory` (see `crates/obzenflow_runtime_services/src/journal.rs`).

Infra provides:

- `disk_journals(base_path: PathBuf)` → `Fn(FlowId) -> DiskJournalFactory`
- `memory_journals()` → `Fn(FlowId) -> MemoryJournalFactory`

Disk run directory layout:

- `<base>/flows/<flow_id>/system.log`
- `<base>/flows/<flow_id>/<stage>.log` (plus per-stage `*_error.log`)
- `<base>/flows/<flow_id>/run_manifest.json` (FLOWIP-095a)

`FlowApplication` prints a replay hint after successful completion when disk journals were used.

## Replay archives 

`DiskReplayArchive` (`src/journal/disk/replay_archive.rs`) implements `ReplayArchive` by reading an on-disk run directory:

- Validates `run_manifest.json` version and checks archive version compatibility against `build_info::OBZENFLOW_VERSION`.
- Derives archive status by scanning `system.log` for terminal pipeline lifecycle events.
- Opens source-stage readers using `DiskJournalReader<ChainEvent>`.

Replay wiring:

- `FlowApplication` maps `--replay-from` / `--allow-incomplete-archive` into env vars (`OBZENFLOW_REPLAY_FROM`, `OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE`).
- The DSL calls `FlowJournalFactory::replay_archive_from_env()` and passes the resulting archive into `StageResourcesBuilder`, enabling source stages to “read from archive” instead of their live dependencies.

## Web server + endpoints (feature `warp-server`)

When `warp-server` is enabled, infra provides a Warp-based `WebServer` implementation (`src/web/warp/warp_server.rs`) and a “batteries included” server wiring function:

- `start_web_server_with_config(...)` (`src/web/web_server.rs`)

Built-in endpoints (registered by `start_web_server_with_config` / `FlowApplication --server`):

- `GET /api/topology`: structural topology + cycle membership + middleware stack configs + join metadata (`src/web/endpoints/topology.rs`)
- `GET /metrics`: Prometheus exposition when a `MetricsExporter` is available (`src/web/endpoints/metrics.rs`)
- `GET /health`, `GET /ready`: liveness/readiness (ready reflects pipeline state when a `FlowHandle` is provided)
- `POST /api/flow/control`: play/stop control surface for server-mode flows (`src/web/endpoints/flow_control.rs`)
- `GET /api/flow/events` (SSE): streams low-volume lifecycle/middleware events from the system journal when attached

### HTTP ingestion endpoints 

Infra includes reusable ingestion endpoints that accept HTTP POSTs and feed an in-process channel:

- `create_ingestion_endpoints(IngestionConfig)` (`src/web/endpoints/event_ingestion/mod.rs`)

It returns `(endpoints, rx, state)`:

- `endpoints`: `Vec<Box<dyn HttpEndpoint>>` to register with `FlowApplicationBuilder::with_web_endpoints(...)`
- `rx`: `mpsc::Receiver<EventSubmission>` to wire into `obzenflow_adapters::sources::http::HttpSource`
- `state`: `IngestionState` for readiness + telemetry; can be wired to `FlowHandle::state_receiver()` using a `FlowApplicationBuilder` hook

Supported optional protections:

- Auth: API key, bearer token, or HMAC signature verification (`src/web/endpoints/event_ingestion/auth.rs`)
- Schema validation: single-type or registry via `TypedPayload` validators (`src/web/endpoints/event_ingestion/validation.rs`)

## Outbound HTTP client (feature `reqwest-client`)

Infra provides `ReqwestHttpClient` (`src/http_client/reqwest_client.rs`) implementing the core `HttpClient` trait.

- `default_http_client()` (`src/http_client/mod.rs`) returns an `Arc<dyn HttpClient>` when `reqwest-client` is enabled; otherwise it returns a `FeatureNotEnabled` error.

## Where to look

- Application runner: `src/application/flow_application.rs`
- CLI config: `src/application/config.rs`
- Journals + factories: `src/journal/factory.rs`, `src/journal/disk/`, `src/journal/memory/`
- Replay archive: `src/journal/disk/replay_archive.rs`
- Web server + endpoints: `src/web/web_server.rs`, `src/web/endpoints/`
- Warp server implementation: `src/web/warp/warp_server.rs`
- HTTP client implementations: `src/http_client/`

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md`, and `TRADEMARKS.md`.
