# ObzenFlow Infrastructure

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate, though application binaries typically use `obzenflow_infra::application::FlowApplication` directly.

**Layer:** Infrastructure (outermost). Depends on all other ObzenFlow workspace crates.

Wires the workspace together into a runnable application, providing journal backends, an optional HTTP server, and the `FlowApplication` entry point that most binaries use directly.

- **`FlowApplication`** runner that turns a `flow! { ... }` definition into a managed process with tracing, CLI parsing, optional HTTP server, Prometheus metrics, and graceful shutdown.
- **Journal backends.** Disk-backed (`disk_journals`) and in-memory (`memory_journals`) implementations of the journaling traits, plus replay archive support.
- **Web server and endpoints** (feature `warp-server`). Topology, metrics, health/readiness, flow control, SSE event streaming, and HTTP ingestion endpoints.
- **Outbound HTTP client** (feature `reqwest-client`). Reqwest-based implementation of the core `HttpClient` trait.
- **Typed env parsing** (`env`). `env_var`, `env_var_or`, `env_var_required`, and `env_bool` helpers that distinguish missing from malformed environment variables with actionable error messages.

## Features

No default features are enabled. Opt in as needed:

| Feature | What it enables |
|---------|-----------------|
| `warp-server` | HTTP server, `--server` CLI flag, hosting endpoints |
| `console` | `tokio-console` support for async debugging |
| `reqwest-client` | Outbound HTTP client for pull/poll sources |
| `prometheus` | Convenience re-export of the Prometheus exporter |

## `FlowApplication` entry points

- `FlowApplication::run(flow).await` for the common case.
- `FlowApplication::builder()` for custom log levels, extra web endpoints, or hooks.

CLI flags include `--server`, `--server-port`, `--replay-from`, `--cors-mode`, and others. See `FlowConfig` for the full list.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
