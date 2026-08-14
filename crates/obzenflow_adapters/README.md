# ObzenFlow Adapters

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Adapters (outer). Depends on `obzenflow_runtime` and `obzenflow_core`.

Provides concrete typed policy, observer, source, sink, and monitoring implementations.

- **Typed policy and observer attachments.** Control factories for rate limiting, circuit breaking, and effect resilience bind at supported live-I/O surfaces. Seven ordinary observer helpers bind read-only, live-only callbacks at framework-owned interception points through the `flow!` macro.
- **Source adapters.** Ready-to-use source handlers: CSV file reader, HTTP pull/poll sources with pluggable decoders, and an HTTP ingestion source for server-mode flows.
- **Sink adapters.** Console sink (with JSON, debug, and table formatters) and CSV file sink.
- **Monitoring exporters.** Prometheus exporter out of the box, with a console summary exporter for local development. The `MetricsExporter` trait makes it straightforward to build custom exporters for other backends.

The built-in adapters are intentionally small in number. To create a source,
sink, transform, stateful stage, or join, implement its runtime handler trait,
wire it into a `flow!` block with the stage macros, and let the runtime handle
journalling, lifecycle, and supervision. Custom cross-cutting behaviour binds
through the typed factory attachment surfaces, not a generic handler wrapper.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
