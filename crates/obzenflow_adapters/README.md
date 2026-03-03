# ObzenFlow Adapters

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Adapters (outer). Depends on `obzenflow_runtime` and `obzenflow_core`.

Provides the concrete middleware, source, sink, and monitoring implementations that plug into the runtime via handler traits.

- **Middleware system.** Composable middleware factories (rate limiting, circuit breakers, observability enrichment, windowing) applied at the flow or stage level through the `flow!` macro. Middleware wraps handler traits transparently so the runtime stays middleware-agnostic.
- **Source adapters.** Ready-to-use source handlers: CSV file reader, HTTP pull/poll sources with pluggable decoders, and an HTTP ingestion source for server-mode flows.
- **Sink adapters.** Console sink (with JSON, debug, and table formatters) and CSV file sink.
- **Monitoring exporters.** Prometheus exporter out of the box, with a console summary exporter for local development. The `MetricsExporter` trait makes it straightforward to build custom exporters for other backends.

The built-in adapters are intentionally small in number. The framework is designed so that creating your own sources, sinks, and middleware is trivial: implement the corresponding handler trait (`SourceHandler`, `SinkHandler`, `TransformHandler`, etc.), wire it into a `flow!` block with the stage macros, and the runtime handles journaling, lifecycle, and supervision automatically.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
