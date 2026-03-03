# ObzenFlow Core

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Core (innermost). No dependencies on other ObzenFlow workspace crates.

The business-domain nucleus of the framework, defining the types and traits the rest of the system speaks:

- Event model (`ChainEvent`, `SystemEvent`, payloads, context blocks)
- Journaling contracts (`Journal<T>`, `JournalReader<T>`, `EventEnvelope<T>`)
- Verification contracts between stages (`Contract` + built-in contracts)
- Metrics and observability interfaces (wide-events DTOs, observer/exporter traits)
- Ports for outer layers (HTTP client and web server abstractions, control-middleware ports)
- Strong identifiers and time primitives (typed IDs, `MetricsDuration`)
- Typed payloads (`TypedPayload`) for compile-time event type resolution and schema versioning

This crate intentionally avoids infrastructure concerns (storage, networking, async runtimes, logging). Outer layers implement these interfaces and inject them into runtime services.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
