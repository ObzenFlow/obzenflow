# ObzenFlow Core

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Core (innermost). No dependencies on other ObzenFlow workspace crates.

The business-domain nucleus of the framework, defining the types and traits the rest of the system speaks:

- Event model (`ChainEvent`, `SystemEvent`, payloads, context blocks)
- Journaling contracts (`Journal<T>`, `JournalReader<T>`, `JournalRecord<T::Payload>`)
- Verification contracts between stages (`Contract` + built-in contracts)
- Measurement snapshots, observability contracts, and the synchronous `MetricsSnapshotExporter` publication port
- Ports for outer layers (HTTP client and portable web endpoint interfaces, control-middleware ports)
- Strong identifiers and time primitives (typed IDs, `MetricsDuration`)
- Typed payloads (`TypedPayload`) for compile-time event type resolution and schema versioning
- Effect outcome carriers (`EffectOutcomeFacts`, `TypedFactSet`) binding an effect's possible outcomes to the exact typed facts a stage may author

This crate intentionally avoids infrastructure concerns (storage, networking, async runtimes, logging). Outer layers implement these interfaces and inject them into runtime services.

## Record contract (FLOWIP-145a)

Manifest 4.0 journals and JSONL exports contain exactly `envelope` and `payload` at the root. Event authors own `envelope.provenance.event`; journals assign `envelope.provenance.journal`. The descriptor selects the payload family: `fact`, `composite_data`, `flow_signal`, `delivery`, `execution`, or `system`. Application facts retain their original JSON value, including scalars, arrays and null. Built-in composite carriers are durable intermediary records; a final application result remains a fact even when it has composite ancestry.

Optional `envelope.observability` attachments carry typed measurements and their capture identity. Removing an attachment does not remove execution accounting, causal identity, effect evidence or atomic membership. Live measurements use the runtime's bounded observation view; they do not create journal entries. Missing measurement families stay unavailable, and a measured zero remains a measurement.

The manifest change is a clean schema break. Journal framing remains version 2; older record layouts require recording a new run with this build.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
