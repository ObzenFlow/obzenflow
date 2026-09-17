# ObzenFlow Core

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Core (innermost). No dependencies on other ObzenFlow workspace crates.

The business-domain nucleus of the framework, defining the types and traits the rest of the system speaks:

- Event model (`ChainEvent`, `SystemEvent`, payloads, context blocks)
- Journaling contracts (`Journal<T>`, `JournalReader<T>`, `FlowJournalFactory`, `ReplayArchive`)
- Verification contracts between stages (`Contract` + built-in contracts)
- Measurement snapshots, observability contracts, and the synchronous `MetricsSnapshotExporter` publication port
- Ports for outer layers (HTTP client and portable web endpoint interfaces, control-middleware ports)
- Strong identifiers and time primitives (typed IDs, `MetricsDuration`)
- Typed payloads (`TypedPayload`) for compile-time event type resolution and schema versioning
- Effect outcome carriers (`EffectOutcomeFacts`, `TypedFactSet`) binding an effect's possible outcomes to the exact typed facts a stage may author

This crate intentionally avoids infrastructure concerns (storage, networking, async runtimes, logging). Outer layers implement these interfaces and inject them into runtime services.

Record components have separate homes under `event`: `envelope`, `provenance`, `observability`, and `payloads`. `JournalEvent` describes an authored event and `JournalRecord` composes the committed envelope and payload. Protected execution accounting belongs to provenance; diagnostic snapshots belong to observability.

The `journal` module owns the storage contracts: append inputs, configuration, readers, factories, and archives. Both `append` and `append_group` take `AppendOptions`, which carries the causal parent and optional deferred capture. Capture reads the event and returns only its optional observation packet. The journal applies its configured policy to that complete packet. `JournalConfig::default()` retains observability on every record; sparse capture is opt-in. Runtime executes replay and gathers measurements through these Core contracts.

Observation retention uses a shared Core family key: the selected field or record
variant name plus its effect or edge subject. Strum derives record variant names
from `ObservationRecord`; there is no parallel observation-kind catalogue or
numbered schema. Field selection is explicit and exhaustive, and runtime snapshots
keep their own capture stamps. These keys belong to internal retention and
disposable indexes; journal records and backend HTTP/SSE schemas keep their
existing Serde representations.

## Record contract (FLOWIP-145a)

Manifest 4.0 journals and JSONL exports contain exactly `envelope` and `payload` at the root. Event authors own `envelope.provenance.event`; journals assign `envelope.provenance.journal`. The descriptor selects the payload family: `fact`, `composite_data`, `flow_signal`, `delivery`, `execution`, or `system`. Application facts retain their original JSON value, including scalars, arrays and null. Built-in composite carriers are durable intermediary records; a final application result remains a fact even when it has composite ancestry.

Optional `envelope.observability` attachments carry typed measurements and their capture identity. Removing an attachment does not remove execution accounting, causal identity, effect evidence or atomic membership. Live measurements use the runtime's bounded observation view; they do not create journal entries. Missing measurement families stay unavailable, and a measured zero remains a measurement.

`envelope.provenance.event.runtime.accounting` retains factual counters. Diagnostic progress positions, copied event IDs/clocks and the instrumentation FSM label live together in `envelope.observability.runtime_snapshot`. That snapshot has its own capture stamp because the appending stage can differ from the owner of existing handler or forwarded measurements. These copies establish neither recovery positions nor factual lifecycle state.

The manifest change is a clean schema break. Journal framing is version 4; older record layouts require recording a new run with this build.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
