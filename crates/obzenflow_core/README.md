# ObzenFlow Core

Core defines the domain types and contracts shared by the framework. Application
authors access these through `obzenflow::schema`, `obzenflow::effects`, and the
other public facade modules.

Core is the innermost layer. It has no dependency on Runtime, Adapters, flow
construction, or Infra. The companion `obzenflow_core_derive` crate generates
implementations of Core's schema contracts at compile time.

## Responsibilities

- Events, typed payloads, fact carriers, identifiers, and causal context.
- Journal append and read contracts, factories, archives, and run manifests.
- Delivery, effect, and stage-verification contracts.
- Measurement snapshots and the `MetricsSnapshotExporter` publication port.
- HTTP, web endpoint, and control-policy ports implemented by outer layers.

Runtime executes flows through these contracts. Storage, network clients,
servers, and reporting implementations belong to outer layers.

## Record contract

A logical journal record contains `envelope` and `payload`. Event authors supply
`envelope.provenance.event`; the journal assigns `envelope.provenance.journal`.
Application facts preserve their JSON values, including scalars, arrays, and null.

Execution accounting and causal identity belong to provenance. Optional
`envelope.observability` attachments carry measurements and their capture stamps.
Changing observation retention does not remove payloads, effect evidence,
delivery receipts, or atomic membership. Missing measurements remain distinct
from measured zero.

`JOURNAL_SCHEMA_VERSION` is the shared authority for records, framing, and
manifests. The current schema is 5.0; older archives require a new recording.
Infra owns the encoding described in the
[journal format reference](https://github.com/obzenflow/obzenflow/blob/main/crates/obzenflow_infra/src/journal/disk/codec/README.md).

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
