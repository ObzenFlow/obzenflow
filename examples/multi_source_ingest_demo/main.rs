// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c canonical example: heterogeneous fan-in via per-branch alignment.
//!
//! # The invariant
//!
//! After FLOWIP-114c, every stage except joins is homogeneous at its fan-in.
//! Two upstreams of different concrete types into the same non-join slot is
//! a build error (`FlowBuildError::EdgeTypingMismatch`). The fix is one of
//! three patterns:
//!
//! 1. **Two typed inputs of different types: use a join.** A `join!` is the
//!    canonical typed two-input operator in ObzenFlow. The `reference` and
//!    `stream` slots are independently typed.
//! 2. **Three or more inputs: per-branch alignment transforms.** Insert a
//!    small transform on each branch that maps the source-specific type to
//!    a common envelope type, so the fan-in is homogeneous on that envelope.
//!    This example demonstrates that pattern.
//! 3. **Allowed but not idiomatic: enum / envelope payload.** A single
//!    upstream can declare an output of sealed-enum type whose variants
//!    carry the original shapes. The type-level rule is satisfied, but the
//!    alignment is hidden inside one stage rather than expressed as a
//!    visible per-branch transform in the topology graph. Prefer per-branch
//!    alignment unless the upstream is naturally union-shaped.
//!
//! # The flow this example builds
//!
//! ```text
//! kafka_source   : KafkaRawEvent  -> align_kafka   : KafkaRawEvent  -> IngestedEvent ┐
//! webhook_source : WebhookEnvelope -> align_webhook : WebhookEnvelope -> IngestedEvent ┼-> aggregator -> sink
//! file_source    : FileLine       -> align_file    : FileLine       -> IngestedEvent ┘
//! ```
//!
//! Every stage is typed; every edge is typed; every alignment step is a
//! visible stage. The fan-in into `aggregator` is homogeneous on
//! `IngestedEvent`.
//!
//! # What you'll see if you violate the rule
//!
//! Three structured `FlowBuildError` variants are emitted by
//! `build_typed_flow!`:
//!
//! - `StageMissingTypingMetadata { stage_name }` when a descriptor has no
//!   typing metadata (the legacy untyped path, gone after 114c).
//! - `UnspecifiedTypingOnApplicableSlot { stage_name, slot }` when an
//!   applicable slot is `Unspecified`. Applicable slots are: `output` for
//!   sources / transforms / stateful / joins; `input` for transforms /
//!   stateful / sinks; `reference` and `stream` for joins.
//! - `EdgeTypingMismatch { upstream_stage, downstream_stage, role,
//!   expected_type, actual_type, kind, suggested_fix }` with `kind`:
//!   - `SingleEdge` when one upstream `Exact` type disagrees with the
//!     downstream slot's declared `Exact` type.
//!   - `HeterogeneousFanIn { other_upstream_stages, other_actual_types }`
//!     when two or more upstreams in the same `(downstream, role)` group
//!     emit different `Exact` types.
//!
//! # Why the runtime is still type-erased
//!
//! The runtime operates on `ChainEvent` (a type-erased wire shape) for
//! durability, replay, and provenance. The typing metadata is a
//! *fingerprint* (in the Apache Flink `TypeInformation` / Apache Beam
//! `TypeDescriptor` / Kafka Streams `Serde` sense), not a Rust-type-system
//! constraint on the runtime. Identity is by `std::any::TypeId`, canonical
//! within one compilation; two stages declared with the same Rust type but
//! different syntactic spellings compare as equal.
//!
//! See FLOWIP-114b "Runtime invariant: one stage = one journal = one
//! output contract" for the architectural reason.
//!
//! # File layout
//!
//! - [`domain`]   — the five payload types (three source-specific, one
//!   envelope, one summary).
//! - [`handlers`] — the three sources, three alignment `Map`s, aggregator,
//!   and sink. Mechanical; not the teaching point.
//! - [`flow`]     — the `flow!` definition. This is the canonical surface
//!   for the FLOWIP-114c authoring pattern.

mod domain;
mod flow;
mod handlers;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    flow::run().await
}
