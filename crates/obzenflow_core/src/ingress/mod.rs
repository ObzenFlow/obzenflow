// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Hosted-ingress admission and ingestion.
//!
//! This is the single home for the hosted-edge ingress story. It consolidates
//! the former root-level `ingress.rs` (admission port and binding slot) and the
//! former `event::ingestion` module (submission DTOs, accepted-event provenance,
//! and the live ingestion gauge) so one concept has one home.
//!
//! - [`admission`]: the fail-fast admission vocabulary (`IngressAttemptContext`,
//!   `IngressAdmissionDecision`, `IngressAdmissionOutcome`, `IngressRefusalReason`,
//!   `EdgeShedReason`, `IngressAttemptSeq`).
//! - [`boundary`]: the core-owned neutral admission port `IngressBoundaryMiddleware`
//!   (infra calls it, adapters implement it).
//! - [`binding`]: the write-once `HostedIngressBindingSlot` the DSL fills and
//!   `FlowApplication` reads during web-surface wiring.
//! - [`submission`]: the HTTP submission DTOs, the accepted-event `IngressContext`
//!   provenance, and the live `IngestionTelemetry` gauge.

mod admission;
mod binding;
mod boundary;
mod submission;

pub use admission::{
    EdgeShedReason, IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext,
    IngressAttemptSeq, IngressRefusalKind, IngressRefusalReason,
};
pub use binding::{FilledHostedIngress, HostedIngressAlreadyBound, HostedIngressBindingSlot};
pub use boundary::IngressBoundaryMiddleware;
pub use submission::{
    BatchSubmission, EventSubmission, IngestionTelemetry, IngestionTelemetrySnapshot,
    IngressContext, SubmissionIngressContext, SubmissionResponse,
};
