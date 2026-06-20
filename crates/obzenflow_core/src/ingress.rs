// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: the core-owned neutral ingress admission port.
//!
//! Listener ingress is the moment outside work arrives at a hosted edge, before
//! it enters the internal source buffer. This port is the fail-fast admission
//! contract the hosted HTTP endpoints (`obzenflow_infra`) call and the middleware
//! adapters (`obzenflow_adapters`) implement. It lives in core so infra can call
//! it without depending on adapter middleware concepts, and adapters can
//! implement it without infra depending on them (the FLOWIP-115b onion-ownership
//! rule: putting it in infra creates a dependency cycle, putting it in adapters
//! makes hosted web endpoints depend on adapter middleware).
//!
//! Ingress is not source polling and not FLOWIP-115e backpressure. It admits,
//! rejects (rate-limited), or sheds (edge overload) a submission attempt without
//! ever waiting for a token while holding a listener request.

use crate::StageId;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Monotonic, per-hosted-ingress submission-attempt sequence (FLOWIP-115d).
///
/// One sequence per external submission attempt: a single `POST {base}/events`
/// or a single `POST {base}/batch` consumes one sequence for the whole request.
/// It is attempt context and the v1 cross-journal audit merge key, never part of
/// the protected-unit key. Because it is assigned at admission time and carried
/// on the accepted row, source-journal position may differ from sequence order
/// under concurrency; auditors merge by the carried sequence value, not position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IngressAttemptSeq(pub u64);

/// Distinguishes a rate-limit refusal from a hosted-ingress edge shed.
///
/// `RateLimited` is the rate-limiter middleware's token-bucket decision (maps to
/// `429`). `EdgeShed` is a hosted-ingress admission outcome owned by infra before
/// source data exists (maps to `503`); it is not FLOWIP-115e backpressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressRefusalKind {
    RateLimited,
    EdgeShed,
}

/// Why a hosted-ingress edge shed an attempt. `BufferFull` is edge-shed because
/// the source input channel could not reserve capacity; it is not FLOWIP-115e
/// backpressure, which is runtime-owned downstream credit at output commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeShedReason {
    NotReady,
    BufferFull,
    ChannelClosed,
    ListenerOverloaded,
    EvidenceUnavailable,
}

/// Per-attempt facts handed to the ingress boundary for one external submission.
///
/// The boundary instance is already bound to one protected unit (the linked
/// source stage plus hosted target), so identity is implicit; this carries only
/// the request-scoped facts an admission charge or proof row needs.
#[derive(Debug, Clone, Copy)]
pub struct IngressAttemptContext {
    pub attempt_seq: IngressAttemptSeq,
    /// HTTP submission requests in this attempt (1 for `/events` and `/batch`).
    pub request_count: u64,
    /// Validation-accepted events in this attempt (1 for `/events`; the
    /// validation-accepted subset size for `/batch`).
    pub event_count: u64,
    /// Batches in this attempt (0 for `/events`, 1 for `/batch`).
    pub batch_count: u64,
}

/// The fail-fast admission decision from the ingress boundary (FLOWIP-115d).
///
/// 115D HTTP ingress never waits for a token while holding a listener request: a
/// token exhaustion is a `Reject`, never a wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressAdmissionDecision {
    /// Admit the submission; reserved work may enter the source buffer.
    Accept,
    /// Refuse: the limiter's token bucket would wait. Maps to `429` plus
    /// `Retry-After` where the protocol supports it.
    Reject { retry_after: Option<Duration> },
    /// Shed: a hosted-edge overload outcome. Maps to `503` plus `Retry-After`.
    Shed {
        reason: EdgeShedReason,
        retry_after: Option<Duration>,
    },
}

/// What infra observed after acting on the admission decision and enqueue.
///
/// Admitted policies observe this in reverse order (the forward-admission,
/// fail-fast ingress chain). It never represents downstream source handling or
/// later handler results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressAdmissionOutcome {
    /// Admission succeeded and reserved work was enqueued.
    AcceptedForEnqueue,
    /// A later policy rejected (rate-limited) the attempt.
    RejectedBy,
    /// A later policy or the hosted edge shed the attempt.
    ShedBy,
}

/// The neutral, core-owned ingress admission port.
///
/// Infra (hosted HTTP endpoints) calls it; adapters implement it (composing
/// their policy onion behind it). `on_ingress` is fail-fast and never blocks the
/// listener; `observe` records the admission outcome for the forward-fail-fast
/// chain. Durable reject/shed evidence is owned by the hosting layer's evidence
/// recorder, not this port.
pub trait IngressBoundaryMiddleware: Send + Sync {
    fn label(&self) -> &'static str;

    /// Run admission before an accepted submission enters the internal source
    /// buffer. Fail-fast: it must not wait for a token while holding the request.
    fn on_ingress(&self, attempt: &IngressAttemptContext) -> IngressAdmissionDecision;

    /// Observe the terminal admission outcome for one attempt.
    fn observe(&self, attempt: &IngressAttemptContext, outcome: IngressAdmissionOutcome);
}

// ---------------------------------------------------------------------------
// Hosted-ingress binding slot (FLOWIP-115d)
// ---------------------------------------------------------------------------

/// The boundary plus replay-stable identity the DSL fills into a hosted-ingress
/// slot during source-stage materialization.
///
/// It carries only core-native types so the runtime source-handler trait can
/// return the slot without depending on adapter carrier concepts. The adapter
/// carrier ingress surface/unit are built transiently by the DSL binder from
/// this data when it materializes the boundary.
pub struct FilledHostedIngress {
    /// Runtime stage id of the linked source stage.
    pub stage_id: StageId,
    /// Replay-stable source stage key (`StageConfig.name`, the `run_manifest.json`
    /// key).
    pub stage_key: String,
    /// The composed ingress admission boundary, or `None` when a hosted source
    /// has no ingress middleware attached (the slot is still filled so startup
    /// can verify every hosted surface was placed in topology).
    pub boundary: Option<Arc<dyn IngressBoundaryMiddleware>>,
}

/// A second source stage attempted to bind a hosted surface that was already
/// bound (cloned source halves must not silently attach one listener surface to
/// multiple source stages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostedIngressAlreadyBound;

impl std::fmt::Display for HostedIngressAlreadyBound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hosted ingress binding slot is already bound to a source stage"
        )
    }
}

impl std::error::Error for HostedIngressAlreadyBound {}

/// Write-once binding slot shared between a hosted source's typed half (which
/// enters flow topology) and its hosted web-surface half (which enters
/// `FlowApplication`).
///
/// `http_ingress` creates it, the DSL fills it during source-stage
/// materialization, and `FlowApplication` reads it during web-surface wiring.
/// Cloning shares the same write-once cell, so a fill through the source half is
/// visible to the surface half.
#[derive(Clone)]
pub struct HostedIngressBindingSlot {
    base_path: String,
    cell: Arc<OnceLock<FilledHostedIngress>>,
}

impl HostedIngressBindingSlot {
    pub fn new(base_path: impl Into<String>) -> Self {
        Self {
            base_path: base_path.into(),
            cell: Arc::new(OnceLock::new()),
        }
    }

    /// The normalised hosted base path identifying this surface.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Fill the slot once. Returns `Err` if it was already filled, which the
    /// DSL surfaces as a build error for a double-bound listener surface.
    pub fn fill(&self, filled: FilledHostedIngress) -> Result<(), HostedIngressAlreadyBound> {
        self.cell.set(filled).map_err(|_| HostedIngressAlreadyBound)
    }

    /// The filled binding, or `None` if the source half was never placed in
    /// flow topology (a startup error for a registered hosted surface).
    pub fn filled(&self) -> Option<&FilledHostedIngress> {
        self.cell.get()
    }

    /// Whether the slot has been filled by source-stage materialization.
    pub fn is_filled(&self) -> bool {
        self.cell.get().is_some()
    }
}
