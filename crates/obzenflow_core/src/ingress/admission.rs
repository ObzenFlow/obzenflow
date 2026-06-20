// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: the fail-fast ingress admission vocabulary.
//!
//! Per-attempt facts, the admission decision, the observed outcome, and the
//! refusal-reason labels. Ingress is not source polling and not FLOWIP-115e
//! backpressure; it admits, rejects (rate-limited), or sheds (edge overload) a
//! submission attempt without ever waiting for a token while holding a listener
//! request.

use serde::{Deserialize, Serialize};
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

/// The reason an ingress submission attempt was refused, recorded on the durable
/// `IngressRefusal` system-journal fact (FLOWIP-115d). Telemetry projects the
/// per-`(ingress_key, reason)` refusal count from these facts, so the labels are
/// stable and the metric is replay-faithful (`state = fold(facts)`).
///
/// `RateLimited` is the limiter's token-bucket decision (429). `NotReady`,
/// `BufferFull`, `ChannelClosed`, and `ListenerOverloaded` are hosted-edge shed
/// outcomes (503). `Validation` is a per-event input-validation rejection; it is
/// journalled (unlike the other protocol 4xx rejects, which the bucketed
/// HTTP-surface metrics already cover) because a partially accepted batch returns
/// `200`, so its per-event validation rejections are otherwise invisible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressRefusalReason {
    RateLimited,
    NotReady,
    BufferFull,
    ChannelClosed,
    ListenerOverloaded,
    Validation,
}

impl IngressRefusalReason {
    /// Stable label used as the projected metric reason key.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::NotReady => "not_ready",
            Self::BufferFull => "buffer_full",
            Self::ChannelClosed => "channel_closed",
            Self::ListenerOverloaded => "listener_overloaded",
            Self::Validation => "validation",
        }
    }

    /// Map a hosted-edge shed reason to its durable refusal reason. Returns
    /// `None` for `EvidenceUnavailable`: a failed refusal-fact append cannot
    /// itself be journalled, so that path sheds `503` without recording a fact.
    pub fn from_edge_shed(reason: EdgeShedReason) -> Option<Self> {
        match reason {
            EdgeShedReason::NotReady => Some(Self::NotReady),
            EdgeShedReason::BufferFull => Some(Self::BufferFull),
            EdgeShedReason::ChannelClosed => Some(Self::ChannelClosed),
            EdgeShedReason::ListenerOverloaded => Some(Self::ListenerOverloaded),
            EdgeShedReason::EvidenceUnavailable => None,
        }
    }
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
