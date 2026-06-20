// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP ingestion submission DTOs and accepted-event provenance.
//!
//! Consolidated here from the former `event::ingestion` module so the whole
//! ingress story (admission, boundary, binding, and these submission/provenance
//! types) has a single home under `obzenflow_core::ingress`.

use super::admission::IngressAttemptSeq;
use serde::{Deserialize, Serialize};

/// Event submission payload from HTTP clients (FLOWIP-084d).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSubmission {
    /// Event type (e.g., "order.created")
    pub event_type: String,

    /// Event payload (arbitrary JSON)
    pub data: serde_json::Value,

    /// Optional metadata (arbitrary JSON)
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,

    /// Framework-owned handoff data used while an accepted HTTP event crosses into `flow!`.
    ///
    /// This field is intentionally skipped from user JSON. It is populated by the ingress
    /// endpoints after semantic checks pass and consumed by `HttpSource` when constructing the
    /// first `ChainEvent`.
    #[serde(skip)]
    pub ingress_handoff: Option<SubmissionIngressContext>,
}

/// Batch submission payload (FLOWIP-084d).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSubmission {
    pub events: Vec<EventSubmission>,
}

/// Response for event submission endpoints (FLOWIP-084d).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionResponse {
    pub accepted: usize,
    pub rejected: usize,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub errors: Vec<String>,
}

/// Framework-owned handoff context carried on accepted HTTP submissions before they become
/// `ChainEvent`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionIngressContext {
    pub accepted_at_ns: u64,
    pub ingress_key: String,
    pub batch_index: Option<usize>,
    /// FLOWIP-115d: the per-attempt sequence, the cross-journal merge key against
    /// `IngressRefusal` facts. A batch's accepted rows share one sequence and are
    /// ordered within it by `batch_index`.
    pub attempt_seq: IngressAttemptSeq,
}

/// Provenance attached to accepted events when they enter the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngressContext {
    pub accepted_at_ns: u64,
    pub ingress_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<usize>,
    /// FLOWIP-115d: the per-attempt sequence carried onto the accepted source row,
    /// the merge key with system-journal `IngressRefusal` facts.
    pub attempt_seq: IngressAttemptSeq,
}
