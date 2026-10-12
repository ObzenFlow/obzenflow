// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP ingestion submission DTOs and accepted-event provenance.
//!
//! Consolidated here from the former `event::ingestion` module so the whole
//! ingress story (admission, boundary, binding, and these submission/provenance
//! types) has a single home under `obzenflow_core::ingress`.

use super::admission::IngressAttemptSeq;
use super::identity::IngressRequestIdentity;
use super::IngressKey;
use crate::EventType;
use serde::{Deserialize, Serialize};

/// Event submission payload from HTTP clients (FLOWIP-084d).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSubmission {
    /// Event type (e.g., "order.created")
    pub event_type: EventType,

    /// Event payload (arbitrary JSON)
    pub data: serde_json::Value,

    /// Optional metadata (arbitrary JSON); never part of request identity.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// Batch submission payload (FLOWIP-084d).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSubmission {
    pub events: Vec<EventSubmission>,
}

/// Provenance attached to accepted events when they enter the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngressContext {
    pub accepted_at_ns: u64,
    pub ingress_key: IngressKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<usize>,
    /// FLOWIP-115d: the per-attempt sequence carried onto the accepted source row,
    /// the merge key with system-journal `IngressRefusal` facts.
    pub attempt_seq: IngressAttemptSeq,
    /// FLOWIP-084l: the sender's request identity, committed with the row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<IngressRequestIdentity>,
}
