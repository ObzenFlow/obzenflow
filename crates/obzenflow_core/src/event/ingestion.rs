// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ingress::IngressAttemptSeq;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

/// Snapshot of HTTP ingestion telemetry exported through the infrastructure metrics path.
///
/// FLOWIP-115d: this is now a live gauge only. The former in-memory request,
/// accepted, and per-reason reject counters were removed. Accepted throughput is
/// the hosted source stage's processed count, the protocol 4xx rejects are the
/// bucketed HTTP-surface metrics, and admission, shed, and validation refusals are
/// projected from durable `IngressRefusal` facts (`state = fold(facts)`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestionTelemetrySnapshot {
    pub base_path: String,
    pub channel_depth: usize,
    pub channel_capacity: usize,
}

/// Framework-owned handoff context carried on accepted HTTP submissions before they become
/// `ChainEvent`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionIngressContext {
    pub accepted_at_ns: u64,
    pub base_path: String,
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
    pub base_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<usize>,
    /// FLOWIP-115d: the per-attempt sequence carried onto the accepted source row,
    /// the merge key with system-journal `IngressRefusal` facts.
    pub attempt_seq: IngressAttemptSeq,
}

/// Shared, live HTTP ingestion gauge (FLOWIP-084d, reduced in FLOWIP-115d).
///
/// This holds only the live channel-depth gauge, the one ingestion signal that
/// cannot be reconstructed by folding journal facts. Refusals are durable
/// `IngressRefusal` facts and accepted throughput is the source stage processed
/// count, so neither is kept here as an in-memory counter. It stores a
/// channel-depth closure to avoid pulling async runtime dependencies into the core
/// crate.
pub struct IngestionTelemetry {
    base_path: String,
    channel_capacity: usize,
    channel_depth: Arc<dyn Fn() -> usize + Send + Sync>,
}

impl std::fmt::Debug for IngestionTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionTelemetry")
            .field("base_path", &self.base_path)
            .field("channel_capacity", &self.channel_capacity)
            .finish_non_exhaustive()
    }
}

impl IngestionTelemetry {
    pub fn new(
        base_path: String,
        channel_capacity: usize,
        channel_depth: Arc<dyn Fn() -> usize + Send + Sync>,
    ) -> Self {
        Self {
            base_path,
            channel_capacity,
            channel_depth,
        }
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub fn channel_capacity(&self) -> usize {
        self.channel_capacity
    }

    pub fn channel_depth(&self) -> usize {
        (self.channel_depth)()
    }

    pub fn snapshot(&self) -> IngestionTelemetrySnapshot {
        IngestionTelemetrySnapshot {
            base_path: self.base_path.clone(),
            channel_depth: self.channel_depth(),
            channel_capacity: self.channel_capacity,
        }
    }
}
