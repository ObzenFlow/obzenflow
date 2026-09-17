// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Optional observation packets shared by live handoff and committed attachments.

use super::payloads::execution_payload::CircuitState;
use super::types::DurationMs;
use crate::ai::LlmObservability;
use crate::id::FlowId;
use crate::time::MetricsDuration;
use crate::{EventId, ReaderGeneration, StageId, WriterId};
use serde::{Deserialize, Serialize};

mod validation;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureScope {
    pub flow_id: FlowId,
    pub resume_generation: ReaderGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CaptureSeq(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureReason {
    Record,
    Initial,
    Periodic,
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureStamp {
    pub capture_scope: CaptureScope,
    pub observer: WriterId,
    pub capture_seq: CaptureSeq,
    pub capture_reason: CaptureReason,
    pub observed_at_ms: u64,
}

/// Forwarding preserves the capture stamp of the existing measurement families.
/// The runtime snapshot carries its own stamp because the appending stage can
/// differ from that owner. Backend views select each populated family separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityContext {
    pub capture: CaptureStamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeObservability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_snapshot: Option<RuntimeSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_time: Option<MetricsDuration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sli: Option<SliSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<ObservationRecord>,
}

impl ObservabilityContext {
    pub fn new(capture: CaptureStamp) -> Self {
        Self {
            capture,
            runtime: None,
            runtime_snapshot: None,
            processing_time: None,
            metrics: None,
            sli: None,
            records: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.runtime.is_none()
            && self.runtime_snapshot.is_none()
            && self.processing_time.is_none()
            && self.metrics.is_none()
            && self.sli.is_none()
            && self.records.is_empty()
    }

    /// Retain only evidence captured by the requested owner. The surrounding
    /// packet's owner does not determine ownership of its runtime snapshot.
    pub fn for_observer(&self, observer: WriterId) -> Option<Self> {
        let snapshot = self
            .runtime_snapshot
            .as_ref()
            .filter(|snapshot| snapshot.capture.observer == observer);
        let mut packet = if self.capture.observer == observer {
            self.clone()
        } else {
            Self::new(snapshot?.capture)
        };
        packet.runtime_snapshot = snapshot.cloned();
        (!packet.is_empty()).then_some(packet)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "observation_type",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ObservationRecord {
    Llm {
        metadata: LlmObservability,
    },
    CircuitBreakerSummary {
        #[serde(skip_serializing_if = "Option::is_none")]
        effect_type: Option<String>,
        window_duration_s: u64,
        requests_processed: u64,
        requests_rejected: u64,
        observed_state: CircuitState,
        consecutive_failures: usize,
        rejection_rate: f64,
        successes_total: u64,
        failures_total: u64,
        opened_total: u64,
        time_in_closed_seconds: f64,
        time_in_open_seconds: f64,
        time_in_half_open_seconds: f64,
    },
    RateLimiterActivity {
        #[serde(skip_serializing_if = "Option::is_none")]
        effect_type: Option<String>,
        window_ms: u64,
        delayed_events: u64,
        delay_ms_total: u64,
        delay_ms_max: u64,
        limit_rate: f64,
    },
    RateLimiterUtilisation {
        #[serde(skip_serializing_if = "Option::is_none")]
        effect_type: Option<String>,
        utilization_percent: f64,
        events_in_window: u64,
        window_size_ms: u64,
    },
    BackpressureActivity {
        window_ms: u64,
        delayed_events: u64,
        delay_ms_total: u64,
        delay_ms_max: u64,
        min_credit: Option<u64>,
        limiting_downstream_stage_id: Option<StageId>,
    },
    ResourceUsage {
        cpu_percent: f64,
        memory_bytes: u64,
        thread_count: Option<u32>,
    },
    HttpPull(self::http::HttpPullMeasurements),
    AiChunkingWork {
        rerender_attempts_total: u64,
        max_decomposition_depth_reached: u32,
        budget_overhead_tokens: u64,
        excluded_items: Vec<usize>,
    },
    StageHeartbeat {
        activity: StageActivity,
        handler_blocked_ms: Option<DurationMs>,
        last_consumed_event_id: Option<EventId>,
        last_output_event_id: Option<EventId>,
    },
    EdgeLiveness {
        upstream: StageId,
        reader: StageId,
        state: EdgeLivenessState,
        idle_ms: DurationMs,
        last_reader_seq: Option<super::types::SeqNo>,
        last_event_id: Option<EventId>,
    },
    HttpSurface {
        snapshot: self::http::HttpSurfaceMetricsSnapshot,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StageActivity {
    /// Supervisor is polling for events (dispatch loop is running normally).
    Polling,
    /// Handler is processing an event.
    Processing {
        event_id: EventId,
        elapsed_ms: DurationMs,
    },
    /// The canonical deterministic merge is waiting on a quiet input
    /// (FLOWIP-095d). This is idle-by-rule, never hung: an ordered fan-in
    /// delivers nothing while any non-exhausted input has no head. `upstream`
    /// names an input being waited on so operators can debug rate coupling.
    WaitingOnQuietInput { upstream: Option<StageId> },
    /// Stage is draining.
    Draining,
    /// Stage has completed.
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeLivenessState {
    /// Edge is healthy: data is flowing or the edge is idle within expected bounds.
    Healthy,
    /// Edge is idle: no data observed recently, but the stage is alive.
    Idle,
    /// Edge is suspect: no data and no heartbeat response within the warning threshold.
    Suspect,
    /// Edge appears stalled: handler may be hung or upstream may be down.
    Stalled,
    /// Edge has recovered from a previous non-healthy state.
    Recovered,
}

/// Offering a sample has no journal, acknowledgement, or publication capability.
pub trait ObservationSink: Send + Sync {
    fn offer(&self, observation: ObservabilityContext) -> ObservationOffer;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationOffer {
    Accepted,
    Dropped,
}

/// A runtime-owner capability installed on existing handler/middleware seams.
/// It stamps and offers optional evidence without publication or acknowledgement.
pub trait ObservationRecorder: Send + Sync + std::fmt::Debug {
    fn observe(&self, record: ObservationRecord);
    fn observe_with_reason(&self, record: ObservationRecord, _reason: CaptureReason) {
        self.observe(record);
    }
}

#[derive(Debug, Default)]
pub struct NoObservations;
impl ObservationRecorder for NoObservations {
    fn observe(&self, _record: ObservationRecord) {}
}

pub trait ObservationSource: Send + Sync {
    fn active_scope(&self) -> Option<CaptureScope> {
        None
    }
    fn snapshot(&self) -> Vec<ObservabilityContext>;
}

pub mod families;
pub mod http;
pub mod measurement_snapshots;
pub mod runtime_observability;
pub mod runtime_snapshot;

pub use families::{observation_families, ObservationKind};
pub use http::*;
pub use measurement_snapshots::{MetricsSnapshot, SliSnapshot};
pub use runtime_observability::{
    CircuitBreakerMeasurements, EffectCircuitBreakerContext, EffectRateLimiterContext,
    MeasurementWindow, RateLimiterMeasurements, RuntimeObservability, TimingMeasurements,
};
pub use runtime_snapshot::{ExecutionProgress, RuntimeSnapshot};
