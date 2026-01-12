use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Stable rejection reasons for HTTP ingestion telemetry (FLOWIP-084d).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestionRejectionReason {
    Auth,
    Validation,
    BufferFull,
    NotReady,
    PayloadTooLarge,
    InvalidJson,
    ChannelClosed,
}

impl IngestionRejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Validation => "validation",
            Self::BufferFull => "buffer_full",
            Self::NotReady => "not_ready",
            Self::PayloadTooLarge => "payload_too_large",
            Self::InvalidJson => "invalid_json",
            Self::ChannelClosed => "channel_closed",
        }
    }
}

/// Snapshot of HTTP ingestion telemetry emitted via wide events (FLOWIP-084d).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestionTelemetrySnapshot {
    pub base_path: String,
    pub channel_depth: usize,
    pub channel_capacity: usize,

    pub requests_total: u64,
    pub events_accepted_total: u64,
    pub events_rejected_auth_total: u64,
    pub events_rejected_validation_total: u64,
    pub events_rejected_buffer_full_total: u64,
    pub events_rejected_not_ready_total: u64,
    pub events_rejected_payload_too_large_total: u64,
    pub events_rejected_invalid_json_total: u64,
    pub events_rejected_channel_closed_total: u64,
}

impl IngestionTelemetrySnapshot {
    pub fn is_zero(&self) -> bool {
        self.requests_total == 0
            && self.events_accepted_total == 0
            && self.events_rejected_auth_total == 0
            && self.events_rejected_validation_total == 0
            && self.events_rejected_buffer_full_total == 0
            && self.events_rejected_not_ready_total == 0
            && self.events_rejected_payload_too_large_total == 0
            && self.events_rejected_invalid_json_total == 0
            && self.events_rejected_channel_closed_total == 0
            && self.channel_depth == 0
    }
}

/// Shared, thread-safe ingestion telemetry updated by HTTP endpoints (FLOWIP-084d).
///
/// This type intentionally stores only atomics plus a channel-depth function to
/// avoid pulling async runtime dependencies into the core crate.
pub struct IngestionTelemetry {
    base_path: String,
    channel_capacity: usize,
    channel_depth: Arc<dyn Fn() -> usize + Send + Sync>,

    // Monotonic counters (Prometheus-style).
    requests_total: AtomicU64,
    events_accepted_total: AtomicU64,
    events_rejected_auth_total: AtomicU64,
    events_rejected_validation_total: AtomicU64,
    events_rejected_buffer_full_total: AtomicU64,
    events_rejected_not_ready_total: AtomicU64,
    events_rejected_payload_too_large_total: AtomicU64,
    events_rejected_invalid_json_total: AtomicU64,
    events_rejected_channel_closed_total: AtomicU64,
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
            requests_total: AtomicU64::new(0),
            events_accepted_total: AtomicU64::new(0),
            events_rejected_auth_total: AtomicU64::new(0),
            events_rejected_validation_total: AtomicU64::new(0),
            events_rejected_buffer_full_total: AtomicU64::new(0),
            events_rejected_not_ready_total: AtomicU64::new(0),
            events_rejected_payload_too_large_total: AtomicU64::new(0),
            events_rejected_invalid_json_total: AtomicU64::new(0),
            events_rejected_channel_closed_total: AtomicU64::new(0),
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

    pub fn observe_request(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_accepted(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.events_accepted_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn observe_rejected(&self, reason: IngestionRejectionReason, count: usize) {
        if count == 0 {
            return;
        }
        let value = count as u64;
        match reason {
            IngestionRejectionReason::Auth => {
                self.events_rejected_auth_total
                    .fetch_add(value, Ordering::Relaxed);
            }
            IngestionRejectionReason::Validation => {
                self.events_rejected_validation_total
                    .fetch_add(value, Ordering::Relaxed);
            }
            IngestionRejectionReason::BufferFull => {
                self.events_rejected_buffer_full_total
                    .fetch_add(value, Ordering::Relaxed);
            }
            IngestionRejectionReason::NotReady => {
                self.events_rejected_not_ready_total
                    .fetch_add(value, Ordering::Relaxed);
            }
            IngestionRejectionReason::PayloadTooLarge => {
                self.events_rejected_payload_too_large_total
                    .fetch_add(value, Ordering::Relaxed);
            }
            IngestionRejectionReason::InvalidJson => {
                self.events_rejected_invalid_json_total
                    .fetch_add(value, Ordering::Relaxed);
            }
            IngestionRejectionReason::ChannelClosed => {
                self.events_rejected_channel_closed_total
                    .fetch_add(value, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> IngestionTelemetrySnapshot {
        IngestionTelemetrySnapshot {
            base_path: self.base_path.clone(),
            channel_depth: self.channel_depth(),
            channel_capacity: self.channel_capacity,
            requests_total: self.requests_total.load(Ordering::Relaxed),
            events_accepted_total: self.events_accepted_total.load(Ordering::Relaxed),
            events_rejected_auth_total: self.events_rejected_auth_total.load(Ordering::Relaxed),
            events_rejected_validation_total: self
                .events_rejected_validation_total
                .load(Ordering::Relaxed),
            events_rejected_buffer_full_total: self
                .events_rejected_buffer_full_total
                .load(Ordering::Relaxed),
            events_rejected_not_ready_total: self
                .events_rejected_not_ready_total
                .load(Ordering::Relaxed),
            events_rejected_payload_too_large_total: self
                .events_rejected_payload_too_large_total
                .load(Ordering::Relaxed),
            events_rejected_invalid_json_total: self
                .events_rejected_invalid_json_total
                .load(Ordering::Relaxed),
            events_rejected_channel_closed_total: self
                .events_rejected_channel_closed_total
                .load(Ordering::Relaxed),
        }
    }
}
