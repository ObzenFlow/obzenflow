// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::ingestion::EventSubmission;
use obzenflow_core::event::ingestion::IngestionTelemetry;
use obzenflow_core::event::{SystemEvent, SystemEventType, WriterId};
use obzenflow_core::id::SystemId;
use obzenflow_core::ingress::{
    HostedIngressBindingSlot, IngressAttemptContext, IngressAttemptSeq, IngressBoundaryMiddleware,
    IngressRefusalReason,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::web::{ManagedResponse, Response, WebError};
use obzenflow_runtime::pipeline::PipelineState;
use serde_json::json;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

use super::{AuthConfig, ValidationConfig};

/// Configuration for event ingestion (FLOWIP-084d).
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Base path for endpoints (default: `/api/ingest`).
    ///
    /// Results in:
    /// - `{base_path}/events`
    /// - `{base_path}/batch`
    /// - `{base_path}/health`
    pub base_path: String,

    /// Maximum events per batch (default: 1000).
    pub max_batch_size: usize,

    /// Maximum request body size in bytes (default: 1MB).
    pub max_body_size: usize,

    /// Channel buffer capacity (default: 10,000).
    pub buffer_capacity: usize,

    /// Optional authentication.
    pub auth: Option<AuthConfig>,

    /// Optional schema validation.
    pub validation: Option<ValidationConfig>,

    /// FLOWIP-115d: record hosted-ingress reject/shed attempts (and per-event
    /// validation rejections) as durable `IngressRefusal` facts on the system
    /// journal; telemetry projects the refusal count from those facts. On by
    /// default. Disabling it is an explicit operational choice that forfeits
    /// replayable refusal evidence. A flow with no system journal must disable
    /// it, otherwise startup fails.
    pub record_ingress_refusals: bool,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            base_path: "/api/ingest".to_string(),
            max_batch_size: 1000,
            max_body_size: 1024 * 1024,
            buffer_capacity: 10_000,
            auth: None,
            validation: None,
            record_ingress_refusals: true,
        }
    }
}

/// FLOWIP-115d: the system-journal writer that records hosted-ingress refusal
/// facts. Installed at web-surface wiring time from the host system journal.
struct IngressRefusalWriter {
    journal: Arc<dyn Journal<SystemEvent>>,
    writer_id: WriterId,
}

#[derive(Debug)]
pub(crate) struct IngressRefusalRecordError {
    message: String,
}

impl IngressRefusalRecordError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for IngressRefusalRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for IngressRefusalRecordError {}

/// Shared state between ingestion endpoints.
#[derive(Clone)]
pub struct IngestionState {
    pub tx: mpsc::Sender<EventSubmission>,
    pub ready: Arc<AtomicBool>,
    pub buffer_capacity: usize,
    pub config: IngestionConfig,
    pub telemetry: Arc<IngestionTelemetry>,
    /// FLOWIP-115d: the hosted-ingress binding slot, shared with this surface's
    /// source half. The DSL fills it during source-stage materialization; the
    /// endpoints read the composed admission boundary from it at request time.
    ingress_slot: HostedIngressBindingSlot,
    /// Monotonic per-hosted-ingress submission-attempt sequence.
    attempt_seq: Arc<AtomicU64>,
    /// FLOWIP-115d: write-once installer for the refusal-fact writer, shared with
    /// the endpoint clones. `FlowApplication` web-surface wiring fills it from the
    /// host system journal; a rejected, shed, or validation-refused attempt
    /// appends one `IngressRefusal` fact through it. Absent means refusal
    /// recording is disabled.
    refusal_writer: Arc<OnceLock<IngressRefusalWriter>>,
}

impl IngestionState {
    pub fn new(mut config: IngestionConfig) -> (Self, mpsc::Receiver<EventSubmission>) {
        config.base_path = normalize_base_path(&config.base_path);

        let buffer_capacity = config.buffer_capacity;
        let (tx, rx) = mpsc::channel(buffer_capacity);
        let tx_for_depth = tx.clone();
        let capacity_for_depth = buffer_capacity;
        let depth_fn: Arc<dyn Fn() -> usize + Send + Sync> =
            Arc::new(move || capacity_for_depth.saturating_sub(tx_for_depth.capacity()));
        let telemetry = Arc::new(IngestionTelemetry::new(
            config.base_path.clone(),
            buffer_capacity,
            depth_fn,
        ));
        let ingress_slot = HostedIngressBindingSlot::new(config.base_path.clone());
        let state = Self {
            tx,
            ready: Arc::new(AtomicBool::new(false)),
            buffer_capacity,
            config,
            telemetry,
            ingress_slot,
            attempt_seq: Arc::new(AtomicU64::new(0)),
            refusal_writer: Arc::new(OnceLock::new()),
        };
        (state, rx)
    }

    /// FLOWIP-115d: the hosted-ingress binding slot, shared with the source half.
    pub fn ingress_slot(&self) -> HostedIngressBindingSlot {
        self.ingress_slot.clone()
    }

    /// The composed ingress admission boundary, once the DSL has filled the slot.
    pub fn ingress_boundary(&self) -> Option<Arc<dyn IngressBoundaryMiddleware>> {
        self.ingress_slot.filled().and_then(|f| f.boundary.clone())
    }

    /// Allocate the next monotonic per-hosted-ingress submission-attempt sequence.
    pub fn next_attempt_seq(&self) -> IngressAttemptSeq {
        IngressAttemptSeq(self.attempt_seq.fetch_add(1, Ordering::Relaxed))
    }

    /// Whether this surface is configured to record refusal facts (FLOWIP-115d).
    /// Startup uses this to require a system journal when recording is on.
    pub(crate) fn refusal_recording_enabled(&self) -> bool {
        self.config.record_ingress_refusals
    }

    /// Install the system-journal refusal-fact writer at web-surface wiring time.
    pub(crate) fn install_refusal_writer(&self, journal: Arc<dyn Journal<SystemEvent>>) {
        let _ = self.refusal_writer.set(IngressRefusalWriter {
            journal,
            writer_id: WriterId::from(SystemId::new()),
        });
    }

    /// Append one durable `IngressRefusal` fact for a refused attempt
    /// (FLOWIP-115d). No-op only when refusal recording is explicitly disabled.
    /// When recording is enabled, missing writer/slot wiring or append failure is
    /// evidence unavailability and callers must fail closed before returning a
    /// protocol refusal. The projected refusal metric is a fold of these facts, so
    /// this is the only refusal record kept.
    ///
    /// `attempt.event_count` is the number of events this fact refuses (the whole
    /// rate-limited or shed subset, or the validation-rejected count), which is
    /// the value carried onto the fact.
    pub(crate) async fn record_refusal(
        &self,
        reason: IngressRefusalReason,
        attempt: &IngressAttemptContext,
        http_status: u16,
        retry_after: Option<Duration>,
    ) -> Result<(), IngressRefusalRecordError> {
        if !self.config.record_ingress_refusals {
            return Ok(());
        }

        let Some(writer) = self.refusal_writer.get() else {
            return Err(IngressRefusalRecordError::new(format!(
                "ingress refusal recording is enabled for '{}' but no system-journal writer is installed",
                self.config.base_path
            )));
        };
        let Some(filled) = self.ingress_slot.filled() else {
            return Err(IngressRefusalRecordError::new(format!(
                "ingress refusal recording is enabled for '{}' but the hosted ingress slot is not filled",
                self.config.base_path
            )));
        };
        let event = SystemEvent::new(
            writer.writer_id,
            SystemEventType::IngressRefusal {
                base_path: self.config.base_path.clone(),
                stage_id: filled.stage_id,
                stage_key: filled.stage_key.clone(),
                reason,
                attempt_seq: attempt.attempt_seq,
                request_count: attempt.request_count,
                event_count: attempt.event_count,
                batch_count: attempt.batch_count,
                http_status,
                // Coarse bucket matching the second-granularity `Retry-After`
                // the client receives, so audit and response agree.
                retry_after_ms_bucket: retry_after.map(|d| d.as_secs().max(1).saturating_mul(1000)),
            },
        );
        writer
            .journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| {
                IngressRefusalRecordError::new(format!(
                    "failed to append ingress refusal fact for '{}': {e}",
                    self.config.base_path
                ))
            })
    }

    /// Record refusal evidence or return the fail-closed listener-unavailable
    /// response required by FLOWIP-115d when evidence cannot be written.
    pub(crate) async fn record_refusal_or_unavailable(
        &self,
        reason: IngressRefusalReason,
        attempt: &IngressAttemptContext,
        http_status: u16,
        retry_after: Option<Duration>,
    ) -> Result<Option<ManagedResponse>, WebError> {
        match self
            .record_refusal(reason, attempt, http_status, retry_after)
            .await
        {
            Ok(()) => Ok(None),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to append ingress refusal fact; returning listener unavailable"
                );
                let response = Response::new(503)
                    .with_header("Retry-After".to_string(), "1".to_string())
                    .with_json(&json!({"error": "listener unavailable"}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                Ok(Some(response.into()))
            }
        }
    }

    /// Current channel depth.
    ///
    /// Uses `mpsc::Sender::capacity()` which returns remaining capacity.
    pub fn channel_depth(&self) -> usize {
        self.buffer_capacity.saturating_sub(self.tx.capacity())
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn telemetry(&self) -> Arc<IngestionTelemetry> {
        self.telemetry.clone()
    }

    /// Wire ready signal from `FlowHandle::state_receiver()`.
    ///
    /// Returns a join handle that must be kept alive.
    pub fn watch_pipeline_state(
        &self,
        mut state_rx: watch::Receiver<PipelineState>,
    ) -> tokio::task::JoinHandle<()> {
        let ready = self.ready.clone();
        tokio::spawn(async move {
            let initial_running = matches!(state_rx.borrow().clone(), PipelineState::Running);
            ready.store(initial_running, Ordering::Release);
            loop {
                if state_rx.changed().await.is_err() {
                    break;
                }
                let state = state_rx.borrow().clone();
                let is_running = matches!(state, PipelineState::Running);
                ready.store(is_running, Ordering::Release);
            }
        })
    }
}

pub(crate) fn join_path(base_path: &str, suffix: &str) -> String {
    let base_path = normalize_base_path(base_path);
    format!("{base_path}/{suffix}")
}

pub(crate) fn unix_now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn normalize_base_path(base_path: &str) -> String {
    let trimmed = base_path.trim();
    if trimmed.is_empty() {
        return "/api/ingest".to_string();
    }

    let mut out = if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    };

    while out.len() > 1 && out.ends_with('/') {
        out.pop();
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn watch_pipeline_state_toggles_ready_on_running() {
        let (state, _rx) = IngestionState::new(IngestionConfig::default());
        assert!(!state.is_ready());

        let (tx, rx) = watch::channel(PipelineState::Created);
        let handle = state.watch_pipeline_state(rx);

        tx.send(PipelineState::Running).unwrap();
        tokio::task::yield_now().await;
        assert!(state.is_ready());

        tx.send(PipelineState::Draining).unwrap();
        tokio::task::yield_now().await;
        assert!(!state.is_ready());

        handle.abort();
    }
}
