// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::contract::{
    diagnostic_message, retry_after_secs, IngressErrorCode, BUFFER_FULL, LISTENER_UNAVAILABLE,
    NOT_READY, OVERLOADED, RATE_LIMITED, SHUTTING_DOWN,
};
use super::settlement::{Settlement, SettlementMember};
use super::validation::{validate_submission, ValidationConfig};
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SystemPayload};
use obzenflow_core::ingress::{
    EdgeShedReason, EventSubmission, HostedIngressBindingSlot, IngressAdmissionDecision,
    IngressAdmissionOutcome, IngressAttemptContext, IngressAttemptSeq, IngressBoundaryMiddleware,
    IngressContentDigest, IngressContext, IngressKey, IngressOriginal, IngressRefusalReason,
    IngressRequestId, IngressRequestIdentity,
};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::{
    prove_journal_form, HostedSubmission, IngressDecodeError, IngressDecoder, IngressRecord,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use super::AuthConfig;

/// Configuration for event ingestion (FLOWIP-084d).
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Protocol-neutral ingress identity used in accepted-event provenance and
    /// attempt evidence. `None` means "derive from `base_path`" for the
    /// optional hosted HTTP wrapper (FLOWIP-010 §4b: typed key, no
    /// empty-string sentinel).
    pub ingress_key: Option<IngressKey>,

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

    /// Optional schema validation, run in addition to the decoder.
    pub validation: Option<ValidationConfig>,

    /// FLOWIP-115d, FLOWIP-084l: record ingress attempt evidence (refusals and
    /// recognized duplicates) as durable system-journal facts. On by default.
    /// Disabling it is an explicit operational choice that forfeits that
    /// evidence. A flow with no system journal must disable it, otherwise
    /// startup fails.
    pub record_ingress_evidence: bool,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            base_path: "/api/ingest".to_string(),
            ingress_key: None,
            max_batch_size: 1000,
            max_body_size: 1024 * 1024,
            buffer_capacity: 10_000,
            auth: None,
            validation: None,
            record_ingress_evidence: true,
        }
    }
}

type IngressEvidenceWriter = obzenflow_runtime::pipeline::ingress::IngressEvidenceWriter;

/// Attempt evidence could not be written; the protocol answer fails closed.
#[derive(Debug)]
pub(crate) struct EvidenceUnavailable(String);

impl fmt::Display for EvidenceUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Appends ingress attempt evidence through the pipeline's publication owner.
#[derive(Clone)]
pub(crate) struct EvidenceRecorder {
    enabled: bool,
    writer: Arc<OnceLock<IngressEvidenceWriter>>,
    slot: HostedIngressBindingSlot,
}

/// One refused attempt's fact fields.
pub(crate) struct RefusalEvidence<'a> {
    pub reason: IngressRefusalReason,
    pub attempt: &'a IngressAttemptContext,
    pub http_status: u16,
    pub retry_after: Option<Duration>,
    pub diagnostics: Vec<SourceDiagnostic>,
    pub request_id: Option<&'a IngressRequestId>,
}

impl EvidenceRecorder {
    pub(crate) async fn refusal(
        &self,
        evidence: RefusalEvidence<'_>,
    ) -> Result<(), EvidenceUnavailable> {
        if !self.enabled {
            return Ok(());
        }
        let (writer, stage_id, stage_key) = self.target()?;
        let payload = SystemPayload::IngressRefusal {
            ingress_key: self.slot.ingress_key().clone(),
            stage_id,
            stage_key,
            reason: evidence.reason,
            attempt_seq: evidence.attempt.attempt_seq,
            request_count: evidence.attempt.request_count,
            event_count: evidence.attempt.event_count,
            batch_count: evidence.attempt.batch_count,
            http_status: evidence.http_status,
            // Coarse bucket matching the second-granularity `Retry-After`
            // the client receives, so audit and response agree.
            retry_after_ms_bucket: evidence
                .retry_after
                .map(|delay| retry_after_secs(delay).saturating_mul(1000)),
            diagnostics: evidence.diagnostics,
            request_id: evidence.request_id.cloned(),
        };
        self.append(writer, payload).await
    }

    pub(crate) async fn duplicate(
        &self,
        attempt_seq: IngressAttemptSeq,
        request_id: &IngressRequestId,
        originals: Vec<IngressOriginal>,
    ) -> Result<(), EvidenceUnavailable> {
        if !self.enabled {
            return Ok(());
        }
        let (writer, stage_id, stage_key) = self.target()?;
        let payload = SystemPayload::IngressDuplicateRecognized {
            ingress_key: self.slot.ingress_key().clone(),
            stage_id,
            stage_key,
            attempt_seq,
            request_id: request_id.clone(),
            originals,
        };
        self.append(writer, payload).await
    }

    fn target(
        &self,
    ) -> Result<
        (
            &IngressEvidenceWriter,
            obzenflow_core::StageId,
            obzenflow_core::StageKey,
        ),
        EvidenceUnavailable,
    > {
        let key = self.slot.ingress_key();
        let writer = self.writer.get().ok_or_else(|| {
            EvidenceUnavailable(format!(
                "ingress evidence recording is enabled for '{key}' but no system-journal writer \
                 is installed"
            ))
        })?;
        let filled = self.slot.filled().ok_or_else(|| {
            EvidenceUnavailable(format!(
                "ingress evidence recording is enabled for '{key}' but the hosted ingress slot \
                 is not filled"
            ))
        })?;
        Ok((writer, filled.stage_id, filled.stage_key.clone()))
    }

    async fn append(
        &self,
        writer: &IngressEvidenceWriter,
        payload: SystemPayload,
    ) -> Result<(), EvidenceUnavailable> {
        writer.record(payload).await.map_err(|error| {
            tracing::warn!(
                ingress_key = %self.slot.ingress_key(),
                error = %error,
                "failed to append ingress evidence; answering listener unavailable"
            );
            EvidenceUnavailable(format!(
                "failed to append ingress evidence for '{}': {error}",
                self.slot.ingress_key()
            ))
        })
    }
}

/// A member refused by admission, with the decoder's report for the sender.
#[derive(Debug, Clone)]
pub(crate) struct MemberRefusal {
    pub diagnostic: Box<SourceDiagnostic>,
    pub message: String,
}

impl MemberRefusal {
    fn new(diagnostic: SourceDiagnostic, message: String) -> Self {
        Self {
            diagnostic: Box::new(diagnostic),
            message,
        }
    }

    fn decode(error: IngressDecodeError) -> Self {
        let diagnostic = error.into_diagnostic();
        let message = diagnostic_message(&diagnostic);
        Self::new(diagnostic, message)
    }
}

/// A decoded, proven value and the identity committed with its row.
pub(crate) struct AdmittedValue<T> {
    value: T,
    request: Option<IngressRequestIdentity>,
}

/// Admit one protocol submission: event type, configured validator, the
/// application decoder exactly once, then the journal-form proof (B4).
pub(crate) fn admit_submission<D: IngressDecoder>(
    decoder: &D,
    validation: Option<&ValidationConfig>,
    submission: &EventSubmission,
    request_id: Option<&IngressRequestId>,
) -> Result<AdmittedValue<D::Output>, MemberRefusal> {
    if !D::Output::matches_event_type(submission.event_type.as_str()) {
        let error = super::validation::ValidationError::EventTypeMismatch {
            expected: D::Output::EVENT_TYPE.to_string(),
            actual: submission.event_type.to_string(),
        };
        return Err(MemberRefusal::new(error.diagnostic(), error.to_message()));
    }
    if let Some(validation) = validation {
        validate_submission(submission, validation)
            .map_err(|error| MemberRefusal::new(error.diagnostic(), error.to_message()))?;
    }
    let value = decoder
        .decode(IngressRecord::new(&submission.data))
        .and_then(prove_journal_form)
        .map_err(MemberRefusal::decode)?;
    Ok(AdmittedValue {
        value,
        request: request_id.map(|id| IngressRequestIdentity {
            id: id.clone(),
            digest: IngressContentDigest::of_submission(
                submission.event_type.as_str(),
                &submission.data,
            ),
        }),
    })
}

/// Admit one typed in-process value: digest its serde form, then prove it.
pub(crate) fn admit_typed<T: TypedPayload>(
    value: T,
    request_id: Option<&IngressRequestId>,
) -> Result<AdmittedValue<T>, MemberRefusal> {
    let request = match request_id {
        Some(id) => {
            let form = serde_json::to_value(&value).map_err(|_| {
                let reason = SourceDiagnosticReason::UnrepresentableValue;
                MemberRefusal::new(SourceDiagnostic::new(reason), reason.phrase().to_string())
            })?;
            Some(IngressRequestIdentity {
                id: id.clone(),
                digest: IngressContentDigest::of_typed(form),
            })
        }
        None => None,
    };
    let value = prove_journal_form(value).map_err(MemberRefusal::decode)?;
    Ok(AdmittedValue { value, request })
}

/// One request's admitted members, ready for the edge and the queue.
pub(crate) struct AdmittedRequest<T> {
    pub attempt: IngressAttemptContext,
    pub request_id: Option<IngressRequestId>,
    /// `(position in the request, batch index, value)`.
    pub members: Vec<(usize, Option<usize>, AdmittedValue<T>)>,
}

/// Why the edge refused a whole request after admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeRefusal {
    NotReady,
    BufferFull,
    ShuttingDown,
    RateLimited { retry_after: Option<Duration> },
    Overloaded { retry_after: Option<Duration> },
    EvidenceUnavailable,
}

impl EdgeRefusal {
    pub(crate) fn code(self) -> IngressErrorCode {
        match self {
            Self::NotReady => IngressErrorCode::NotReady,
            Self::BufferFull => IngressErrorCode::BufferFull,
            Self::ShuttingDown => IngressErrorCode::ShuttingDown,
            Self::RateLimited { .. } => IngressErrorCode::RateLimited,
            Self::Overloaded { .. } => IngressErrorCode::Overloaded,
            Self::EvidenceUnavailable => IngressErrorCode::ListenerUnavailable,
        }
    }

    pub(crate) fn status(self) -> u16 {
        match self {
            Self::RateLimited { .. } => 429,
            _ => 503,
        }
    }

    pub(crate) fn retry_after(self) -> Duration {
        match self {
            Self::RateLimited { retry_after } | Self::Overloaded { retry_after } => {
                retry_after.unwrap_or(Duration::from_secs(1))
            }
            _ => Duration::from_secs(1),
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::NotReady => NOT_READY,
            Self::BufferFull => BUFFER_FULL,
            Self::ShuttingDown => SHUTTING_DOWN,
            Self::RateLimited { .. } => RATE_LIMITED,
            Self::Overloaded { .. } => OVERLOADED,
            Self::EvidenceUnavailable => LISTENER_UNAVAILABLE,
        }
    }
}

/// Shared state between ingestion endpoints and the in-process handle.
pub struct IngestionState<T> {
    tx: mpsc::Sender<HostedSubmission<T>>,
    ready: Arc<AtomicBool>,
    buffer_capacity: usize,
    pub(crate) config: IngestionConfig,
    /// FLOWIP-115d: the hosted-ingress binding slot, shared with this surface's
    /// source half. The DSL fills it during source-stage materialization.
    ingress_slot: HostedIngressBindingSlot,
    /// Monotonic per-hosted-ingress submission-attempt sequence.
    attempt_seq: Arc<AtomicU64>,
    evidence: EvidenceRecorder,
}

impl<T> Clone for IngestionState<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            ready: self.ready.clone(),
            buffer_capacity: self.buffer_capacity,
            config: self.config.clone(),
            ingress_slot: self.ingress_slot.clone(),
            attempt_seq: self.attempt_seq.clone(),
            evidence: self.evidence.clone(),
        }
    }
}

impl<T> IngestionState<T> {
    pub fn new(mut config: IngestionConfig) -> (Self, mpsc::Receiver<HostedSubmission<T>>) {
        config.base_path = normalize_base_path(&config.base_path);
        let ingress_key = resolve_ingress_key(config.ingress_key.clone(), &config.base_path);
        let buffer_capacity = config.buffer_capacity;
        let (tx, rx) = mpsc::channel(buffer_capacity);
        let ingress_slot = HostedIngressBindingSlot::new(ingress_key);
        let evidence = EvidenceRecorder {
            enabled: config.record_ingress_evidence,
            writer: Arc::new(OnceLock::new()),
            slot: ingress_slot.clone(),
        };
        let state = Self {
            tx,
            ready: Arc::new(AtomicBool::new(false)),
            buffer_capacity,
            config,
            ingress_slot,
            attempt_seq: Arc::new(AtomicU64::new(0)),
            evidence,
        };
        (state, rx)
    }

    /// FLOWIP-115d: the hosted-ingress binding slot, shared with the source half.
    pub fn ingress_slot(&self) -> HostedIngressBindingSlot {
        self.ingress_slot.clone()
    }

    /// The resolved protocol-neutral ingress identity (configured or
    /// derived from the base path).
    pub fn ingress_key(&self) -> &IngressKey {
        self.ingress_slot.ingress_key()
    }

    /// The composed ingress admission boundary, once the DSL has filled the slot.
    pub(crate) fn ingress_boundary(&self) -> Option<Arc<dyn IngressBoundaryMiddleware>> {
        self.ingress_slot.filled().and_then(|f| f.boundary.clone())
    }

    /// Allocate the next monotonic per-hosted-ingress submission-attempt sequence.
    pub(crate) fn next_attempt_seq(&self) -> IngressAttemptSeq {
        IngressAttemptSeq(self.attempt_seq.fetch_add(1, Ordering::Relaxed))
    }

    /// Whether this surface records attempt evidence. Startup uses this to
    /// require a system journal when recording is on.
    pub(crate) fn evidence_recording_enabled(&self) -> bool {
        self.config.record_ingress_evidence
    }

    /// Install the system-journal evidence writer at wiring time; the first
    /// installed writer stays.
    pub(crate) fn install_evidence_writer(&self, writer: IngressEvidenceWriter) {
        if self.evidence.writer.set(writer).is_err() {
            tracing::debug!(
                ingress_key = %self.ingress_key(),
                "ingress evidence writer already installed"
            );
        }
    }

    /// The acknowledgement deadline in force.
    pub(crate) fn ack_deadline(&self) -> Duration {
        self.ingress_slot.timing().ack_deadline()
    }

    /// Current channel depth.
    pub fn channel_depth(&self) -> usize {
        self.buffer_capacity.saturating_sub(self.tx.capacity())
    }

    pub fn channel_capacity(&self) -> usize {
        self.buffer_capacity
    }

    pub fn is_ready(&self) -> bool {
        // FLOWIP-120n F12: during resume catch-up a Running pipeline still
        // refuses ingress until the hosted source crosses to live.
        self.ready.load(Ordering::Acquire) && self.ingress_slot.is_resume_live()
    }

    /// The pipeline-running conjunct of readiness (health diagnostics).
    pub fn pipeline_running(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// The resume-live conjunct of readiness (health diagnostics).
    pub fn resume_live(&self) -> bool {
        self.ingress_slot.is_resume_live()
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

    /// Record a whole-request refusal before admission and answer it.
    pub(crate) async fn refuse_not_ready(
        &self,
        attempt: &IngressAttemptContext,
        request_id: Option<&IngressRequestId>,
    ) -> EdgeRefusal {
        self.record_edge_refusal(EdgeRefusal::NotReady, attempt, request_id)
            .await
    }

    /// Record the validation refusals of one request, one fact for all of
    /// its refused members.
    pub(crate) async fn record_validation_refusals(
        &self,
        attempt_seq: IngressAttemptSeq,
        batch: bool,
        request_id: Option<&IngressRequestId>,
        refused: &[(usize, MemberRefusal)],
    ) -> Result<(), EvidenceUnavailable> {
        let attempt = IngressAttemptContext {
            attempt_seq,
            request_count: 1,
            event_count: refused.len() as u64,
            batch_count: u64::from(batch),
        };
        let diagnostics = refused
            .iter()
            .map(|(position, refusal)| {
                let diagnostic = SourceDiagnostic::clone(&refusal.diagnostic);
                if batch {
                    diagnostic.record(*position as u64)
                } else {
                    diagnostic
                }
            })
            .collect();
        self.evidence
            .refusal(RefusalEvidence {
                reason: IngressRefusalReason::Validation,
                attempt: &attempt,
                http_status: 400,
                retry_after: None,
                diagnostics,
                request_id,
            })
            .await
    }

    /// Reserve capacity, run edge admission, and queue every admitted member
    /// with its receipt. The settlement then waits for each member's commit.
    pub(crate) async fn enqueue(
        &self,
        request: AdmittedRequest<T>,
    ) -> Result<Settlement, EdgeRefusal>
    where
        T: Send + 'static,
    {
        let AdmittedRequest {
            attempt,
            request_id,
            members,
        } = request;
        if !self.is_ready() {
            return Err(self
                .record_edge_refusal(EdgeRefusal::NotReady, &attempt, request_id.as_ref())
                .await);
        }

        let mut permits = Vec::with_capacity(members.len());
        for _ in 0..members.len() {
            match self.tx.try_reserve() {
                Ok(permit) => permits.push(permit),
                Err(TrySendError::Full(())) => {
                    drop(permits);
                    return Err(self
                        .record_edge_refusal(EdgeRefusal::BufferFull, &attempt, request_id.as_ref())
                        .await);
                }
                Err(TrySendError::Closed(())) => {
                    drop(permits);
                    return Err(self
                        .record_edge_refusal(
                            EdgeRefusal::ShuttingDown,
                            &attempt,
                            request_id.as_ref(),
                        )
                        .await);
                }
            }
        }

        // FLOWIP-115d: the edge reserved capacity first, so buffer-full had
        // precedence; admission is fail-fast and never waits for a token.
        let accepted_boundary = match self.ingress_boundary() {
            Some(boundary) => match boundary.on_ingress(&attempt) {
                IngressAdmissionDecision::Accept => Some(boundary),
                IngressAdmissionDecision::Reject { retry_after } => {
                    boundary.observe(&attempt, IngressAdmissionOutcome::RejectedBy);
                    drop(permits);
                    return Err(self
                        .record_edge_refusal(
                            EdgeRefusal::RateLimited { retry_after },
                            &attempt,
                            request_id.as_ref(),
                        )
                        .await);
                }
                IngressAdmissionDecision::Shed {
                    reason,
                    retry_after,
                } => {
                    boundary.observe(&attempt, IngressAdmissionOutcome::ShedBy);
                    drop(permits);
                    let refusal = match reason {
                        EdgeShedReason::NotReady => EdgeRefusal::NotReady,
                        EdgeShedReason::BufferFull => EdgeRefusal::BufferFull,
                        EdgeShedReason::ChannelClosed => EdgeRefusal::ShuttingDown,
                        EdgeShedReason::ListenerOverloaded => {
                            EdgeRefusal::Overloaded { retry_after }
                        }
                        EdgeShedReason::EvidenceUnavailable => EdgeRefusal::EvidenceUnavailable,
                    };
                    return Err(self
                        .record_edge_refusal(refusal, &attempt, request_id.as_ref())
                        .await);
                }
            },
            None => None,
        };

        let accepted_at_ns = unix_now_nanos();
        let mut waiting = Vec::with_capacity(members.len());
        for (permit, (position, batch_index, admitted)) in permits.into_iter().zip(members) {
            let (receipt, answer) = oneshot::channel();
            let context = IngressContext {
                accepted_at_ns,
                ingress_key: self.ingress_key().clone(),
                batch_index,
                attempt_seq: attempt.attempt_seq,
                request: admitted.request,
            };
            permit.send(HostedSubmission::new(admitted.value, context, receipt));
            waiting.push(SettlementMember {
                position,
                batch_index,
                answer,
            });
        }
        if let Some(boundary) = accepted_boundary {
            boundary.observe(&attempt, IngressAdmissionOutcome::AcceptedForEnqueue);
        }
        Ok(Settlement::spawn(
            waiting,
            self.evidence.clone(),
            attempt,
            request_id,
        ))
    }

    /// Record one whole-request edge refusal; a failed append fails closed.
    async fn record_edge_refusal(
        &self,
        refusal: EdgeRefusal,
        attempt: &IngressAttemptContext,
        request_id: Option<&IngressRequestId>,
    ) -> EdgeRefusal {
        let reason = match refusal {
            EdgeRefusal::NotReady => IngressRefusalReason::NotReady,
            EdgeRefusal::BufferFull => IngressRefusalReason::BufferFull,
            EdgeRefusal::ShuttingDown => IngressRefusalReason::ChannelClosed,
            EdgeRefusal::RateLimited { .. } => IngressRefusalReason::RateLimited,
            EdgeRefusal::Overloaded { .. } => IngressRefusalReason::ListenerOverloaded,
            // A failed append cannot itself be journalled.
            EdgeRefusal::EvidenceUnavailable => return refusal,
        };
        let recorded = self
            .evidence
            .refusal(RefusalEvidence {
                reason,
                attempt,
                http_status: refusal.status(),
                retry_after: Some(refusal.retry_after()),
                diagnostics: Vec::new(),
                request_id,
            })
            .await;
        match recorded {
            Ok(()) => refusal,
            Err(_) => EdgeRefusal::EvidenceUnavailable,
        }
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

fn resolve_ingress_key(ingress_key: Option<IngressKey>, fallback_base_path: &str) -> IngressKey {
    match ingress_key {
        Some(key) if !key.as_str().trim().is_empty() => IngressKey(key.as_str().trim().to_string()),
        _ => IngressKey(fallback_base_path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Probe {
        value: u32,
    }

    impl TypedPayload for Probe {
        const EVENT_TYPE: &'static str = "probe";
    }

    #[tokio::test]
    async fn watch_pipeline_state_toggles_ready_on_running() {
        let (state, _) = IngestionState::<Probe>::new(IngestionConfig::default());
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
