// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared stage-authored output commit helper (FLOWIP-120b).
//!
//! `OutputCommitter` centralizes the commit core for the stage-authored output
//! paths that have been migrated to it: the supervisor pending-output drain,
//! the immediate `fx.emit` path for typed derived facts, domain effect outcome
//! facts, and the effects-layer reserved framework effect/capture record append.
//! The shared core includes wide-event enrichment, per-type instrumentation,
//! journal append, heartbeat tracking, and the middleware mirror.
//!
//! This is not yet a type-system-enforced journal write boundary. Stage
//! contexts still expose raw journal handles for control, error, delivery, and
//! compatibility paths, so future code can still append directly. Treat this as
//! a consolidation helper until stage data journals are wrapped in
//! intention-specific writer types.
//!
//! The caller still owns the decisions this committer does not yet absorb:
//!
//! * Backpressure credit and requeue stay in the drain for legacy returned
//!   handler outputs. Direct data facts use track-only physical-row accounting
//!   around their durable append; stronger direct-fact admission remains a
//!   later 120b slice.
//! * The pending-output drain still calls validation before credit reservation,
//!   preserving the fail-before-backpressure-ordering rule. The validation rule
//!   itself now lives here so every committer-based authoring path has one
//!   contract check.
//! * Deterministic output identity is assigned before commit by the authoring
//!   surface. `fx.emit` already uses a per-input output ordinal; returned
//!   handler outputs and reserved framework effect records still arrive
//!   prebuilt.
//!
//! Behaviour is selected by which handles are present and by [`CommitOptions`].
//! The drain and `fx.emit` supply the stage handles they have; the reserved
//! framework effect/capture record path supplies only the journal, preserving
//! its compatibility append until typed outcome facts replace it.

use std::sync::Arc;

use crate::stages::observer::{ObserverReport, StageObserverBundle};
use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope, StageType};
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::CorrelationId;
use obzenflow_core::event::{ChainEventContent, EventEnvelope, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::ChainEvent;

use crate::backpressure::{BackpressureReservation, BackpressureWriter, DirectFactClaim};
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal;
use crate::stages::observer::dispatch::run_output_commit_observers;

fn output_contract_summary(output_contract: &StageOutputContract) -> String {
    output_contract
        .outputs
        .iter()
        .map(|output| {
            format!(
                "{} event_type={} schema_version={:?} visibility={:?}",
                output.payload_key(),
                output.event_type.as_deref().unwrap_or("<none>"),
                output.schema_version,
                output.visibility,
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A boxed, thread-safe error from a commit attempt. Each caller maps this onto
/// its own error type: the drain prefixes it with `Failed to write pending
/// output`, and the effects layer wraps it in `EffectError::Journal`.
pub(crate) type CommitError = Box<dyn std::error::Error + Send + Sync>;

enum PhysicalDataReservation {
    Legacy(BackpressureReservation),
    Direct(DirectFactClaim),
    DirectTracked {
        claim: DirectFactClaim,
        reservation: BackpressureReservation,
    },
}

impl PhysicalDataReservation {
    fn commit(self, rows: u64) -> Result<(), CommitError> {
        match self {
            Self::Legacy(reservation) => {
                reservation.commit(rows);
                Ok(())
            }
            Self::Direct(claim) => claim.commit().map_err(Into::into),
            Self::DirectTracked { claim, reservation } => {
                claim
                    .commit()
                    .map_err(|error| -> CommitError { error.into() })?;
                reservation.commit(rows);
                Ok(())
            }
        }
    }
}

fn reserve_direct_data_rows(
    writer: Option<&BackpressureWriter>,
    rows: u64,
) -> Result<Option<PhysicalDataReservation>, CommitError> {
    if rows == 0 {
        return Ok(None);
    }
    let Some(writer) = writer else {
        return Ok(None);
    };
    if let Some(admission) = writer.direct_fact_admission() {
        if let Some(claim) = admission.claim(rows)? {
            if claim.requires_track_accounting() {
                return Ok(Some(PhysicalDataReservation::DirectTracked {
                    claim,
                    reservation: writer.reserve_tracked(rows),
                }));
            }
            return Ok(Some(PhysicalDataReservation::Direct(claim)));
        }
    }
    Ok(Some(PhysicalDataReservation::Legacy(
        writer.reserve_tracked(rows),
    )))
}

/// Per-commit behaviour that is not implied by which handles are present.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CommitOptions {
    /// Count this event in per-type producer instrumentation through
    /// `record_output_event`. Only the drain's data branch sets this. The
    /// drain's non-data branch and the unrouted effect-record path leave it
    /// `false`, matching their behaviour before Step 1.
    pub count_output: bool,
    /// Validate `Data` event types against the stage output contract before
    /// committing. The pending-output drain enables this and runs the same
    /// validation before credit reservation. Framework-owned effect/capture
    /// compatibility facts leave it disabled until typed domain outcome facts
    /// replace that compatibility path.
    pub validate_output_contract: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MirrorPolicy {
    None,
    FrameworkMiddlewareAllowlist,
}

/// The kind of stage-runtime journal append, used to gate the
/// `before_output_commit` observer hook and the framework system-journal mirror.
///
/// Only the four wired variants exist. Other stage-runtime appends are
/// out-of-surface raw appends today: error-journal and error-routed-data writes,
/// backpressure activity pulses, sink delivery receipts, forwarded sink-boundary
/// control rows, source/stage lifecycle events, ingress refusal facts, and the
/// `fx.emit` / domain-effect-outcome / framework-effect-record facts (which flow
/// through `NonDataStageFact`) each append directly to their journal (and mirror
/// directly where applicable) rather than through this seam. Routing them through
/// named intents is deferred to a committer-consolidation slice (FLOWIP-120b);
/// it is not required for the `before_output_commit` boundary, which only the
/// `NormalStageData` path reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageAppendIntent {
    NormalStageData,
    NonDataStageFact,
    FrameworkObservability,
    ObserverDiagnostic,
}

/// One member of a policy-neutral atomic journal group.
pub(crate) struct AtomicCommitEntry {
    pub event: ChainEvent,
    pub options: CommitOptions,
    pub intent: StageAppendIntent,
}

impl StageAppendIntent {
    pub(crate) fn mirror_policy(self) -> MirrorPolicy {
        match self {
            Self::FrameworkObservability => MirrorPolicy::FrameworkMiddlewareAllowlist,
            Self::NormalStageData | Self::NonDataStageFact | Self::ObserverDiagnostic => {
                MirrorPolicy::None
            }
        }
    }

    pub(crate) fn runs_output_commit_hooks(self) -> bool {
        matches!(self, Self::NormalStageData)
    }
}

/// Shared commit path for migrated stage-authored outputs (FLOWIP-120b).
///
/// Holds borrowed handles for the duration of one commit. An absent handle
/// (`None`) skips the corresponding step, which is how the effects layer
/// reproduces its current bare append with only a journal handle.
pub(crate) struct OutputCommitter<'a> {
    /// The stage data journal every stage-authored event is appended to.
    pub data_journal: &'a Arc<dyn Journal<ChainEvent>>,
    /// Wide-event flow context stamped on the committed event. Absent on the
    /// effects-layer path, which does not enrich effect records today.
    pub flow_context: Option<&'a FlowContext>,
    /// System journal for mirroring middleware lifecycle rows. Absent on the
    /// effects-layer path.
    pub system_journal: Option<&'a Arc<dyn Journal<SystemEvent>>>,
    /// Stage instrumentation for per-type producer counting and the
    /// runtime-context snapshot. Absent on the effects-layer path.
    pub instrumentation: Option<&'a Arc<StageInstrumentation>>,
    /// Heartbeat state, updated with the last committed output id. Absent on the
    /// effects-layer path.
    pub heartbeat_state: Option<&'a Arc<HeartbeatState>>,
    /// Runtime output contract for stage-authored domain `Data` facts. Absent
    /// on reserved framework append paths and legacy callers.
    pub output_contract: Option<&'a StageOutputContract>,
    /// Physical-row accounting for direct `Data` facts. Pending-output drains
    /// leave this absent because they own an enforced or replay-scoped
    /// reservation outside the commit helper.
    pub backpressure_writer: Option<&'a BackpressureWriter>,
    /// Observer bundle for stage output commit hooks.
    pub observers: Option<&'a StageObserverBundle>,
    /// Per-event observer execution scope for replay suppression.
    pub observer_scope: MiddlewareExecutionScope,
}

impl OutputCommitter<'_> {
    /// Commit a fully-constructed event to the data journal.
    ///
    /// The event must already carry its content, identity, and any
    /// effect-provenance or error status its author set. This applies the shared
    /// commit core in the same order the drain used before Step 1: flow-context
    /// enrichment, optional per-type counting, runtime-context enrichment,
    /// journal append, heartbeat tracking, and the middleware mirror. Each step
    /// is gated on the relevant handle, so an effects-layer committer holding
    /// only a journal handle performs a bare append, exactly as
    /// `append_effect_record` did before Step 1.
    pub(crate) async fn commit_prebuilt(
        &self,
        event: ChainEvent,
        parent: Option<&EventEnvelope<ChainEvent>>,
        options: CommitOptions,
    ) -> Result<EventEnvelope<ChainEvent>, CommitError> {
        let intent = if event.is_data() {
            StageAppendIntent::NormalStageData
        } else {
            StageAppendIntent::NonDataStageFact
        };
        self.commit_prebuilt_with_intent(event, parent, options, intent)
            .await
    }

    pub(crate) async fn commit_prebuilt_with_intent(
        &self,
        event: ChainEvent,
        parent: Option<&EventEnvelope<ChainEvent>>,
        options: CommitOptions,
        intent: StageAppendIntent,
    ) -> Result<EventEnvelope<ChainEvent>, CommitError> {
        let event = self
            .prepare_prebuilt_with_intent(event, parent, options, intent)
            .await?;

        // Direct facts are already past input admission, so this path must not
        // wait. It nevertheless records every durable physical Data row. A
        // failed append drops the reservation and releases it.
        let backpressure_reservation =
            reserve_direct_data_rows(self.backpressure_writer, u64::from(event.is_data()))?;

        let written = self
            .data_journal
            .append(event, parent)
            .await
            .map_err(|e| -> CommitError { e.to_string().into() })?;

        if let Some(reservation) = backpressure_reservation {
            reservation.commit(1)?;
        }

        self.finish_committed(&written, options, intent).await;
        Ok(written)
    }

    /// Commit every member through one journal atomic-group primitive. Event
    /// enrichment happens before the append; counters, heartbeat state, and
    /// best-effort system mirrors advance only after the complete group is
    /// visible.
    pub(crate) async fn commit_atomic_group(
        &self,
        group_id: &str,
        entries: Vec<AtomicCommitEntry>,
        parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, CommitError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if self.observers.is_some()
            && entries
                .iter()
                .any(|entry| entry.intent.runs_output_commit_hooks())
        {
            return Err("atomic journal groups cannot run output-commit observers before their commit point".into());
        }

        let mut prepared = Vec::with_capacity(entries.len());
        let mut metadata = Vec::with_capacity(entries.len());
        for entry in entries {
            let event = self
                .prepare_prebuilt_with_intent(entry.event, parent, entry.options, entry.intent)
                .await?;
            prepared.push(event);
            metadata.push((entry.options, entry.intent));
        }

        let data_count = prepared.iter().filter(|event| event.is_data()).count() as u64;
        let backpressure_reservation =
            reserve_direct_data_rows(self.backpressure_writer, data_count)?;

        let member_count = metadata.len();
        let written = match self
            .data_journal
            .append_group(group_id, prepared, parent)
            .await
        {
            Ok(written) => written,
            Err(error) => {
                if let Some(instrumentation) = self.instrumentation {
                    instrumentation
                        .terminal_group_commit_failures_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                tracing::error!(
                    group_id,
                    member_count,
                    error = %error,
                    "atomic terminal journal group commit failed"
                );
                return Err(error.to_string().into());
            }
        };

        // A successful Journal call means every prepared member is visible.
        // Account the physical rows before checking the returned-envelope
        // cardinality so a broken Journal implementation cannot leak debt.
        if let Some(reservation) = backpressure_reservation {
            reservation.commit(data_count)?;
        }
        if let Some(instrumentation) = self.instrumentation {
            instrumentation
                .terminal_groups_committed_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if written.len() != metadata.len() {
            tracing::error!(
                group_id,
                member_count,
                returned_envelopes = written.len(),
                "journal violated atomic-group envelope cardinality"
            );
            return Err(format!(
                "atomic journal group '{group_id}' returned {} envelopes for {} members",
                written.len(),
                metadata.len()
            )
            .into());
        }
        for (envelope, (options, intent)) in written.iter().zip(metadata) {
            self.finish_committed(envelope, options, intent).await;
        }
        Ok(written)
    }

    async fn prepare_prebuilt_with_intent(
        &self,
        event: ChainEvent,
        parent: Option<&EventEnvelope<ChainEvent>>,
        options: CommitOptions,
        intent: StageAppendIntent,
    ) -> Result<ChainEvent, CommitError> {
        self.validate_prebuilt(&event, options)?;

        let mut event = event;

        // One-to-one handlers frequently author a fresh typed event and rely
        // on the commit seam for integration metadata. Fan-in accumulators
        // author their exact union before this point, so only apply the parent
        // fallback when no per-output activation provenance exists.
        if event.composite_activations().is_empty() {
            if let Some(parent) = parent {
                event = event.try_with_composite_activations(
                    parent.event.composite_activations().to_vec(),
                )?;
            }
        }

        if let Some(flow_context) = self.flow_context {
            event = event.with_flow_context(flow_context.clone());
            if intent.runs_output_commit_hooks() && !self.observer_scope.is_deterministic_replay() {
                apply_runtime_journey_identity(&mut event, flow_context);
            }
        }

        if let Some(instrumentation) = self.instrumentation {
            // Stamp the per-event processing-time wide-event field from the
            // runtime's own per-invocation measurement (FLOWIP-115f, replacing the
            // deleted TimingMiddleware). This is live-run wall-clock evidence: the
            // same replay gate as journey identity skips it under strict replay. The
            // field is excluded from replay equivalence by the value-preserving
            // projection, like runtime_context telemetry; the authoritative original
            // journal holds the live measurement.
            if intent.runs_output_commit_hooks() && !self.observer_scope.is_deterministic_replay() {
                event.processing_info.processing_time = instrumentation.last_processing_time();
            }
            event = event.with_runtime_context(instrumentation.snapshot_with_control());
        }

        if intent.runs_output_commit_hooks() {
            if let (Some(observers), Some(flow_context)) = (self.observers, self.flow_context) {
                let before = value_preserving_projection(&event)?;
                let parent_event = parent.map(|envelope| &envelope.event);
                let report = run_output_commit_observers(
                    observers,
                    flow_context.stage_id,
                    &flow_context.stage_name,
                    flow_context,
                    self.observer_scope,
                    parent_event,
                    &mut event,
                )
                .map_err(|e| -> CommitError { e.to_string().into() })?;
                ensure_value_preserving(before, &event)?;
                append_observer_diagnostics(
                    report,
                    flow_context,
                    self.instrumentation,
                    self.data_journal,
                    parent,
                )
                .await?;
            }
        }

        Ok(event)
    }

    async fn finish_committed(
        &self,
        written: &EventEnvelope<ChainEvent>,
        options: CommitOptions,
        intent: StageAppendIntent,
    ) {
        if let Some(instrumentation) = self.instrumentation {
            if options.count_output && written.event.is_data() {
                instrumentation.record_output_event(&written.event);
            }
        }

        if let Some(heartbeat) = self.heartbeat_state {
            heartbeat.record_last_output(written.event.id);
        }

        if matches!(
            intent.mirror_policy(),
            MirrorPolicy::FrameworkMiddlewareAllowlist
        ) {
            if let Some(system_journal) = self.system_journal {
                mirror_middleware_event_to_system_journal(written, system_journal).await;
            }
        }
    }

    async fn append_no_hook_prebuilt(
        &self,
        mut event: ChainEvent,
        parent: Option<&EventEnvelope<ChainEvent>>,
        intent: StageAppendIntent,
    ) -> Result<EventEnvelope<ChainEvent>, CommitError> {
        debug_assert!(!intent.runs_output_commit_hooks());

        if let Some(flow_context) = self.flow_context {
            event = event.with_flow_context(flow_context.clone());
        }

        if let Some(instrumentation) = self.instrumentation {
            event = event.with_runtime_context(instrumentation.snapshot_with_control());
        }

        let written = self
            .data_journal
            .append(event, parent)
            .await
            .map_err(|e| -> CommitError { e.to_string().into() })?;

        if let Some(heartbeat) = self.heartbeat_state {
            heartbeat.record_last_output(written.event.id);
        }

        if matches!(
            intent.mirror_policy(),
            MirrorPolicy::FrameworkMiddlewareAllowlist
        ) {
            if let Some(system_journal) = self.system_journal {
                mirror_middleware_event_to_system_journal(&written, system_journal).await;
            }
        }

        Ok(written)
    }

    /// Validate a prebuilt event before a caller performs any external gating
    /// such as backpressure reservation.
    pub(crate) fn validate_prebuilt(
        &self,
        event: &ChainEvent,
        options: CommitOptions,
    ) -> Result<(), CommitError> {
        if !options.validate_output_contract || !event.is_data() {
            return Ok(());
        }

        // Error-marked rows are forwarded provenance, not handler output. An
        // in-band business error (Validation/Domain) keeps its input event
        // type as it passes through, so checking it against the stage's
        // declared output types would kill every type-changing stage that
        // forwards one, contradicting the error-routing doctrine that
        // business errors stay in the main pipeline for downstream stages to
        // observe.
        if matches!(
            event.processing_info.status,
            obzenflow_core::event::status::processing_status::ProcessingStatus::Error { .. }
        ) {
            return Ok(());
        }

        let Some(output_contract) = self.output_contract else {
            return Ok(());
        };
        if output_contract.is_empty() {
            return Ok(());
        }

        let event_type = event.event_type();
        if output_contract.contains_event_type(&event_type) {
            return Ok(());
        }

        let declared = output_contract_summary(output_contract);
        Err(format!(
            "Data output event type `{event_type}` is not declared in the stage output contract (declared: [{declared}])"
        )
        .into())
    }
}

pub(crate) struct FrameworkObservabilityCommit<'a> {
    pub flow_context: &'a FlowContext,
    pub data_journal: &'a Arc<dyn Journal<ChainEvent>>,
    pub system_journal: Option<&'a Arc<dyn Journal<SystemEvent>>>,
    pub instrumentation: Option<&'a Arc<StageInstrumentation>>,
    pub heartbeat_state: Option<&'a Arc<HeartbeatState>>,
    /// Stage physical-row writer. Most events on this path are non-Data, but
    /// middleware may author durable framework Data facts through the same
    /// buffer, and those rows participate in B2 accounting.
    pub backpressure_writer: &'a BackpressureWriter,
    pub parent: Option<&'a EventEnvelope<ChainEvent>>,
    pub observer_scope: MiddlewareExecutionScope,
}

pub(crate) async fn commit_framework_observability_events(
    events: Vec<ChainEvent>,
    context: FrameworkObservabilityCommit<'_>,
) -> Result<(), CommitError> {
    if events.is_empty() {
        return Ok(());
    }

    let committer = OutputCommitter {
        data_journal: context.data_journal,
        flow_context: Some(context.flow_context),
        system_journal: context.system_journal,
        instrumentation: context.instrumentation,
        heartbeat_state: context.heartbeat_state,
        output_contract: None,
        backpressure_writer: Some(context.backpressure_writer),
        observers: None,
        observer_scope: context.observer_scope,
    };

    for event in events {
        committer
            .commit_prebuilt_with_intent(
                event,
                context.parent,
                CommitOptions::default(),
                StageAppendIntent::FrameworkObservability,
            )
            .await?;
    }

    Ok(())
}

pub(crate) fn is_framework_middleware_observability_event(event: &ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::CircuitBreaker(_) | MiddlewareLifecycle::RateLimiter(_)
        ))
    )
}

pub(crate) async fn append_observer_diagnostics(
    report: ObserverReport,
    flow_context: &FlowContext,
    instrumentation: Option<&Arc<StageInstrumentation>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), CommitError> {
    if report.is_empty() {
        return Ok(());
    }
    validate_observer_diagnostics(&report)?;

    let committer = OutputCommitter {
        data_journal,
        flow_context: Some(flow_context),
        system_journal: None,
        instrumentation,
        heartbeat_state: None,
        output_contract: None,
        backpressure_writer: None,
        observers: None,
        observer_scope: MiddlewareExecutionScope::LiveHandler,
    };

    for diagnostic in report.diagnostics {
        committer
            .append_no_hook_prebuilt(diagnostic, parent, StageAppendIntent::ObserverDiagnostic)
            .await
            .map_err(|e| format!("Failed to append observer diagnostic: {e}"))?;
    }

    Ok(())
}

fn validate_observer_diagnostics(report: &ObserverReport) -> Result<(), CommitError> {
    for diagnostic in &report.diagnostics {
        if diagnostic.is_data() || diagnostic.is_control() {
            return Err(format!(
                "observer diagnostic '{}' cannot author Data or flow-control events",
                diagnostic.event_type()
            )
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod observer_diagnostic_tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};

    #[test]
    fn observer_reports_reject_data_and_flow_control_before_append() {
        let writer = WriterId::from(StageId::new());
        let reports = [
            ObserverReport::empty().with_diagnostic(ChainEventFactory::data_event(
                writer,
                "test.rogue",
                serde_json::json!({ "value": 1 }),
            )),
            ObserverReport::empty().with_diagnostic(ChainEventFactory::eof_event(writer, true)),
        ];

        for report in reports {
            let diagnostic = &report.diagnostics[0];
            assert!(
                diagnostic.is_data() || diagnostic.is_control(),
                "fixture must exercise a prohibited observer side channel"
            );
            assert!(
                validate_observer_diagnostics(&report).is_err(),
                "prohibited diagnostics must fail before any journal append"
            );
        }
    }
}

fn value_preserving_projection(event: &ChainEvent) -> Result<serde_json::Value, CommitError> {
    let mut value = serde_json::to_value(event).map_err(|e| -> CommitError { e.into() })?;
    if let Some(processing) = value
        .as_object_mut()
        .and_then(|object| object.get_mut("processing_info"))
        .and_then(|value| value.as_object_mut())
    {
        processing.remove("processing_time");
    }
    if let Some(object) = value.as_object_mut() {
        object.remove("observability");
    }
    Ok(value)
}

fn ensure_value_preserving(
    before: serde_json::Value,
    event: &ChainEvent,
) -> Result<(), CommitError> {
    let after = value_preserving_projection(event)?;
    if before == after {
        return Ok(());
    }
    Err("output-commit observer changed non-observability event fields".into())
}

fn apply_runtime_journey_identity(event: &mut ChainEvent, flow: &FlowContext) {
    if !event.is_data() || event.correlation.is_some() {
        return;
    }

    let should_mint = matches!(
        flow.stage_type,
        StageType::FiniteSource | StageType::InfiniteSource
    ) || event.causality.is_root();

    if should_mint {
        let correlation_id = CorrelationId::new();
        let mut payload = CorrelationPayload::new(&flow.stage_name, event.id);
        payload.metadata = Some(serde_json::json!({
            "flow_name": flow.flow_name,
            "flow_id": flow.flow_id,
            "source_event_id": event.id.to_string(),
        }));
        event.set_single_correlation(correlation_id, Some(payload));
    } else {
        tracing::warn!(
            event_id = %event.id,
            stage_name = %flow.stage_name,
            "Non-source derived data event missing correlation_id"
        );
    }
}
