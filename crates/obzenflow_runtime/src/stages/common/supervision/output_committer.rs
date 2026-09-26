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
//! journal append and heartbeat tracking.
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

use crate::messaging::DeliveredRecord;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::journal::AppendOptions;
use std::sync::Arc;

use obzenflow_core::event::context::{MiddlewareExecutionScope, StageType};
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::provenance::FlowContext;

use obzenflow_core::event::{ChainPayload, CorrelationId, JournalRecord};
use obzenflow_core::journal::{Journal, JournalCapture};
use obzenflow_core::{ChainEvent, WriterId};

use crate::backpressure::{BackpressureReservation, BackpressureWriter, DirectFactClaim};
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::{CaptureProjection, RuntimeCapture, StageInstrumentation};
use crate::stages::common::heartbeat::HeartbeatState;

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

fn event_is_authored_by_stage(
    event: &ChainEvent,
    flow_context: &FlowContext,
    _scope: MiddlewareExecutionScope,
) -> bool {
    // Sources have no upstream data journal from which they can forward a
    // foreign Data row. Until the source witness pass installs the runtime
    // writer before handler dispatch, raw source handlers may still construct
    // their output with a handler-owned WriterId. The source commit boundary
    // is therefore the structural authorship proof in both live and replay.
    if matches!(
        flow_context.stage_type,
        StageType::FiniteSource | StageType::InfiniteSource
    ) {
        return true;
    }

    if event.writer_id == WriterId::from(flow_context.stage_id) {
        return true;
    }

    false
}

/// A boxed, thread-safe error from a commit attempt. Each caller maps this onto
/// its own error type: the drain prefixes it with `Failed to write pending
/// output`, and the effects layer wraps it in `EffectError::Journal`.
pub(crate) type CommitError = Box<dyn std::error::Error + Send + Sync>;

/// A stage control row advances the emitted position only after its append is
/// acknowledged. It does not advance the authored Data frontier or row count.
pub(crate) fn commit_control_output(
    journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    mut event: ChainEvent,
) -> futures::future::BoxFuture<'static, Result<JournalRecord<ChainPayload>, CommitError>> {
    let journal = journal.clone();
    let instrumentation = instrumentation.clone();
    crate::supervised_base::publication::commit(async move {
        let authored_writer = event.writer_id;
        if let ChainPayload::FlowControl(FlowControlPayload::Eof {
            writer_id,
            writer_seq,
            writer_seq_by_event_type,
            last_event_id,
            ..
        }) = &mut event.payload
        {
            // Seal only after predecessor publications have finished their
            // accounting in this stage's writer order.
            let (seq, by_type, last) = instrumentation.authored_data_frontier();
            *writer_id = Some(authored_writer);
            *writer_seq = Some(seq);
            *writer_seq_by_event_type = by_type;
            *last_event_id = last;
        }
        let mut snapshot = instrumentation.capture_accounting();
        snapshot.project_emission(&event);
        event = snapshot.attach_to(event);
        let capture = instrumentation.journal_capture(None, vec![(1, true)]);
        let written = crate::supervised_base::publication::append_inline(
            &journal,
            event,
            AppendOptions::default().with_capture(capture),
        )
        .await?;
        instrumentation.record_emitted(&written.authored());
        Ok(written)
    })
}

/// Error-journal Data participates in emitted-output accounting without
/// advancing the authored data-journal transport frontier.
pub(crate) fn commit_error_output(
    journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    mut event: ChainEvent,
    parent: Option<&DeliveredRecord<ChainPayload>>,
) -> futures::future::BoxFuture<'static, Result<JournalRecord<ChainPayload>, CommitError>> {
    use futures::FutureExt;
    let journal = journal.clone();
    let instrumentation = instrumentation.clone();
    let parent = parent.cloned();
    crate::supervised_base::publication::commit(async move {
        let mut snapshot = instrumentation.capture_accounting();
        if event.consumes_data_credit() {
            snapshot.accounting.events_emitted_total =
                snapshot.accounting.events_emitted_total.saturating_add(1);
            snapshot.project_emission(&event);
        }
        let emitted = u64::from(event.consumes_data_credit());
        event = snapshot.attach_to(event);
        let capture = instrumentation.journal_capture(None, vec![(emitted, true)]);
        let written = crate::supervised_base::publication::append_inline(
            &journal,
            event,
            AppendOptions::from_record(parent.as_ref().map(DeliveredRecord::record))?
                .with_capture(capture),
        )
        .await?;
        if written.consumes_data_credit() {
            instrumentation.record_error_journal_output_event(&written.authored());
        }
        Ok(written)
    })
    .boxed()
}

enum PhysicalDataReservation {
    Legacy(BackpressureReservation),
    Direct(DirectFactClaim),
    DirectTracked {
        claim: DirectFactClaim,
        reservation: BackpressureReservation,
    },
}

impl PhysicalDataReservation {
    fn indeterminate(self) {
        match self {
            Self::Legacy(reservation) => reservation.indeterminate(),
            Self::Direct(claim) => claim.indeterminate(),
            Self::DirectTracked { claim, reservation } => {
                claim.indeterminate();
                reservation.indeterminate();
            }
        }
    }

    fn commit(self, rows: u64) -> Result<(), CommitError> {
        match self {
            Self::Legacy(reservation) => {
                reservation.commit(rows);
                Ok(())
            }
            Self::Direct(claim) => claim.commit().map_err(|error| {
                crate::supervised_base::publication::accounting_failed(error.into())
            }),
            Self::DirectTracked { claim, reservation } => {
                claim.commit().map_err(|error| {
                    crate::supervised_base::publication::accounting_failed(error.into())
                })?;
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

/// The kind of stage-runtime journal append, used to gate runtime enrichment
/// and the framework system-journal mirror.
///
/// Only the five wired variants exist. Other stage-runtime appends are
/// out-of-surface raw appends today: error-journal and error-routed-data writes,
/// backpressure activity pulses, sink delivery receipts, forwarded sink-boundary
/// control rows, source/stage lifecycle events, ingress refusal facts, and the
/// `fx.emit` / domain-effect-outcome / framework-effect-record facts (which flow
/// through `NonDataStageFact`) each append directly to their journal (and mirror
/// directly where applicable) rather than through this seam. Routing them through
/// named intents is deferred to a committer-consolidation slice (FLOWIP-120b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StageAppendIntent {
    NormalStageData,
    NonDataStageFact,
    FrameworkTerminal,
    FrameworkObservability,
}

/// One member of a policy-neutral atomic journal group.
pub(crate) struct AtomicCommitEntry {
    pub event: ChainEvent,
    pub options: CommitOptions,
    pub intent: StageAppendIntent,
}

impl StageAppendIntent {
    pub(crate) fn receives_runtime_data_enrichment(self) -> bool {
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
    /// Per-event execution scope for replay-sensitive runtime enrichment.
    pub observer_scope: MiddlewareExecutionScope,
}

struct OwnedOutputCommitter {
    /// The stage data journal every stage-authored event is appended to.
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    /// Wide-event flow context stamped on the committed event. Absent on the
    /// effects-layer path, which does not enrich effect records today.
    pub flow_context: Option<FlowContext>,

    /// Stage instrumentation for per-type producer counting and the
    /// runtime-context snapshot. Absent on the effects-layer path.
    pub instrumentation: Option<Arc<StageInstrumentation>>,
    /// Heartbeat state, updated with the last committed output id. Absent on the
    /// effects-layer path.
    pub heartbeat_state: Option<Arc<HeartbeatState>>,
    /// Runtime output contract for stage-authored domain `Data` facts. Absent
    /// on reserved framework append paths and legacy callers.
    pub output_contract: Option<StageOutputContract>,
    /// Physical-row accounting for direct `Data` facts. Pending-output drains
    /// leave this absent because they own an enforced or replay-scoped
    /// reservation outside the commit helper.
    pub backpressure_writer: Option<BackpressureWriter>,
    /// Per-event execution scope for replay-sensitive runtime enrichment.
    pub observer_scope: MiddlewareExecutionScope,
}
impl OwnedOutputCommitter {
    fn borrowed(&self) -> OutputCommitter<'_> {
        OutputCommitter {
            data_journal: &self.data_journal,
            flow_context: self.flow_context.as_ref(),

            instrumentation: self.instrumentation.as_ref(),
            heartbeat_state: self.heartbeat_state.as_ref(),
            output_contract: self.output_contract.as_ref(),
            backpressure_writer: self.backpressure_writer.as_ref(),
            observer_scope: self.observer_scope,
        }
    }
}

impl OutputCommitter<'_> {
    fn owned(&self) -> OwnedOutputCommitter {
        OwnedOutputCommitter {
            data_journal: self.data_journal.clone(),
            flow_context: self.flow_context.cloned(),

            instrumentation: self.instrumentation.cloned(),
            heartbeat_state: self.heartbeat_state.cloned(),
            output_contract: self.output_contract.cloned(),
            backpressure_writer: self.backpressure_writer.cloned(),
            observer_scope: self.observer_scope,
        }
    }

    /// Commit a fully-constructed event to the data journal.
    ///
    /// The event must already carry its content, identity, and any
    /// effect-provenance or error status its author set. This applies the shared
    /// commit core in the same order the drain used before Step 1: flow-context
    /// enrichment, optional per-type counting, runtime-context enrichment,
    /// journal append and heartbeat tracking. Each step
    /// is gated on the relevant handle, so an effects-layer committer holding
    /// only a journal handle performs a bare append, exactly as
    /// `append_effect_record` did before Step 1.
    pub(crate) async fn commit_prebuilt(
        &self,
        event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
        options: CommitOptions,
    ) -> Result<JournalRecord<ChainPayload>, CommitError> {
        let intent = if event.consumes_data_credit() {
            StageAppendIntent::NormalStageData
        } else if is_framework_middleware_observability_event(&event) {
            // Source policies drain their control events through this generic
            // path. Preserve the system-journal mirror for their transitions;
            // its allowlist and author check exclude activity and forwarded rows.
            StageAppendIntent::FrameworkObservability
        } else {
            StageAppendIntent::NonDataStageFact
        };
        self.commit_prebuilt_with_intent(event, parent, options, intent)
            .await
    }

    pub(crate) async fn commit_prebuilt_with_intent(
        &self,
        event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
        options: CommitOptions,
        intent: StageAppendIntent,
    ) -> Result<JournalRecord<ChainPayload>, CommitError> {
        let owned = self.owned();
        let parent = parent.cloned();
        crate::supervised_base::publication::commit(async move {
            owned
                .borrowed()
                .commit_prebuilt_with_intent_inline(event, parent.as_ref(), options, intent)
                .await
        })
        .await
    }

    async fn commit_prebuilt_with_intent_inline(
        &self,
        event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
        options: CommitOptions,
        intent: StageAppendIntent,
    ) -> Result<JournalRecord<ChainPayload>, CommitError> {
        let event = self
            .prepare_prebuilt_with_intent(event, parent, options, intent)
            .await?;

        // Direct facts are already past input admission, so this path must not
        // wait. It nevertheless records every durable physical Data row. A
        // failed append drops the reservation and releases it.
        let backpressure_reservation = reserve_direct_data_rows(
            self.backpressure_writer,
            u64::from(event.consumes_data_credit()),
        )?;

        let capture = self.observation_capture(vec![(
            u64::from(options.count_output && event.consumes_data_credit()),
            intent.receives_runtime_data_enrichment(),
        )]);
        let written = match crate::supervised_base::publication::append_inline(
            self.data_journal,
            event,
            AppendOptions::from_record(parent.map(DeliveredRecord::record))?.with_capture(capture),
        )
        .await
        {
            Ok(written) => written,
            Err(error) => {
                if crate::supervised_base::publication::is_indeterminate(&error) {
                    if let Some(reservation) = backpressure_reservation {
                        reservation.indeterminate();
                    }
                }
                return Err(Box::new(error));
            }
        };

        if let Some(reservation) = backpressure_reservation {
            reservation.commit(1)?;
        }

        self.account_committed(&written, options);
        Ok(written)
    }

    pub(crate) async fn commit_reserved_prebuilt(
        &self,
        event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
        options: CommitOptions,
        reservation: BackpressureReservation,
    ) -> Result<JournalRecord<ChainPayload>, CommitError> {
        let owned = self.owned();
        let parent = parent.cloned();
        crate::supervised_base::publication::commit(async move {
            let committer = owned.borrowed();
            let event = committer
                .prepare_prebuilt_with_intent(
                    event,
                    parent.as_ref(),
                    options,
                    StageAppendIntent::NormalStageData,
                )
                .await?;
            let capture = committer.observation_capture(vec![(
                u64::from(options.count_output && event.consumes_data_credit()),
                true,
            )]);
            let written = match crate::supervised_base::publication::append_inline(
                committer.data_journal,
                event,
                AppendOptions::from_record(parent.as_ref().map(DeliveredRecord::record))?
                    .with_capture(capture),
            )
            .await
            {
                Ok(written) => written,
                Err(error) => {
                    if crate::supervised_base::publication::is_indeterminate(&error) {
                        reservation.indeterminate();
                    }
                    return Err(Box::new(error) as CommitError);
                }
            };
            reservation.commit(1);
            committer.account_committed(&written, options);
            Ok(written)
        })
        .await
    }

    /// Seal and commit a framework-owned terminal EOF at the current durable
    /// output frontier.
    ///
    /// Framework strategy code carries only terminal intent. The runtime owns
    /// these transport fields because only the commit boundary knows which
    /// preceding Data facts are durably visible.
    pub(crate) async fn commit_authored_terminal(
        &self,
        event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
    ) -> Result<JournalRecord<ChainPayload>, CommitError> {
        let owned = self.owned();
        let parent = parent.cloned();
        crate::supervised_base::publication::commit(async move {
            owned
                .borrowed()
                .commit_authored_terminal_inline(event, parent.as_ref())
                .await
        })
        .await
    }

    async fn commit_authored_terminal_inline(
        &self,
        mut event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
    ) -> Result<JournalRecord<ChainPayload>, CommitError> {
        let flow_context = self
            .flow_context
            .ok_or("framework terminal commit requires a flow context")?;
        let instrumentation = self
            .instrumentation
            .ok_or("framework terminal commit requires stage instrumentation")?;
        let expected_writer = WriterId::from(flow_context.stage_id);

        if event.writer_id != expected_writer {
            return Err(format!(
                "framework terminal envelope writer {:?} does not match stage {:?}",
                event.writer_id, expected_writer
            )
            .into());
        }

        let (expected_seq, expected_by_type, expected_last_event_id) =
            instrumentation.authored_data_frontier();

        let ChainPayload::FlowControl(FlowControlPayload::Eof {
            writer_id,
            writer_seq,
            writer_seq_by_event_type,
            last_event_id,
            ..
        }) = &mut event.payload
        else {
            return Err("framework terminal commit requires an EOF event".into());
        };

        if writer_id.is_some_and(|writer| writer != expected_writer) {
            return Err("framework terminal payload writer conflicts with its stage author".into());
        }
        if writer_seq.is_some_and(|seq| seq != expected_seq) {
            return Err(format!(
                "framework terminal writer_seq {:?} conflicts with committed frontier {:?}",
                writer_seq, expected_seq
            )
            .into());
        }
        if !writer_seq_by_event_type.is_empty() && *writer_seq_by_event_type != expected_by_type {
            return Err(
                "framework terminal per-type frontier conflicts with committed output counts"
                    .into(),
            );
        }
        if last_event_id.is_some() && *last_event_id != expected_last_event_id {
            return Err(
                "framework terminal last_event_id conflicts with the committed output frontier"
                    .into(),
            );
        }

        *writer_id = Some(expected_writer);
        *writer_seq = Some(expected_seq);
        *writer_seq_by_event_type = expected_by_type;
        *last_event_id = expected_last_event_id;

        self.commit_prebuilt_with_intent_inline(
            event,
            parent,
            CommitOptions {
                count_output: false,
                validate_output_contract: false,
            },
            StageAppendIntent::FrameworkTerminal,
        )
        .await
    }

    /// Commit every member through one journal atomic-group primitive. Event
    /// enrichment happens before the append; counters, heartbeat state, and
    /// best-effort system mirrors advance only after the complete group is
    /// visible.
    pub(crate) async fn commit_atomic_group(
        &self,
        group_id: &str,
        entries: Vec<AtomicCommitEntry>,
        parent: Option<&DeliveredRecord<ChainPayload>>,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, CommitError> {
        let owned = self.owned();
        let parent = parent.cloned();
        let group_id = group_id.to_owned();
        crate::supervised_base::publication::commit(async move {
            owned
                .borrowed()
                .commit_atomic_group_inline(&group_id, entries, parent.as_ref())
                .await
        })
        .await
    }

    async fn commit_atomic_group_inline(
        &self,
        group_id: &str,
        entries: Vec<AtomicCommitEntry>,
        parent: Option<&DeliveredRecord<ChainPayload>>,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, CommitError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut prepared = Vec::with_capacity(entries.len());
        let mut metadata = Vec::with_capacity(entries.len());
        let mut projections = Vec::with_capacity(entries.len());
        let mut emitted = 0u64;
        let mut last_emission = None;
        let mut snapshot = self
            .instrumentation
            .map(|instrumentation| instrumentation.capture_accounting());
        for entry in entries {
            let mut event = self
                .prepare_prebuilt_with_intent(entry.event, parent, entry.options, entry.intent)
                .await?;
            if let Some(snapshot) = &mut snapshot {
                self.project_committed_output(snapshot, &event, entry.options);
                event = snapshot.clone().attach_to(event);
            }
            if entry.options.count_output && event.consumes_data_credit() {
                emitted += 1;
                last_emission = Some((event.id, event.writer_id));
            }
            projections.push(CaptureProjection {
                emitted,
                measurements: entry.intent.receives_runtime_data_enrichment(),
                last_emission,
            });
            prepared.push(event);
            metadata.push((entry.options, entry.intent));
        }

        let data_count = prepared
            .iter()
            .filter(|event| event.consumes_data_credit())
            .count() as u64;
        let backpressure_reservation =
            reserve_direct_data_rows(self.backpressure_writer, data_count)?;

        let member_count = metadata.len();
        let written = match crate::supervised_base::publication::append_group_inline(
            self.data_journal,
            group_id,
            prepared,
            AppendOptions::from_record(parent.map(DeliveredRecord::record))?
                .with_capture(self.observation_capture(projections)),
        )
        .await
        {
            Ok(written) => written,
            Err(error) => {
                tracing::error!(
                    group_id,
                    member_count,
                    error = %error,
                    "atomic terminal journal group commit failed"
                );
                if crate::supervised_base::publication::is_indeterminate(&error) {
                    if let Some(reservation) = backpressure_reservation {
                        reservation.indeterminate();
                    }
                }
                return Err(Box::new(error));
            }
        };

        // A successful Journal call means every prepared member is visible.
        // Account the physical rows before checking the returned-envelope
        // cardinality so a broken Journal implementation cannot leak debt.
        if let Some(reservation) = backpressure_reservation {
            reservation.commit(data_count)?;
        }
        if written.len() != metadata.len() {
            tracing::error!(
                group_id,
                member_count,
                returned_envelopes = written.len(),
                "journal violated atomic-group envelope cardinality"
            );
            return Err(crate::supervised_base::publication::accounting_failed(
                format!(
                    "atomic journal group '{group_id}' returned {} envelopes for {} members",
                    written.len(),
                    metadata.len()
                )
                .into(),
            ));
        }
        for (envelope, (options, _)) in written.iter().zip(&metadata) {
            self.account_committed(envelope, *options);
        }
        Ok(written)
    }

    async fn prepare_prebuilt_with_intent(
        &self,
        event: ChainEvent,
        parent: Option<&DeliveredRecord<ChainPayload>>,
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
                event = event
                    .try_with_composite_activations(parent.composite_activations().to_vec())?;
            }
        }

        if let Some(flow_context) = self.flow_context {
            // A live source has no upstream author to preserve: every Data row
            // crossing this commit boundary is authored by the source stage.
            // Raw source handlers predate runtime-installed writers and may
            // construct envelopes with an arbitrary WriterId, so seal that
            // identity here. Strict replay preserves the archived writer and
            // resolves it through the topology-keyed replay alias instead.
            if intent == StageAppendIntent::NormalStageData
                && event.consumes_data_credit()
                && !self.observer_scope.is_deterministic_replay()
                && matches!(
                    flow_context.stage_type,
                    StageType::FiniteSource | StageType::InfiniteSource
                )
            {
                event.writer_id = WriterId::from(flow_context.stage_id);
            }
            event = event.with_flow_context(flow_context.clone());
            if intent.receives_runtime_data_enrichment()
                && !self.observer_scope.is_deterministic_replay()
            {
                apply_runtime_journey_identity(&mut event, flow_context);
            }
        }

        if let Some(instrumentation) = self.instrumentation {
            let mut snapshot = instrumentation.capture_accounting();
            self.project_committed_output(&mut snapshot, &event, options);
            event = snapshot.attach_to(event);
        }

        Ok(event)
    }

    fn observation_capture<P: Into<CaptureProjection>>(
        &self,
        projections: Vec<P>,
    ) -> JournalCapture<ChainEvent> {
        if self.observer_scope.is_deterministic_replay() {
            JournalCapture::Historical
        } else if let Some(instrumentation) = self.instrumentation {
            instrumentation.journal_capture(Some(self.observer_scope), projections)
        } else {
            JournalCapture::default()
        }
    }

    /// A visible row describes its committed prefix, including itself. Live
    /// counters still advance only after append acknowledgement.
    fn project_committed_output(
        &self,
        snapshot: &mut RuntimeCapture,
        event: &ChainEvent,
        options: CommitOptions,
    ) {
        if !options.count_output || !event.consumes_data_credit() {
            return;
        }
        snapshot.accounting.events_emitted_total =
            snapshot.accounting.events_emitted_total.saturating_add(1);
        snapshot.project_emission(event);
        if self
            .flow_context
            .is_none_or(|context| event_is_authored_by_stage(event, context, self.observer_scope))
        {
            let event_type = event.event_type();
            if let Some(count) = snapshot
                .accounting
                .data_outputs_by_event_type
                .iter_mut()
                .find(|count| count.event_type.as_str() == event_type)
            {
                count.total = count.total.saturating_add(1);
            } else {
                snapshot.accounting.data_outputs_by_event_type.push(
                    obzenflow_core::event::provenance::EventTypeCountContext {
                        event_type: event_type.into(),
                        total: 1,
                    },
                );
                snapshot
                    .accounting
                    .data_outputs_by_event_type
                    .sort_by(|left, right| left.event_type.cmp(&right.event_type));
            }
        }
    }

    fn account_committed(&self, written: &JournalRecord<ChainPayload>, options: CommitOptions) {
        if let Some(instrumentation) = self.instrumentation {
            if options.count_output && written.consumes_data_credit() {
                let authored_here = self.flow_context.is_none_or(|flow_context| {
                    event_is_authored_by_stage(
                        &written.authored(),
                        flow_context,
                        self.observer_scope,
                    )
                });
                if authored_here {
                    instrumentation.record_output_event(&written.authored());
                } else {
                    instrumentation.record_forwarded_output_event(&written.authored());
                }
            }
        }

        if let Some(heartbeat) = self.heartbeat_state {
            heartbeat.record_last_output(written.envelope.provenance.event.id);
        }
    }

    /// Validate a prebuilt event before a caller performs any external gating
    /// such as backpressure reservation.
    pub(crate) fn validate_prebuilt(
        &self,
        event: &ChainEvent,
        options: CommitOptions,
    ) -> Result<(), CommitError> {
        if !options.validate_output_contract || !event.consumes_data_credit() {
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
            event.processing.status,
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

    pub instrumentation: Option<&'a Arc<StageInstrumentation>>,
    pub heartbeat_state: Option<&'a Arc<HeartbeatState>>,
    /// Stage physical-row writer. Most events on this path are non-Data, but
    /// middleware may author durable framework Data facts through the same
    /// buffer, and those rows participate in B2 accounting.
    pub backpressure_writer: &'a BackpressureWriter,
    pub parent: Option<&'a DeliveredRecord<ChainPayload>>,
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

        instrumentation: context.instrumentation,
        heartbeat_state: context.heartbeat_state,
        output_contract: None,
        backpressure_writer: Some(context.backpressure_writer),
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
        &event.payload,
        ChainPayload::Execution(
            ExecutionPayload::CircuitBreaker(_) | ExecutionPayload::RateLimiter(_)
        )
    )
}

fn apply_runtime_journey_identity(event: &mut ChainEvent, flow: &FlowContext) {
    if !event.consumes_data_credit() || event.correlation.is_some() {
        return;
    }

    let should_mint = matches!(
        flow.stage_type,
        StageType::FiniteSource | StageType::InfiniteSource
    ) || event.causality.is_root();

    if should_mint {
        let correlation_id = CorrelationId::new();
        let payload = CorrelationPayload::new(event.id);
        event.set_single_correlation(correlation_id, Some(payload));
    } else {
        tracing::warn!(
            event_id = %event.id,
            stage_name = %flow.stage_name,
            "Non-source derived data event missing correlation_id"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;

    #[test]
    fn journey_enrichment_keeps_origin_identity_and_only_application_metadata() {
        let stage = obzenflow_core::StageId::new();
        let mut flow = FlowContext::new("source", stage);
        flow.stage_type = StageType::FiniteSource;
        let mut event = ChainEventFactory::data_event(
            WriterId::from(stage),
            "application.input",
            serde_json::json!({"id": 1}),
        );
        apply_runtime_journey_identity(&mut event, &flow);
        let origin = event.correlation_payload().unwrap();
        assert_eq!(origin.entry_event_id, event.id);
        assert!(origin.entry_time_ns > 0);
        assert!(origin.metadata.is_none());
        assert_eq!(
            serde_json::to_value(origin)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            2
        );

        for metadata in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!({"flow_name": "custom", "source_event_id": "custom", "flow_id": "custom"}),
        ] {
            event
                .correlation
                .as_mut()
                .unwrap()
                .payload
                .as_mut()
                .unwrap()
                .metadata = Some(metadata);
            let original = event.correlation.clone();
            apply_runtime_journey_identity(&mut event, &flow);
            assert_eq!(event.correlation, original);
        }
    }
}
