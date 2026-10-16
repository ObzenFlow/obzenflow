// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::boundary::PhysicalCallOutcome;
use super::history::{validate_affine_terminal_group, validate_invariant_settlement_evidence};
use super::*;
use crate::execution::UnrecordedEffect;

/// Slot a guarded execution future fills with the real typed outcome, so
/// `perform` records from its own state rather than boundary-returned events.
type ExecutedOutcomeSlot<O> = Arc<Mutex<Option<(O, PreparedEffectSuccess)>>>;

/// Slot the guarded transactional future fills for settlement: the port's
/// result and the outcome committed through the handle, if any.
type TransactionalSettleSlot<O> =
    Arc<Mutex<Option<(Result<(), EffectError>, Option<PreparedEffectOutcome<O>>)>>>;

fn success_observation_events(
    success: &PreparedEffectSuccess,
    writer_id: WriterId,
    parent: &ChainEvent,
    lineage: obzenflow_core::config::LineagePolicy,
) -> Vec<ChainEvent> {
    match success {
        PreparedEffectSuccess::DomainFacts(facts) => facts
            .iter()
            .map(|fact| fact.clone().into_derived_event(writer_id, parent, lineage))
            .collect(),
        PreparedEffectSuccess::RecordedReply(_) => Vec::new(),
    }
}

struct RecoveryAbandonment {
    cursor: EffectCursor,
    descriptor_hash: EffectDescriptorHash,
    descriptor: EffectDescriptor,
    highest_started_attempt: EffectAttemptOrdinal,
    causal_input_id: EventId,
    reason: EffectAbortReason,
    control_events: Vec<ChainEvent>,
}

/// Authority and durable identity prepared after history selection and live
/// admission, then consumed by exactly one safety-specific execution path.
struct PreparedLiveEffect {
    identity: EffectIdentity,
    cursor: EffectCursor,
    descriptor_hash: EffectDescriptorHash,
    descriptor: EffectDescriptor,
    binding_context: EffectContext,
}

fn split_invariant_control_events(
    cursor: &EffectCursor,
    attempt: EffectAttemptOrdinal,
    control_events: Vec<ChainEvent>,
) -> Result<(Vec<ChainEvent>, Vec<ChainEvent>), EffectError> {
    let (settlement_index, _) =
        validate_invariant_settlement_evidence(cursor, attempt, &control_events)?;

    let mut preterminal = control_events;
    let terminal = preterminal.split_off(settlement_index);
    Ok((preterminal, terminal))
}

fn restore_archived_effect_identity(
    rebuilt: &mut ChainEvent,
    archived: &ChainEvent,
) -> Result<(), EffectError> {
    let rebuilt_content = serde_json::to_value(&rebuilt.payload)
        .map_err(|error| EffectError::Serialization(error.to_string()))?;
    let archived_content = serde_json::to_value(&archived.payload)
        .map_err(|error| EffectError::Serialization(error.to_string()))?;
    if rebuilt.id != archived.id
        || rebuilt_content != archived_content
        || rebuilt.effect_provenance != archived.effect_provenance
    {
        return Err(EffectError::EffectProvenanceMismatch(format!(
            "rematerialised effect event {} disagrees with its archived durable identity",
            archived.id
        )));
    }
    rebuilt.processing.event_time = archived.processing.event_time;
    rebuilt.effect_provenance = archived.effect_provenance.clone();
    Ok(())
}

fn restore_archived_terminal_identity(
    rebuilt: &mut ChainEvent,
    history: &EffectCursorHistory,
) -> Result<(), EffectError> {
    let archived = history
        .terminal_group_events
        .iter()
        .find(|event| event.id == rebuilt.id)
        .ok_or_else(|| {
            EffectError::EffectProvenanceMismatch(format!(
                "rematerialised terminal event {} is absent from its archived atomic group",
                rebuilt.id
            ))
        })?;
    restore_archived_effect_identity(rebuilt, archived)
}

#[derive(Debug, Clone)]
enum ReconstructionRefusal {
    DescriptorMismatch {
        cursor: EffectCursor,
        expected: EffectDescriptorHash,
        recorded: EffectDescriptorHash,
    },
    Unrecorded {
        cursor: EffectCursor,
    },
    /// A resumed act departs from the archive before it ends (FLOWIP-134k B7).
    Departed {
        reason: obzenflow_core::event::StageFatalReason,
        detail: String,
    },
    HistoryShape(EffectError),
    UndecodableOutcome(EffectError),
}

/// Whether a resume reproduces an act or runs it live (FLOWIP-134k B7). Only
/// a resume takes acts in order; a live run and a strict replay do not check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActPlan {
    Unchecked,
    Reproduce,
    Live,
}

/// Where a committed effect row came from (FLOWIP-134k §4): copied or
/// rematerialized from the archive, which `replay_context` names, or written
/// by live work, which carries none whatever its input's lineage.
#[derive(Clone, Copy)]
enum RowLineage<'a> {
    Live,
    Reproduced(Option<&'a EffectCursorHistory>),
}

impl ReconstructionRefusal {
    fn error(&self) -> EffectError {
        match self {
            Self::DescriptorMismatch {
                cursor,
                expected,
                recorded,
            } => EffectError::DescriptorMismatch {
                cursor: cursor.clone(),
                expected: expected.clone(),
                recorded: recorded.clone(),
            },
            Self::Unrecorded { cursor } => EffectError::MissingRecordedEffect {
                cursor: cursor.clone(),
            },
            Self::Departed { detail, .. } => EffectError::RecordingDeparture {
                detail: detail.clone(),
            },
            Self::HistoryShape(error) | Self::UndecodableOutcome(error) => error.clone(),
        }
    }

    fn reason(&self) -> obzenflow_core::event::StageFatalReason {
        use obzenflow_core::event::StageFatalReason;
        match self {
            Self::DescriptorMismatch { .. } => StageFatalReason::DescriptorMismatch,
            Self::Unrecorded { .. } => StageFatalReason::MissingRecordedEffect,
            Self::Departed { reason, .. } => *reason,
            Self::HistoryShape(_) | Self::UndecodableOutcome(_) => {
                StageFatalReason::ReplayDivergence
            }
        }
    }
}

/// The refusal for a resumed stage whose recorded input ended, at natural EOF
/// or a watermark that completes its frontier, with archived acts left
/// (FLOWIP-134k B7). A drain or stop never checks.
pub(crate) fn unreproduced_acts_fatal(
    history: Option<&Arc<EffectHistory>>,
    execution: &crate::execution::RuntimeExecution,
    stage_id: StageId,
    stage_key: &str,
) -> Option<crate::stages::common::handler_error::StageFatal> {
    use obzenflow_core::event::StageFatalReason;
    let control = execution.resume_control()?;
    if control.stage_is_live(stage_id) {
        return None;
    }
    let skipped = history?.acts().remaining().into_iter().next()?;
    Some(crate::stages::common::handler_error::StageFatal::new(
        obzenflow_core::event::StageFatalCode::Replay,
        if skipped.is_fact() {
            StageFatalReason::UnusedFactHistory
        } else {
            StageFatalReason::UnusedEffectHistory
        },
        format!(
            "stage '{stage_key}' skipped {} its recording holds",
            skipped.describe()
        ),
    ))
}

/// A resume's act check and the cursor's history must agree: an archived act
/// has history, and a live act has none (FLOWIP-134k B7).
fn act_plan_mismatch(
    plan: ActPlan,
    selection: &EffectHistorySelection,
    cursor: &EffectCursor,
) -> Option<ReconstructionRefusal> {
    let agrees = matches!(
        (plan, selection),
        (ActPlan::Unchecked, _)
            | (
                ActPlan::Reproduce,
                EffectHistorySelection::Hit(_) | EffectHistorySelection::InDoubt(_)
            )
            | (ActPlan::Live, EffectHistorySelection::Miss)
    );
    (!agrees).then(|| {
        ReconstructionRefusal::HistoryShape(EffectError::EffectProvenanceMismatch(format!(
            "effect cursor {cursor:?} disagrees with the stage's archived acts"
        )))
    })
}

/// The first condition that closes authoring for an invocation. The stage
/// fails with it whatever the handler does with the returned error.
#[derive(Debug, Clone)]
enum AuthoringHalt {
    Binding(BindingAuthorityFault),
    /// A replay reached a cursor its recording holds no outcome for
    /// (FLOWIP-134i B13).
    RecordingEnded(EffectCursor),
    Reconstruction(ReconstructionRefusal),
}

impl AuthoringHalt {
    fn error(&self) -> EffectError {
        match self {
            Self::Binding(fault) => EffectError::BindingAuthority {
                fault: fault.clone(),
            },
            Self::RecordingEnded(cursor) => EffectError::RecordingEnded {
                cursor: cursor.clone(),
            },
            Self::Reconstruction(refusal) => refusal.error(),
        }
    }

    fn stage_fatal(&self) -> crate::stages::common::handler_error::StageFatal {
        match self {
            Self::Binding(fault) => fault.stage_fatal(),
            Self::RecordingEnded(_) => crate::stages::common::handler_error::StageFatal::new(
                obzenflow_core::event::StageFatalCode::Replay,
                obzenflow_core::event::StageFatalReason::RecordingEnded,
                self.error().to_string(),
            ),
            Self::Reconstruction(refusal) => crate::stages::common::handler_error::StageFatal::new(
                obzenflow_core::event::StageFatalCode::Replay,
                refusal.reason(),
                self.error().to_string(),
            ),
        }
    }
}

/// The erased effectful authoring core: every runtime declaration, output
/// contract, replay, and commit check lives here, unchanged by the typed
/// facade (FLOWIP-120z). `pub(crate)` deliberately: the unit tests exercise
/// these defence-in-depth checks through this seam, and no untyped public
/// escape exists.
pub(crate) struct EffectsCore {
    causal: obzenflow_core::event::CausalFrontier,
    ctx: EffectInvocationContext,
    next_effect_ordinal: EffectOrdinal,
    next_output_ordinal: EffectOutputOrdinal,
    routed_output_fact_count: usize,
    committed_facts: Vec<ChainEvent>,
    halt: Option<AuthoringHalt>,
}

impl EffectsCore {
    pub(crate) fn new(ctx: EffectInvocationContext) -> Self {
        Self {
            causal: crate::supervised_base::publication::capture(),
            ctx,
            next_effect_ordinal: EffectOrdinal::new(0),
            next_output_ordinal: EffectOutputOrdinal::new(0),
            routed_output_fact_count: 0,
            committed_facts: Vec::new(),
            halt: None,
        }
    }

    fn incorporate_history(
        &mut self,
        frontier: &obzenflow_core::event::CausalFrontier,
    ) -> Result<(), EffectError> {
        self.causal
            .merge(frontier)
            .map_err(|error| EffectError::ReplayArchive(error.to_string()))?;
        crate::supervised_base::publication::incorporate(frontier)
            .map_err(|error| EffectError::ReplayArchive(error.to_string()))
    }

    fn gate_authoring(&self) -> Result<(), EffectError> {
        match &self.halt {
            Some(halt) => Err(halt.error()),
            None => Ok(()),
        }
    }

    pub(crate) fn ensure_authoring_open(&self) -> Result<(), EffectError> {
        self.gate_authoring()
    }

    fn latch(&mut self, halt: AuthoringHalt) -> EffectError {
        self.halt.get_or_insert(halt).error()
    }

    fn latch_binding_fault(&mut self, fault: BindingAuthorityFault) -> EffectError {
        self.latch(AuthoringHalt::Binding(fault))
    }

    fn refuse(&mut self, refusal: ReconstructionRefusal) -> EffectError {
        self.latch(AuthoringHalt::Reconstruction(refusal))
    }

    fn refuse_descriptor(
        &mut self,
        cursor: &EffectCursor,
        expected: &EffectDescriptorHash,
        recorded: &EffectDescriptorHash,
    ) -> EffectError {
        self.refuse(ReconstructionRefusal::DescriptorMismatch {
            cursor: cursor.clone(),
            expected: expected.clone(),
            recorded: recorded.clone(),
        })
    }

    fn recorded_outcome<T>(&mut self, decoded: Result<T, EffectError>) -> Result<T, EffectError> {
        match decoded {
            Err(error) if !matches!(error, EffectError::RecordedFailure { .. }) => {
                Err(self.refuse(ReconstructionRefusal::UndecodableOutcome(error)))
            }
            other => other,
        }
    }

    fn history_shape<T>(&mut self, checked: Result<T, EffectError>) -> Result<T, EffectError> {
        checked.map_err(|error| self.refuse(ReconstructionRefusal::HistoryShape(error)))
    }

    /// The refusal for a cursor without any recorded attempt, or `None` when
    /// this position performs it live (FLOWIP-134i B13).
    fn unrecorded_effect_error(&mut self, cursor: EffectCursor) -> Option<EffectError> {
        let at = crate::execution::ExecutionPosition {
            stage_id: self.ctx.stage_id,
            position: self.ctx.input_seq,
            // The effect context carries no generation; the effect-miss
            // decision is positional (FLOWIP-120n F7).
            generation: None,
        };
        match self.ctx.runtime_execution.missing_outcome(at) {
            UnrecordedEffect::Execute => None,
            UnrecordedEffect::Corrupt => {
                Some(self.refuse(ReconstructionRefusal::Unrecorded { cursor }))
            }
            UnrecordedEffect::RecordingEnded => {
                Some(self.latch(AuthoringHalt::RecordingEnded(cursor)))
            }
        }
    }

    pub(crate) fn project_named_effect<E: NamedEffect>(
        &mut self,
    ) -> Result<EffectBindingUse<E>, EffectError> {
        self.gate_authoring()?;
        let declaration = match self.ctx.effect_declaration(E::EFFECT_TYPE) {
            Ok(declaration) => declaration,
            Err(_) => {
                let fault = BindingAuthorityFault::binding_mismatch(
                    E::EFFECT_TYPE,
                    None,
                    BindingMismatchKind::Mode,
                );
                return Err(self.latch_binding_fault(fault));
            }
        };
        let logical_name = declaration.binding().logical_name().cloned();
        match declaration.binding().typed_projection::<E>() {
            Some(projection) => Ok(projection),
            None => {
                let fault = BindingAuthorityFault::binding_mismatch(
                    E::EFFECT_TYPE,
                    logical_name,
                    BindingMismatchKind::Mode,
                );
                Err(self.latch_binding_fault(fault))
            }
        }
    }

    pub(crate) fn authoring_halt_fatal(
        &self,
    ) -> Option<crate::stages::common::handler_error::StageFatal> {
        self.halt.as_ref().map(AuthoringHalt::stage_fatal)
    }

    #[cfg(test)]
    pub(crate) fn next_effect_ordinal_for_test(&self) -> EffectOrdinal {
        self.next_effect_ordinal
    }

    /// Evidence of every user fact committed by this invocation, in commit
    /// order: direct emissions, effect-outcome facts, and transactional
    /// commits. Captures never appear (no `Data` fact). Read by
    /// `StageCompletion` construction (FLOWIP-120z).
    pub(crate) fn committed_fact_evidence(&self) -> (usize, Vec<obzenflow_core::EventDescriptor>) {
        let types = self
            .committed_facts
            .iter()
            .map(ChainEvent::descriptor)
            .collect();
        (self.committed_facts.len(), types)
    }

    /// The stage key for completion diagnostics (FLOWIP-120z).
    pub(crate) fn stage_key(&self) -> &str {
        &self.ctx.stage_key
    }

    /// True while the invocation reproduces archived acts: always in a strict
    /// replay, never live, and in a resume while the stage has archived acts
    /// left, unless the only one left is its next effect's in-doubt attempt
    /// (FLOWIP-134k B1, B7).
    pub(crate) fn is_replaying(&self) -> bool {
        let reconstructing =
            self.ctx
                .runtime_execution
                .is_reconstructing(crate::execution::ExecutionPosition {
                    stage_id: self.ctx.stage_id,
                    position: self.ctx.input_seq,
                    generation: None,
                });
        if !reconstructing {
            return false;
        }
        match (
            self.ctx.runtime_execution.resume_control(),
            self.ctx.effect_history.as_ref(),
        ) {
            (Some(_), Some(history)) => history.acts().reproducing(&self.next_cursor()),
            (Some(_), None) => false,
            (None, _) => true,
        }
    }

    /// The namespace this stage's identities mint in (FLOWIP-134k B8): the
    /// one its history records, or this run's own in a live run.
    fn namespace(&self) -> String {
        self.ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id().to_string())
            .unwrap_or_else(|| self.ctx.flow_id.to_string())
    }

    fn next_cursor(&self) -> EffectCursor {
        EffectCursor::new(
            self.namespace(),
            self.ctx.stage_key.clone(),
            self.ctx.input_seq.0,
            self.next_effect_ordinal,
        )
    }

    /// Take `offered` against the stage's archived acts. Only a resume takes
    /// them in order (FLOWIP-134k B7): an act that departs from the archive
    /// before it ends refuses here, before any of its rows or calls.
    fn take_act(
        &mut self,
        offered: super::ActKey,
        offered_act: impl FnOnce() -> String,
    ) -> Result<ActPlan, EffectError> {
        use obzenflow_core::event::StageFatalReason;
        let acts = match (
            self.ctx.runtime_execution.resume_control(),
            self.ctx.effect_history.as_ref(),
        ) {
            (None, _) => return Ok(ActPlan::Unchecked),
            (Some(control), Some(history)) if !control.stage_is_live(self.ctx.stage_id) => {
                history.acts().clone()
            }
            (Some(_), _) => return Ok(ActPlan::Live),
        };
        let fact = matches!(offered, super::ActKey::Fact(_));
        let refusal = match acts.take(&offered) {
            super::ActCheck::Reproduce => return Ok(ActPlan::Reproduce),
            super::ActCheck::Live => return Ok(ActPlan::Live),
            super::ActCheck::Missing { next } => ReconstructionRefusal::Departed {
                reason: if fact {
                    StageFatalReason::MissingRecordedFact
                } else {
                    StageFatalReason::MissingRecordedEffect
                },
                detail: format!(
                    "stage '{}' {}, but its recording's next act is {}",
                    self.ctx.stage_key,
                    offered_act(),
                    next.describe()
                ),
            },
            super::ActCheck::Skipped { skipped } => ReconstructionRefusal::Departed {
                reason: if skipped.is_fact() {
                    StageFatalReason::UnusedFactHistory
                } else {
                    StageFatalReason::UnusedEffectHistory
                },
                detail: format!(
                    "stage '{}' skipped {} its recording holds",
                    self.ctx.stage_key,
                    skipped.describe()
                ),
            },
        };
        Err(self.refuse(refusal))
    }

    /// Cross before the first row of the stage's first live act
    /// (FLOWIP-134k §3): one marker for each generation the stage has not
    /// crossed, so its readers advance one generation at a time (F11).
    async fn cross_before_live_act(&mut self) -> Result<(), EffectError> {
        let Some(control) = self.ctx.runtime_execution.resume_control().cloned() else {
            return Ok(());
        };
        if control.stage_is_live(self.ctx.stage_id) {
            return Ok(());
        }
        let target = control.resume_generation();
        let crossed = self
            .ctx
            .runtime_execution
            .stage_crossings()
            .crossed(self.ctx.stage_id);
        for generation in crossed.0.saturating_add(1)..=target.0 {
            self.author_crossing(obzenflow_core::ReaderGeneration(generation))
                .await?;
        }
        control.record_generation_boundary(self.ctx.stage_id, target);
        tracing::info!(
            stage_name = %self.ctx.stage_key,
            generation = target.0,
            "stage crossed its catch-up boundary before its first live act"
        );
        Ok(())
    }

    /// Re-author each crossing the archive recorded before `row`, at its
    /// recorded generation, unless the stage already crossed it here
    /// (FLOWIP-134k B2).
    async fn reauthor_recorded_crossings(&mut self, row: super::ActRow) -> Result<(), EffectError> {
        let Some(history) = self.ctx.effect_history.clone() else {
            return Ok(());
        };
        for generation in history.acts().crossings_before(&row) {
            let crossed = self
                .ctx
                .runtime_execution
                .stage_crossings()
                .crossed(self.ctx.stage_id);
            if generation <= crossed {
                continue;
            }
            self.author_crossing(generation).await?;
            if let Some(control) = self.ctx.runtime_execution.resume_control() {
                control.record_generation_boundary(self.ctx.stage_id, generation);
            }
        }
        Ok(())
    }

    async fn author_crossing(
        &self,
        generation: obzenflow_core::ReaderGeneration,
    ) -> Result<(), EffectError> {
        let mut marker = ChainEventFactory::catch_up_complete_event(
            self.ctx.writer_id,
            generation,
            obzenflow_core::StageKey::from(self.ctx.stage_key.clone()),
        );
        if let Some(flow_context) = &self.ctx.flow_context {
            marker.flow_context = flow_context.clone();
        }
        let committed = match self.ctx.instrumentation.as_ref() {
            Some(instrumentation) => crate::supervised_base::publication::with_snapshot(
                self.causal.clone(),
                crate::stages::common::supervision::output_committer::commit_control_output(
                    &self.ctx.data_journal,
                    instrumentation,
                    marker,
                ),
            )
            .await
            .map(drop)
            .map_err(|error| error.to_string()),
            None => crate::supervised_base::publication::with_snapshot(
                self.causal.clone(),
                crate::supervised_base::publication::append(
                    &self.ctx.data_journal,
                    marker,
                    obzenflow_core::journal::AppendOptions::default(),
                ),
            )
            .await
            .map(drop)
            .map_err(|error| error.to_string()),
        };
        committed.map_err(EffectError::Journal)?;
        self.ctx
            .runtime_execution
            .stage_crossings()
            .advance(self.ctx.stage_id, generation);
        Ok(())
    }

    /// Apply `lineage` to a row about to be committed (FLOWIP-134k §4). A
    /// reproduced row names the archived row it copies, by the identity it
    /// keeps, and the run and stage that recorded it.
    fn mark_lineage(&self, event: &mut ChainEvent, lineage: RowLineage<'_>) {
        match lineage {
            RowLineage::Live => event.replay_context = None,
            RowLineage::Reproduced(history) => {
                let archived = history.and_then(|history| {
                    history
                        .terminal_group_events
                        .iter()
                        .chain(history.attempt_events.values())
                        .chain(history.escape_control_batches.values().flatten())
                        .find(|archived| archived.id == event.id)
                });
                let (original_flow_id, original_stage_id) = match archived {
                    Some(archived) => (
                        archived.flow_context.flow_id.clone(),
                        archived.flow_context.stage_id,
                    ),
                    None => match self.ctx.runtime_execution.archive_for_io() {
                        Some(archive) => (
                            archive.archive_flow_id().to_string(),
                            archive
                                .archived_stage_id(&self.ctx.stage_key)
                                .unwrap_or(self.ctx.stage_id),
                        ),
                        None => (self.ctx.flow_id.to_string(), self.ctx.stage_id),
                    },
                };
                event.replay_context = Some(obzenflow_core::event::provenance::ReplayContext {
                    original_event_id: event.id,
                    original_flow_id,
                    original_stage_id,
                });
            }
        }
    }

    pub(crate) fn drain_committed_facts(&mut self) -> Vec<ChainEvent> {
        std::mem::take(&mut self.committed_facts)
    }

    pub(crate) async fn preflight_next_effect_cursor_is_empty(&self) -> Result<(), EffectError> {
        let (cursor, selection) = self.next_cursor_selection().await?;
        match selection {
            EffectHistorySelection::Miss => Ok(()),
            EffectHistorySelection::Hit(_) => Err(EffectError::EffectProvenanceMismatch(format!(
                "pre-effect failure at cursor {cursor:?} would replace an existing terminal"
            ))),
            EffectHistorySelection::InDoubt(attempts) => {
                let highest = attempts
                    .last()
                    .ok_or_else(|| {
                        EffectError::EffectProvenanceMismatch(format!(
                            "effect cursor {cursor:?} selected InDoubt without an attempt"
                        ))
                    })?
                    .attempt;
                Err(EffectError::EffectProvenanceMismatch(format!(
                    "pre-effect failure at cursor {cursor:?} would erase in-doubt Start({highest})"
                )))
            }
        }
    }

    pub(crate) async fn settlement_divergence(
        &self,
    ) -> Option<crate::stages::common::handler_error::StageFatal> {
        use obzenflow_core::event::StageFatalReason;
        let resumed_next = match (
            self.ctx.runtime_execution.resume_control(),
            self.ctx.effect_history.as_ref(),
        ) {
            (Some(_), Some(history)) => history.acts().remaining().into_iter().next(),
            _ => None,
        };
        let (reason, detail) = match self.next_cursor_selection().await {
            Ok((_, EffectHistorySelection::Miss)) => return None,
            // A resume names the archived act it skipped (FLOWIP-134k B7).
            Ok((_, _)) if resumed_next.is_some() => {
                let skipped = resumed_next.expect("checked above");
                (
                    if skipped.is_fact() {
                        StageFatalReason::UnusedFactHistory
                    } else {
                        StageFatalReason::UnusedEffectHistory
                    },
                    format!(
                        "stage '{}' skipped {} its recording holds",
                        self.ctx.stage_key,
                        skipped.describe()
                    ),
                )
            }
            Ok((cursor, _)) => (
                StageFatalReason::UnusedEffectHistory,
                format!("the rebuild skipped the recorded effect at cursor {cursor:?}"),
            ),
            Err(error) => (StageFatalReason::ReplayDivergence, error.to_string()),
        };
        Some(crate::stages::common::handler_error::StageFatal::new(
            obzenflow_core::event::StageFatalCode::Replay,
            reason,
            detail,
        ))
    }

    async fn next_cursor_selection(
        &self,
    ) -> Result<(EffectCursor, EffectHistorySelection), EffectError> {
        self.gate_authoring()?;
        let cursor = self.next_cursor();
        let archived = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.cursor_history(&cursor))
            .unwrap_or_default();
        let current = current_cursor_history(&self.ctx.data_journal, &cursor).await?;
        let selected = merge_cursor_histories(&cursor, archived, current)?;
        crate::supervised_base::publication::incorporate(&selected.causal)
            .map_err(|error| EffectError::ReplayArchive(error.to_string()))?;
        Ok((cursor, selected.select()))
    }

    pub(crate) async fn request_generated_live_admission(&self) -> Result<(), EffectError> {
        self.gate_authoring()?;
        if self.is_replaying() {
            return Ok(());
        }
        if let Some(admission) = self.ctx.backpressure_writer.direct_fact_admission() {
            admission
                .request_live()
                .await
                .map_err(EffectError::Execution)?;
        }
        Ok(())
    }

    fn observe_effect_outcome(
        &self,
        effect_type: &str,
        outcome: crate::stages::observer::EffectObserverOutcome,
    ) {
        let Some(observers) = self.ctx.observers.as_ref() else {
            return;
        };
        if observers.effect().is_none() {
            return;
        }
        crate::stages::observer::dispatch::run_effect_observers(
            observers,
            self.ctx.flow_id,
            self.ctx.stage_id,
            &self.ctx.stage_key,
            obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
            effect_type,
            outcome,
        );
    }

    fn observe_effect_result<T>(&self, effect_type: &str, result: &Result<T, EffectError>) {
        let outcome = match result {
            Ok(_) => crate::stages::observer::EffectObserverOutcome::Succeeded,
            Err(_) => crate::stages::observer::EffectObserverOutcome::Failed,
        };
        self.observe_effect_outcome(effect_type, outcome);
    }

    fn reserve_effect_ordinal(&mut self) -> Result<EffectOrdinal, EffectError> {
        let effect_ordinal = self.next_effect_ordinal;
        self.next_effect_ordinal = EffectOrdinal::new(
            self.next_effect_ordinal
                .get()
                .checked_add(1)
                .ok_or_else(|| EffectError::Execution("effect ordinal overflow".to_string()))?,
        );
        Ok(effect_ordinal)
    }

    fn reserve_output_ordinal(&mut self) -> Result<EffectOutputOrdinal, EffectError> {
        let output_ordinal = self.next_output_ordinal;
        self.next_output_ordinal = self
            .next_output_ordinal
            .checked_add(1)
            .ok_or_else(|| EffectError::Execution("effect output ordinal overflow".to_string()))?;
        Ok(output_ordinal)
    }

    fn reserve_output_ordinals(
        &mut self,
        count: usize,
    ) -> Result<EffectOutputOrdinal, EffectError> {
        let count = u32::try_from(count).map_err(|_| {
            EffectError::Execution("effect output fact count exceeds u32 range".to_string())
        })?;
        let output_ordinal = self.next_output_ordinal;
        self.next_output_ordinal = output_ordinal
            .checked_add(count)
            .ok_or_else(|| EffectError::Execution("effect output ordinal overflow".to_string()))?;
        Ok(output_ordinal)
    }

    fn advance_output_ordinals_after_reserved_base(
        &mut self,
        reserved_base: EffectOutputOrdinal,
        fact_count: usize,
    ) -> Result<(), EffectError> {
        let fact_count = u32::try_from(fact_count).map_err(|_| {
            EffectError::Execution("effect output fact count exceeds u32 range".to_string())
        })?;
        if fact_count == 0 {
            return Ok(());
        }
        let next = reserved_base
            .checked_add(fact_count)
            .ok_or_else(|| EffectError::Execution("effect output ordinal overflow".to_string()))?;
        if self.next_output_ordinal < next {
            self.next_output_ordinal = next;
        }
        Ok(())
    }

    fn ensure_routed_fanout_capacity(&self, additional_routed: usize) -> Result<(), EffectError> {
        if additional_routed == 0 {
            return Ok(());
        }

        let limit = self.ctx.output_contract.routable_member_count();
        if limit == 0 {
            return Ok(());
        }

        let next = self
            .routed_output_fact_count
            .checked_add(additional_routed)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        if next > limit {
            return Err(EffectError::Execution(format!(
                "stage `{}` authored {next} routed facts for one input, exceeding the FLOWIP-120b v1 bounded fanout limit of {limit} routable output contract members",
                self.ctx.stage_key
            )));
        }

        Ok(())
    }

    fn count_routed_facts(&self, facts: &[TypedFact]) -> usize {
        facts
            .iter()
            .filter(|fact| {
                is_routable_output_fact(Some(&self.ctx.output_contract), &fact.descriptor())
            })
            .count()
    }

    pub(crate) async fn emit<T>(&mut self, fact: T) -> Result<(), EffectError>
    where
        T: TypedPayload,
    {
        self.emit_fact(fact, false).await
    }

    /// Emit one fact before the invocation's first effect, requesting live
    /// admission only when the fact itself is live (FLOWIP-134k B1).
    pub(crate) async fn emit_pre_effect<T>(&mut self, fact: T) -> Result<(), EffectError>
    where
        T: TypedPayload,
    {
        self.emit_fact(fact, true).await
    }

    async fn emit_fact<T>(&mut self, fact: T, pre_effect: bool) -> Result<(), EffectError>
    where
        T: TypedPayload,
    {
        self.gate_authoring()?;
        if !self.ctx.emit_enabled {
            return Err(EffectError::EmitUnsupported {
                stage_key: self.ctx.stage_key.clone(),
            });
        }

        let event_type = T::event_type_name();
        if !self.ctx.output_contract.is_empty()
            && !self
                .ctx
                .output_contract
                .contains_descriptor(&T::descriptor())
        {
            return Err(EffectError::UndeclaredOutput {
                stage_key: self.ctx.stage_key.clone(),
                event_type,
            });
        }
        let routed_fact =
            is_routable_output_fact(Some(&self.ctx.output_contract), &T::descriptor()) as usize;
        self.ensure_routed_fanout_capacity(routed_fact)?;

        let recorded_flow_id = self.namespace();
        let output_ordinal = self.reserve_output_ordinal()?;
        let mut event = deterministic_typed_output_event(
            self.ctx.writer_id,
            &self.ctx.parent.authored(),
            fact,
            &recorded_flow_id,
            &self.ctx.stage_key,
            self.ctx.input_seq,
            output_ordinal,
            self.ctx.lineage,
        )?;
        let input = self.ctx.input_seq.0;
        let plan = self.take_act(super::ActKey::Fact(event.id), || {
            format!(
                "emitted {} at input {input}",
                super::acts::display_name(&event_type)
            )
        })?;
        // A fact the archive holds is rematerialized and says so; a live
        // fact carries no replay provenance; any other computed from
        // recorded input keeps its input's (FLOWIP-134k §4).
        let held = self
            .ctx
            .effect_history
            .as_ref()
            .is_some_and(|history| history.acts().holds_fact(&event.id));
        let replaying = self.is_replaying();
        let lineage = match plan {
            ActPlan::Reproduce => Some(RowLineage::Reproduced(None)),
            ActPlan::Live => {
                self.cross_before_live_act().await?;
                Some(RowLineage::Live)
            }
            ActPlan::Unchecked if replaying && held => Some(RowLineage::Reproduced(None)),
            ActPlan::Unchecked if !replaying => Some(RowLineage::Live),
            ActPlan::Unchecked => None,
        };
        if matches!(lineage, Some(RowLineage::Reproduced(_))) {
            self.reauthor_recorded_crossings(super::ActRow::Fact(event.id))
                .await?;
        }
        if pre_effect && matches!(lineage, Some(RowLineage::Live)) {
            self.request_generated_live_admission().await?;
        }
        if let Some(lineage) = lineage {
            self.mark_lineage(&mut event, lineage);
        }
        let committed_event = event.clone();
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),

            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.output_contract),
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        };
        crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            committer.commit_prebuilt(
                event,
                Some(&self.ctx.parent),
                CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
            ),
        )
        .await
        .map_err(|e| EffectError::Journal(e.to_string()))?;
        self.routed_output_fact_count = self
            .routed_output_fact_count
            .checked_add(routed_fact)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        self.committed_facts.push(committed_event);
        Ok(())
    }

    pub(crate) async fn perform<E>(&mut self, effect: E) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        self.gate_authoring()?;
        let declaration = self.ctx.effect_declaration(E::EFFECT_TYPE)?;
        let invocation = <E::BindingMode as EffectBindingMode<E>>::invocation_binding(&effect);
        if let Err(mismatch) = declaration.binding().compare_invocation(&invocation) {
            let kind = match mismatch {
                super::binding::BindingMatchError::Mode => BindingMismatchKind::Mode,
                super::binding::BindingMatchError::Family => {
                    BindingMismatchKind::ConstructionFamily
                }
                super::binding::BindingMatchError::Evidence => BindingMismatchKind::Evidence,
            };
            let fault = BindingAuthorityFault::binding_mismatch(
                E::EFFECT_TYPE,
                declaration.binding().logical_name().cloned(),
                kind,
            );
            return Err(self.latch_binding_fault(fault));
        }

        let result = Box::pin(self.perform_authorised(effect, declaration.clone())).await;
        if let Err(error) = &result {
            if let Some(fault) = self.binding_fault_for_error::<E>(&declaration, error) {
                return Err(self.latch_binding_fault(fault));
            }
        }
        result
    }

    async fn perform_authorised<E>(
        &mut self,
        effect: E,
        declaration: EffectDeclaration,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let safety = declaration.safety();
        // FLOWIP-120c G10: the missing-key check is a deterministic
        // validation error, so it sits above the effect-history lookup and
        // the boundary consult. Live and replay recompute the same error,
        // nothing is recorded under the cursor, and admission is never
        // charged for a call that can never execute.
        if matches!(safety, EffectSafety::NonIdempotentRequiresKey)
            && effect.idempotency_key().is_none()
        {
            return Err(EffectError::MissingIdempotencyKey {
                effect_type: E::EFFECT_TYPE.to_string(),
            });
        }
        let effect_ordinal = self.reserve_effect_ordinal()?;

        let descriptor = descriptor_for_effect(
            &effect,
            self.ctx.stage_logic_version.clone(),
            E::EFFECT_TYPE,
            E::SCHEMA_VERSION,
            declaration.binding_identity(),
        )?;
        let descriptor_hash = descriptor_hash(&descriptor)?;
        let cursor = EffectCursor::new(
            self.namespace(),
            self.ctx.stage_key.clone(),
            self.ctx.input_seq.0,
            effect_ordinal,
        );
        let input = self.ctx.input_seq.0;
        let plan = self.take_act(super::ActKey::Effect(cursor.clone()), || {
            format!(
                "would perform {} at input {input}",
                super::acts::display_name(E::EFFECT_TYPE)
            )
        })?;
        let cursor_coordinator = self
            .ctx
            .runtime_execution
            .effect_cursor_coordinator()
            .clone();
        let _cursor_guard = cursor_coordinator.lock(&cursor).await?;
        let mut prior_attempts = Vec::new();
        let archived = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.cursor_history(&cursor))
            .unwrap_or_default();
        let current = current_cursor_history(&self.ctx.data_journal, &cursor).await?;
        let selected = self.history_shape(merge_cursor_histories(&cursor, archived, current))?;
        let incorporated = self.incorporate_history(&selected.causal);
        self.history_shape(incorporated)?;
        if matches!(safety, EffectSafety::NonIdempotentAtLeastOnce) {
            self.history_shape(validate_affine_terminal_group(&cursor, &selected))?;
        }

        let selection = selected.select();
        if let Some(refusal) = act_plan_mismatch(plan, &selection, &cursor) {
            return Err(self.refuse(refusal));
        }
        match selection {
            EffectHistorySelection::Hit(records) => {
                for started in &selected.attempts {
                    if started.descriptor_hash != descriptor_hash
                        || started.effect_type.as_str() != E::EFFECT_TYPE
                    {
                        return Err(self.refuse_descriptor(
                            &cursor,
                            &descriptor_hash,
                            &started.descriptor_hash,
                        ));
                    }
                }
                let record_refs = records.iter().collect::<Vec<_>>();
                for record in &record_refs {
                    if record.descriptor_hash != descriptor_hash {
                        return Err(self.refuse_descriptor(
                            &cursor,
                            &descriptor_hash,
                            &record.descriptor_hash,
                        ));
                    }
                }
                let output_result =
                    self.recorded_outcome(E::OutcomeSemantics::decode_success(&record_refs));
                if output_result
                    .as_ref()
                    .is_err_and(|error| !matches!(error, EffectError::RecordedFailure { .. }))
                {
                    return output_result;
                }
                let materialization =
                    self.history_shape(effect_record_group_materialization(&record_refs))?;
                if selected.attempts.is_empty()
                    && selected.abandonment.is_none()
                    && selected.terminal_group_events.is_empty()
                {
                    // Records supplied through the legacy record-only history
                    // API carry no physical-group envelope. Reconstruct them
                    // through the existing per-record path rather than
                    // inventing an atomic cut the archive never proved.
                    self.append_replayed_records(
                        cursor.clone(),
                        descriptor_hash,
                        descriptor,
                        materialization,
                    )
                    .await?;
                } else {
                    self.append_replayed_history(
                        &selected,
                        cursor.clone(),
                        descriptor_hash,
                        descriptor,
                        materialization,
                    )
                    .await?;
                }
                if let Some(abandoned) = selected.abandonment.as_ref() {
                    return Err(EffectError::RecoveryAbandoned {
                        last_started_attempt: abandoned.highest_started_attempt,
                        failure_source: abandoned.cause.source.clone(),
                        code: abandoned.cause.code.clone(),
                        message: abandoned.message.clone(),
                        boundary_retry: abandoned.retry,
                    });
                }
                return output_result;
            }
            EffectHistorySelection::InDoubt(attempts) => {
                if !matches!(safety, EffectSafety::NonIdempotentAtLeastOnce) {
                    return Err(self.refuse(ReconstructionRefusal::HistoryShape(
                        EffectError::EffectProvenanceMismatch(format!(
                            "effect cursor {cursor:?} has attempt history but effect '{}' is not NonIdempotentAtLeastOnce",
                            E::EFFECT_TYPE
                        )),
                    )));
                }
                for started in &attempts {
                    if started.descriptor_hash != descriptor_hash
                        || started.effect_type.as_str() != E::EFFECT_TYPE
                    {
                        return Err(self.refuse_descriptor(
                            &cursor,
                            &descriptor_hash,
                            &started.descriptor_hash,
                        ));
                    }
                }
                let Some(highest_started_attempt) = attempts.last().map(|started| started.attempt)
                else {
                    return Err(self.refuse(ReconstructionRefusal::HistoryShape(
                        EffectError::EffectProvenanceMismatch(format!(
                            "effect cursor {cursor:?} selected InDoubt without an attempt"
                        )),
                    )));
                };
                if !self.ctx.runtime_execution.in_doubt_authorizes_attempt() {
                    tracing::debug!(
                        ?cursor,
                        highest_started_attempt = highest_started_attempt.get(),
                        "replay recording ends inside an attempt"
                    );
                    return Err(self.latch(AuthoringHalt::RecordingEnded(cursor)));
                }
                self.append_replayed_prefix(&cursor, &selected, descriptor.clone())
                    .await?;
                // The re-attempt is the stage's first live act (B1).
                self.cross_before_live_act().await?;
                prior_attempts = attempts;
            }
            EffectHistorySelection::Miss => match plan {
                ActPlan::Live => self.cross_before_live_act().await?,
                ActPlan::Unchecked | ActPlan::Reproduce => {
                    if let Some(error) = self.unrecorded_effect_error(cursor.clone()) {
                        return Err(error);
                    }
                }
            },
        }

        // A replay hit above never requests generated live credit or touches
        // a resolver. A selected live continuation first crosses the outer
        // generated admission barrier, then constructs live authority and
        // performs the effect-specific binding check.
        if let Some(admission) = self.ctx.backpressure_writer.direct_fact_admission() {
            admission
                .request_live()
                .await
                .map_err(EffectError::Execution)?;
        }

        let (metadata_context, binding_context) = self.live_effect_contexts(&declaration)?;
        effect.validate_port_metadata(&metadata_context)?;

        let prepared = PreparedLiveEffect {
            identity: EffectIdentity {
                effect_type: E::EFFECT_TYPE,
                safety,
                cursor: cursor.clone(),
                idempotency_key: effect.idempotency_key(),
            },
            cursor,
            descriptor_hash,
            descriptor,
            binding_context,
        };

        if matches!(safety, EffectSafety::NonIdempotentAtLeastOnce) {
            // Keep the generated affine state machine off the caller's async
            // frame. `perform` is monomorphised for every effect safety class;
            // storing this large future inline also bloats ordinary
            // transactional/repeatable handler futures enough to exhaust the
            // stage task stack before their own branch runs.
            return Box::pin(self.perform_affine(effect, prepared, prior_attempts)).await;
        }

        if matches!(safety, EffectSafety::Transactional) {
            // The transactional boundary owns another sizeable single-use
            // future. Heap-pin it for the same frame-containment reason.
            return Box::pin(self.perform_transactional(effect, prepared)).await;
        }

        let PreparedLiveEffect {
            identity,
            cursor,
            descriptor_hash,
            descriptor,
            binding_context,
        } = prepared;

        let Some(boundary) = self.ctx.effect_boundary.clone() else {
            // Unguarded path: execute directly and record the outcome.
            let mut effect_ctx = binding_context;
            return match Self::execute_into_success(effect, &mut effect_ctx).await {
                Ok((output, success)) => {
                    self.append_success(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        success,
                        Some(EffectFactOrigin::Effect),
                    )
                    .await?;
                    self.observe_effect_outcome(
                        E::EFFECT_TYPE,
                        crate::stages::observer::EffectObserverOutcome::Succeeded,
                    );
                    Ok(output)
                }
                Err((err, observation)) => {
                    self.append_failed_record(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        observation,
                        &err,
                    )
                    .await?;
                    self.observe_effect_outcome(
                        E::EFFECT_TYPE,
                        crate::stages::observer::EffectObserverOutcome::Failed,
                    );
                    Err(err)
                }
            };
        };

        // Policy path: hand the boundary a policy-neutral
        // callable for one physical call. Each call clones the effect and a
        // pristine context, while the terminal successful outcome rides the
        // slot so only `perform` can record it after the boundary returns.
        let outcome_slot: ExecutedOutcomeSlot<E::Outcome> = Arc::new(Mutex::new(None));
        let failure_slot = Arc::new(Mutex::new(None::<(EffectError, EffectObservation)>));
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let operation = {
            let started = started.clone();
            let slot = outcome_slot.clone();
            let failure = failure_slot.clone();
            let writer_id = self.ctx.writer_id;
            let parent_event = self.ctx.parent.authored();
            let lineage = self.ctx.lineage;
            let base_context = binding_context;
            RepeatableEffectOperation::new_with_lifecycle(move |lifecycle| {
                let started = started.clone();
                let effect = effect.clone();
                let mut effect_ctx = base_context.clone();
                let slot = slot.clone();
                let failure = failure.clone();
                let parent_event = parent_event.clone();
                async move {
                    *slot.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    started.store(true, std::sync::atomic::Ordering::Release);
                    *failure.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    lifecycle.mark_started();
                    let output = match effect.execute(&mut effect_ctx).await {
                        Ok(output) => {
                            lifecycle.mark_completed(PhysicalCallOutcome::Succeeded);
                            output
                        }
                        Err(err) => {
                            lifecycle.mark_completed(PhysicalCallOutcome::Failed);
                            *failure.lock().unwrap_or_else(|p| p.into_inner()) =
                                Some((err.clone(), EffectObservation::returned_failure()));
                            return Err(err);
                        }
                    };
                    let success =
                        E::OutcomeSemantics::prepare_success(&output).inspect_err(|error| {
                            *failure.lock().unwrap_or_else(|p| p.into_inner()) = Some((
                                error.clone(),
                                EffectObservation::OutcomePreparationFailed {},
                            ));
                        })?;
                    *failure.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    let observation =
                        success_observation_events(&success, writer_id, &parent_event, lineage);
                    *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some((output, success));
                    Ok(observation)
                }
            })
        };

        let report = boundary
            .around_repeatable_effect(&identity, &self.ctx.parent.authored(), operation)
            .await;
        let control_events = report.control_events;

        let observed_failure = failure_slot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        // Runtime-owned evidence is authoritative. A refusal after a completed
        // attempt cannot turn that attempt into a zero-call rejection. Dropping
        // an in-flight future leaves no terminal, just as interruption does.
        let outcome = if outcome_slot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            EffectBoundaryOutcome::Executed(Ok(Vec::new()))
        } else if let Some((error, _)) = &observed_failure {
            EffectBoundaryOutcome::Executed(Err(error.clone()))
        } else if started.load(std::sync::atomic::Ordering::Acquire) {
            return Err(EffectError::Execution(
                "effect execution was interrupted without a completed outcome".to_string(),
            ));
        } else {
            report.outcome
        };
        match outcome {
            EffectBoundaryOutcome::Executed(Ok(_observation)) => {
                let (output, success) = outcome_slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                    .ok_or_else(|| {
                        EffectError::Execution(
                            "effect boundary reported success without an executed outcome"
                                .to_string(),
                        )
                    })?;
                self.append_success_with_control_events(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    success,
                    Some(EffectFactOrigin::Effect),
                    control_events,
                )
                .await?;
                self.observe_effect_outcome(
                    E::EFFECT_TYPE,
                    crate::stages::observer::EffectObserverOutcome::Succeeded,
                );
                Ok(output)
            }
            EffectBoundaryOutcome::Executed(Err(reported_error)) => {
                let (err, observation) = observed_failure
                    .unwrap_or((reported_error, EffectObservation::ExecutionRejected {}));
                self.append_failed_record_with_control_events(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    observation,
                    &err,
                    control_events,
                )
                .await?;
                self.observe_effect_outcome(
                    E::EFFECT_TYPE,
                    crate::stages::observer::EffectObserverOutcome::Failed,
                );
                Err(err)
            }
            EffectBoundaryOutcome::Aborted(reason) => {
                let result = self
                    .record_boundary_abort_with_control_events(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        reason,
                        control_events,
                    )
                    .await;
                if result.is_err() {
                    self.observe_effect_outcome(
                        E::EFFECT_TYPE,
                        crate::stages::observer::EffectObserverOutcome::Failed,
                    );
                }
                result
            }
        }
    }

    /// Execute an effect and prepare its mode-specific durable success.
    ///
    /// Both the empty-outcome and decomposition failures depend on the live
    /// external result, so callers record them under the effect cursor like
    /// any other execution failure and strict replay reproduces them.
    async fn execute_into_success<E>(
        effect: E,
        effect_ctx: &mut EffectContext,
    ) -> Result<(E::Outcome, PreparedEffectSuccess), (EffectError, EffectObservation)>
    where
        E: Effect,
    {
        let output = effect
            .execute(effect_ctx)
            .await
            .map_err(|error| (error, EffectObservation::returned_failure()))?;
        let success = E::OutcomeSemantics::prepare_success(&output)
            .map_err(|error| (error, EffectObservation::OutcomePreparationFailed {}))?;
        Ok((output, success))
    }

    async fn perform_affine<E>(
        &mut self,
        effect: E,
        prepared: PreparedLiveEffect,
        prior_attempts: Vec<EffectAttemptStarted>,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let PreparedLiveEffect {
            identity,
            cursor,
            descriptor_hash,
            descriptor,
            binding_context,
        } = prepared;
        let highest_prior_attempt = u32::try_from(prior_attempts.len()).map_err(|_| {
            EffectError::EffectProvenanceMismatch(
                "effect attempt history exceeds u32 range".to_string(),
            )
        })?;
        let recovery_causal_input_id = prior_attempts
            .first()
            .map(|started| started.causal_input_id);
        let next_attempt = highest_prior_attempt
            .checked_add(1)
            .ok_or_else(|| EffectError::Execution("effect attempt ordinal overflow".to_string()))?;
        let attempt = EffectAttemptOrdinal::new(next_attempt);
        let outcome_group_id = effect_outcome_group_id(&cursor);
        let started = EffectAttemptStarted {
            cursor: cursor.clone(),
            descriptor_hash: descriptor_hash.clone(),
            effect_type: EffectType::new(E::EFFECT_TYPE),
            attempt,
            outcome_group_id: outcome_group_id.clone(),
            causal_input_id: self.ctx.parent.envelope.provenance.event.id,
        };
        let mut start_event = build_effect_attempt_started_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            started,
            descriptor.clone(),
            self.ctx.lineage,
        )?;
        self.mark_lineage(&mut start_event, RowLineage::Live);

        let outcome_slot: ExecutedOutcomeSlot<E::Outcome> = Arc::new(Mutex::new(None));
        let failure_slot = Arc::new(Mutex::new(None::<(EffectError, EffectObservation)>));
        let start_committed = Arc::new(AtomicBool::new(false));
        let operation = {
            let slot = outcome_slot.clone();
            let failure = failure_slot.clone();
            let start_committed = start_committed.clone();
            let data_journal = self.ctx.data_journal.clone();
            let flow_context = self.ctx.flow_context.clone();
            let instrumentation = self.ctx.instrumentation.clone();
            let heartbeat_state = self.ctx.heartbeat_state.clone();
            let backpressure_writer = self.ctx.backpressure_writer.clone();
            let parent = self.ctx.parent.clone();
            let writer_id = self.ctx.writer_id;
            let parent_event = self.ctx.parent.authored();
            let lineage = self.ctx.lineage;
            let base_context = binding_context;
            let causal = self.causal.clone();
            AffineEffectOperation::new_with_lifecycle(
                highest_prior_attempt,
                move |lifecycle| async move {
                    let committer = OutputCommitter {
                        data_journal: &data_journal,
                        flow_context: flow_context.as_ref(),

                        instrumentation: instrumentation.as_ref(),
                        heartbeat_state: heartbeat_state.as_ref(),
                        output_contract: None,
                        backpressure_writer: Some(&backpressure_writer),
                        observer_scope:
                            obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
                    };
                    crate::supervised_base::publication::with_snapshot(
                        causal,
                        committer.commit_prebuilt_with_intent(
                            start_event,
                            Some(&parent),
                            CommitOptions::default(),
                            StageAppendIntent::NonDataStageFact,
                        ),
                    )
                    .await
                    .map_err(|error| EffectError::Journal(error.to_string()))?;
                    start_committed.store(true, Ordering::Release);

                    *slot.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    lifecycle.mark_started();
                    let mut effect_ctx = base_context;
                    let output = match effect.execute(&mut effect_ctx).await {
                        Ok(output) => {
                            lifecycle.mark_completed(PhysicalCallOutcome::Succeeded);
                            output
                        }
                        Err(error) => {
                            lifecycle.mark_completed(PhysicalCallOutcome::Failed);
                            *failure.lock().unwrap_or_else(|p| p.into_inner()) =
                                Some((error.clone(), EffectObservation::returned_failure()));
                            return Err(error);
                        }
                    };
                    let success =
                        E::OutcomeSemantics::prepare_success(&output).inspect_err(|error| {
                            *failure.lock().unwrap_or_else(|p| p.into_inner()) = Some((
                                error.clone(),
                                EffectObservation::OutcomePreparationFailed {},
                            ));
                        })?;
                    *failure.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    let observation =
                        success_observation_events(&success, writer_id, &parent_event, lineage);
                    *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some((output, success));
                    Ok(observation)
                },
            )
        };
        let provenance = operation.provenance();
        let report = match self.ctx.effect_boundary.clone() {
            Some(boundary) => {
                boundary
                    .around_affine_effect(&identity, &self.ctx.parent.authored(), operation)
                    .await
            }
            None => operation.execute().await.into_report(Vec::new()),
        };
        let (boundary_outcome, control_events) = report.into_parts(&provenance)?;

        match boundary_outcome {
            SingleUseEffectBoundaryOutcome::Executed(execution) => {
                let result = execution.result();
                if !start_committed.load(Ordering::Acquire) {
                    let error = match result {
                        Err(error) => error.clone(),
                        Ok(_) => EffectError::Execution(
                            "affine executor returned without a durable attempt start".to_string(),
                        ),
                    };
                    // A failed Start publication authorises neither the
                    // executor nor an effect terminal. Publishing a failure
                    // here would create a terminal without its required
                    // Start and turn a journal cut into a false known result.
                    self.ctx
                        .runtime_execution
                        .effect_cursor_coordinator()
                        .poison(cursor);
                    drop(control_events);
                    return Err(error);
                }

                match result {
                    Ok(_) => {
                        let (output, success) = outcome_slot
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .take()
                            .ok_or_else(|| {
                                EffectError::Execution(
                                    "affine effect boundary reported success without an executed outcome"
                                        .to_string(),
                                )
                            })?;
                        self.append_affine_success(
                            cursor,
                            descriptor_hash,
                            descriptor,
                            success,
                            attempt,
                            control_events,
                        )
                        .await?;
                        self.observe_effect_outcome(
                            E::EFFECT_TYPE,
                            crate::stages::observer::EffectObserverOutcome::Succeeded,
                        );
                        Ok(output)
                    }
                    Err(error) => {
                        let observation = failure_slot
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_ref()
                            .map(|(_, observation)| observation.clone())
                            .ok_or_else(|| {
                                EffectError::Execution(
                                    "affine failure is missing invocation evidence".to_string(),
                                )
                            })?;
                        self.append_affine_failed_record(
                            cursor,
                            descriptor_hash,
                            descriptor,
                            (observation, error),
                            attempt,
                            control_events,
                        )
                        .await?;
                        self.observe_effect_outcome(
                            E::EFFECT_TYPE,
                            crate::stages::observer::EffectObserverOutcome::Failed,
                        );
                        Err(error.clone())
                    }
                }
            }
            SingleUseEffectBoundaryOutcome::Aborted(reason) => {
                let result = if highest_prior_attempt == 0 {
                    self.record_boundary_abort_with_control_events(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        reason,
                        control_events,
                    )
                    .await
                } else {
                    let causal_input_id = recovery_causal_input_id.ok_or_else(|| {
                        EffectError::EffectProvenanceMismatch(format!(
                            "effect cursor {cursor:?} selected recovery abandonment without an archived Start identity"
                        ))
                    })?;
                    self.record_recovery_abandonment(RecoveryAbandonment {
                        cursor,
                        descriptor_hash,
                        descriptor,
                        highest_started_attempt: EffectAttemptOrdinal::new(highest_prior_attempt),
                        causal_input_id,
                        reason,
                        control_events,
                    })
                    .await
                };
                self.observe_effect_result(E::EFFECT_TYPE, &result);
                result
            }
        }
    }

    async fn commit_framework_effect_event(&self, event: ChainEvent) -> Result<(), EffectError> {
        let cursor = event
            .effect_provenance
            .as_ref()
            .map(|provenance| provenance.cursor.clone());
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),

            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: None,
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        };
        if let Err(error) = crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            committer.commit_prebuilt_with_intent(
                event,
                Some(&self.ctx.parent),
                CommitOptions::default(),
                StageAppendIntent::NonDataStageFact,
            ),
        )
        .await
        {
            if let Some(cursor) = cursor {
                self.ctx
                    .runtime_execution
                    .effect_cursor_coordinator()
                    .poison(cursor);
            }
            return Err(EffectError::Journal(error.to_string()));
        }
        Ok(())
    }

    async fn append_affine_success(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        success: PreparedEffectSuccess,
        attempt: EffectAttemptOrdinal,
        control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        let facts = match success {
            PreparedEffectSuccess::DomainFacts(facts) => facts,
            PreparedEffectSuccess::RecordedReply(output) => {
                let record = EffectRecord {
                    observation: EffectObservation::returned_success(),
                    cursor: cursor.clone(),
                    descriptor_hash,
                    descriptor,
                    outcome: EffectOutcomePayload::Succeeded { output },
                    origin: None,
                };
                let mut event = build_effect_record_event(
                    self.ctx.writer_id,
                    &self.ctx.parent,
                    record,
                    self.ctx.lineage,
                )?;
                event
                    .effect_provenance
                    .as_mut()
                    .ok_or_else(|| {
                        EffectError::EffectProvenanceMismatch(
                            "affine recorded reply is missing effect provenance".to_string(),
                        )
                    })?
                    .attempt = Some(attempt);
                return self
                    .commit_terminal_group(
                        &cursor,
                        vec![AtomicCommitEntry {
                            event,
                            options: CommitOptions::default(),
                            intent: StageAppendIntent::NonDataStageFact,
                        }],
                        control_events,
                        RowLineage::Live,
                    )
                    .await;
            }
        };
        let routed_fact_count = self.count_routed_facts(&facts);
        self.ensure_routed_fanout_capacity(routed_fact_count)?;
        let output_ordinal = self.reserve_output_ordinals(facts.len())?;
        let mut committed_events = build_domain_effect_success_facts(
            self.ctx.writer_id,
            &self.ctx.parent,
            cursor.clone(),
            descriptor_hash,
            descriptor,
            facts,
            output_ordinal,
            Some(EffectFactOrigin::Effect),
            self.ctx.lineage,
        )?;
        for event in &mut committed_events {
            let provenance = event.effect_provenance.as_mut().ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(
                    "affine success fact is missing effect provenance".to_string(),
                )
            })?;
            provenance.attempt = Some(attempt);
        }
        let entries = committed_events
            .iter()
            .cloned()
            .map(|event| AtomicCommitEntry {
                event,
                options: CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
                intent: StageAppendIntent::NormalStageData,
            })
            .collect();
        self.commit_terminal_group(&cursor, entries, control_events, RowLineage::Live)
            .await?;
        self.routed_output_fact_count = self
            .routed_output_fact_count
            .checked_add(routed_fact_count)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        self.committed_facts.extend(committed_events);
        Ok(())
    }

    async fn append_affine_failed_record(
        &self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        observed_failure: (EffectObservation, &EffectError),
        attempt: EffectAttemptOrdinal,
        mut control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        let (observation, error) = observed_failure;
        if matches!(error, EffectError::EffectTargetInvariantViolation { .. }) {
            let (preterminal, terminal) =
                split_invariant_control_events(&cursor, attempt, control_events)?;
            self.commit_escape_control_group(
                &cursor,
                attempt,
                preterminal,
                obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
                RowLineage::Live,
            )
            .await?;
            control_events = terminal;
        }
        let record = EffectRecord {
            observation,
            cursor: cursor.clone(),
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: error.error_type(),
                error_message: error.error_message(),
                retry: error.retry_disposition(),
                cause: error.failure_cause(),
                detail: error.failure_detail(),
            },
            origin: None,
        };
        let mut event = build_effect_record_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            self.ctx.lineage,
        )?;
        event
            .effect_provenance
            .as_mut()
            .ok_or_else(|| {
                EffectError::EffectProvenanceMismatch(
                    "affine failure record is missing effect provenance".to_string(),
                )
            })?
            .attempt = Some(attempt);
        self.commit_terminal_group(
            &cursor,
            vec![AtomicCommitEntry {
                event,
                options: CommitOptions::default(),
                intent: StageAppendIntent::NonDataStageFact,
            }],
            control_events,
            RowLineage::Live,
        )
        .await
    }

    async fn record_recovery_abandonment<T>(
        &self,
        abandonment: RecoveryAbandonment,
    ) -> Result<T, EffectError> {
        let RecoveryAbandonment {
            cursor,
            descriptor_hash,
            descriptor,
            highest_started_attempt,
            causal_input_id,
            reason,
            control_events,
        } = abandonment;
        let error = EffectError::RecoveryAbandoned {
            last_started_attempt: highest_started_attempt,
            failure_source: reason.cause.source.clone(),
            code: reason.cause.code.clone(),
            message: reason.message.clone(),
            boundary_retry: reason.retry,
        };
        let record = EffectRecord {
            observation: EffectObservation::ExecutionUnresolved {
                reason: EffectUnresolvedReason::RecoveryAbandoned {
                    admission_event_id: deterministic_effect_evidence_event_id(
                        &cursor,
                        &EffectAttemptStarted::descriptor(),
                        Some(highest_started_attempt),
                    ),
                    attempt: highest_started_attempt,
                },
                external_completion: ExternalCompletion::Unknown,
            },
            cursor: cursor.clone(),
            descriptor_hash: descriptor_hash.clone(),
            descriptor: descriptor.clone(),
            outcome: EffectOutcomePayload::Failed {
                error_type: error.error_type(),
                error_message: error.error_message(),
                retry: RetryDisposition::NotRetryable,
                cause: Some(reason.cause.clone()),
                detail: None,
            },
            origin: None,
        };
        let record_event = build_effect_record_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            self.ctx.lineage,
        )?;
        let abandoned = EffectRecoveryAbandoned {
            cursor: cursor.clone(),
            descriptor_hash,
            effect_type: descriptor.effect_type.clone(),
            outcome_group_id: effect_outcome_group_id(&cursor),
            highest_started_attempt,
            causal_input_id,
            cause: reason.cause,
            message: reason.message,
            retry: reason.retry,
        };
        let abandoned_event = build_effect_recovery_abandoned_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            abandoned,
            descriptor,
            self.ctx.lineage,
        )?;
        self.commit_terminal_group(
            &cursor,
            vec![
                AtomicCommitEntry {
                    event: record_event,
                    options: CommitOptions::default(),
                    intent: StageAppendIntent::NonDataStageFact,
                },
                AtomicCommitEntry {
                    event: abandoned_event,
                    options: CommitOptions::default(),
                    intent: StageAppendIntent::NonDataStageFact,
                },
            ],
            control_events,
            RowLineage::Live,
        )
        .await?;
        Err(error)
    }

    async fn append_failed_record(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        observation: EffectObservation,
        err: &EffectError,
    ) -> Result<(), EffectError> {
        self.append_record(
            EffectRecord {
                observation,
                cursor,
                descriptor_hash,
                descriptor,
                outcome: EffectOutcomePayload::Failed {
                    error_type: err.error_type(),
                    error_message: err.error_message(),
                    retry: err.retry_disposition(),
                    cause: err.failure_cause(),
                    detail: err.failure_detail(),
                },
                origin: None,
            },
            RowLineage::Live,
        )
        .await
    }

    async fn append_failed_record_with_control_events(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        observation: EffectObservation,
        err: &EffectError,
        control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        let record = EffectRecord {
            observation,
            cursor: cursor.clone(),
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: err.error_type(),
                error_message: err.error_message(),
                retry: err.retry_disposition(),
                cause: err.failure_cause(),
                detail: err.failure_detail(),
            },
            origin: None,
        };
        let event = build_effect_record_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            self.ctx.lineage,
        )?;
        self.commit_terminal_group(
            &cursor,
            vec![AtomicCommitEntry {
                event,
                options: CommitOptions::default(),
                intent: StageAppendIntent::NonDataStageFact,
            }],
            control_events,
            RowLineage::Live,
        )
        .await?;
        Ok(())
    }

    async fn record_boundary_abort_with_control_events<T>(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        reason: EffectAbortReason,
        control_events: Vec<ChainEvent>,
    ) -> Result<T, EffectError> {
        let err = EffectError::BoundaryRejected {
            rejected_by: reason.cause.source.clone(),
            code: reason.cause.code.clone(),
            message: reason.message.clone(),
            retry: reason.retry,
        };
        let record = EffectRecord {
            observation: EffectObservation::ExecutionRejected {},
            cursor: cursor.clone(),
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: err.error_type(),
                error_message: err.error_message(),
                retry: reason.retry,
                cause: Some(reason.cause),
                detail: None,
            },
            origin: None,
        };
        let event = build_effect_record_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            self.ctx.lineage,
        )?;
        self.commit_terminal_group(
            &cursor,
            vec![AtomicCommitEntry {
                event,
                options: CommitOptions::default(),
                intent: StageAppendIntent::NonDataStageFact,
            }],
            control_events,
            RowLineage::Live,
        )
        .await?;
        Err(err)
    }

    pub(crate) async fn capture<T>(
        &mut self,
        label: &'static str,
        value: T,
    ) -> Result<T, EffectError>
    where
        T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.gate_authoring()?;
        let effect_ordinal = self.reserve_effect_ordinal()?;
        let descriptor = EffectDescriptor::new(
            "obzenflow.capture",
            label,
            1,
            self.ctx.stage_logic_version.clone(),
            hash_json_value(&Value::String(label.to_string()))?,
        );
        let descriptor_hash = descriptor_hash(&descriptor)?;
        let cursor = EffectCursor::new(
            self.namespace(),
            self.ctx.stage_key.clone(),
            self.ctx.input_seq.0,
            effect_ordinal,
        );
        let input = self.ctx.input_seq.0;
        let plan = self.take_act(super::ActKey::Effect(cursor.clone()), || {
            format!(
                "would perform {} at input {input}",
                super::acts::display_name("obzenflow.capture")
            )
        })?;

        if let Some(history) = self.ctx.effect_history.clone() {
            let held = history.find_group(&cursor);
            let selection = if held.is_some() {
                EffectHistorySelection::Hit(Vec::new())
            } else {
                EffectHistorySelection::Miss
            };
            if let Some(refusal) = act_plan_mismatch(plan, &selection, &cursor) {
                return Err(self.refuse(refusal));
            }
            if let Some(records) = held {
                let frontier = history.cursor_history(&cursor).causal;
                let incorporated = self.incorporate_history(&frontier);
                self.history_shape(incorporated)?;
                let [record] = records.as_slice() else {
                    return Err(self.refuse(ReconstructionRefusal::HistoryShape(
                        EffectError::EffectProvenanceMismatch(format!(
                            "capture cursor {cursor:?} recorded {} outcome records",
                            records.len()
                        )),
                    )));
                };
                if record.descriptor_hash != descriptor_hash {
                    return Err(self.refuse_descriptor(
                        &cursor,
                        &descriptor_hash,
                        &record.descriptor_hash,
                    ));
                }
                let output_result = self.recorded_outcome(decode_effect_outcome(&record.outcome));
                let output = match output_result {
                    Ok(output) => output,
                    Err(err @ EffectError::RecordedFailure { .. }) => {
                        let materialization =
                            self.history_shape(effect_record_group_materialization(&records))?;
                        self.append_replayed_records(
                            cursor,
                            descriptor_hash,
                            descriptor,
                            materialization,
                        )
                        .await?;
                        return Err(err);
                    }
                    Err(err) => return Err(err),
                };
                let materialization =
                    self.history_shape(effect_record_group_materialization(&records))?;
                self.append_replayed_records(cursor, descriptor_hash, descriptor, materialization)
                    .await?;
                return Ok(output);
            }

            if plan != ActPlan::Live {
                if let Some(error) = self.unrecorded_effect_error(cursor.clone()) {
                    return Err(error);
                }
            }
        }
        if plan == ActPlan::Live {
            self.cross_before_live_act().await?;
        }

        let output =
            serde_json::to_value(&value).map_err(|e| EffectError::Serialization(e.to_string()))?;
        self.append_record(
            EffectRecord {
                observation: EffectObservation::ValueCaptured {},
                cursor,
                descriptor_hash,
                descriptor,
                outcome: EffectOutcomePayload::Succeeded { output },
                origin: None,
            },
            RowLineage::Live,
        )
        .await?;
        self.observe_effect_outcome(
            "obzenflow.capture",
            crate::stages::observer::EffectObserverOutcome::Succeeded,
        );
        Ok(value)
    }

    async fn perform_transactional<E>(
        &mut self,
        effect: E,
        prepared: PreparedLiveEffect,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let PreparedLiveEffect {
            identity,
            cursor,
            descriptor_hash,
            descriptor,
            binding_context: mut effect_ctx,
        } = prepared;
        let executor_slot = transactional_effect_port_slot::<E>();
        let executor = executor_slot.label();
        let port = effect_ctx.port(executor_slot)?;
        let output_ordinal = match E::OutcomeSemantics::KIND {
            EffectOutcomeKind::DomainFacts => Some(self.reserve_output_ordinal()?),
            EffectOutcomeKind::RecordedReply => None,
        };
        let commit: EffectCommitHandle<E::Outcome, E::OutcomeSemantics> =
            EffectCommitHandle::new(EffectCommitHandleParams {
                writer_id: self.ctx.writer_id,
                data_journal: self.ctx.data_journal.clone(),
                flow_context: self.ctx.flow_context.clone(),
                instrumentation: self.ctx.instrumentation.clone(),
                heartbeat_state: self.ctx.heartbeat_state.clone(),
                output_contract: self.ctx.output_contract.clone(),
                backpressure_writer: self.ctx.backpressure_writer.clone(),
                parent: self.ctx.parent.clone(),
                cursor: cursor.clone(),
                descriptor_hash: descriptor_hash.clone(),
                descriptor: descriptor.clone(),
                output_ordinal,
                lineage: self.ctx.lineage,
                defer_persistence: self.ctx.effect_boundary.is_some(),
            });
        let commit_observer = commit.clone();

        let Some(boundary) = self.ctx.effect_boundary.clone() else {
            // The port commits its outcome through the handle. Its returned value is
            // intentionally not used as the live result: the committed record is the single
            // source of truth, so a live run decodes the same journaled outcome a replay
            // would, and a port that commits one outcome but returns another cannot make
            // live output diverge from replay output (FLOWIP-120a).
            let port_result = port
                .execute_and_commit(effect, &mut effect_ctx, commit)
                .await
                .map(|_| ());
            let outcome = commit_observer.settled_outcome();
            let result =
                self.settle_transactional::<E>(executor, output_ordinal, port_result, outcome);
            self.observe_effect_result(E::EFFECT_TYPE, &result);
            return result;
        };

        // FLOWIP-120c H5: transactional effects route through the boundary.
        // Admission runs before `execute_and_commit`, observation after
        // `settled_outcome()`; rejection-only in v1, no fallback synthesis.
        let settle_slot: TransactionalSettleSlot<E::Outcome> = Arc::new(Mutex::new(None));
        let operation = {
            let slot = settle_slot.clone();
            let observer = commit_observer.clone();
            let executor_name = executor.to_string();
            SingleUseEffectOperation::new_with_lifecycle(move |lifecycle| {
                async move {
                    *slot.lock().unwrap_or_else(|p| p.into_inner()) = None;
                    lifecycle.mark_started();
                    let port_result = port
                        .execute_and_commit(effect, &mut effect_ctx, commit)
                        .await
                        .map(|_| ());
                    let outcome = observer.settled_outcome();
                    // A committed failure is the dependency result even when the
                    // transactional port returned `Ok`; conversely, a committed
                    // success remains authoritative if the port later returned an
                    // error. No-commit and append failures retain their typed
                    // coordination/journal errors for health classification.
                    lifecycle.mark_completed(match &outcome {
                        Some(PreparedEffectOutcome::Success { .. }) => {
                            PhysicalCallOutcome::Succeeded
                        }
                        Some(PreparedEffectOutcome::Failure { .. }) => PhysicalCallOutcome::Failed,
                        None if port_result.is_err() => PhysicalCallOutcome::Failed,
                        None => PhysicalCallOutcome::Succeeded,
                    });
                    // The boundary observes a faithful classification of how the
                    // operation ended; the precise error the caller sees is settled
                    // from the slot, never from this observation result.
                    let observation = match (&port_result, &outcome) {
                        (
                            _,
                            Some(PreparedEffectOutcome::Success {
                                observation_events, ..
                            }),
                        ) => Ok(observation_events.clone()),
                        (_, Some(PreparedEffectOutcome::Failure { outcome, .. })) => {
                            Err(match recorded_failure_from_outcome::<E::Outcome>(outcome) {
                                Err(err) => err,
                                Ok(_) => EffectError::EffectProvenanceMismatch(
                                    "expected recorded effect failure".to_string(),
                                ),
                            })
                        }
                        (Err(err), None) => Err(err.clone()),
                        (Ok(()), None) => Err(EffectError::TransactionalCommitMissing {
                            effect_type: E::EFFECT_TYPE.to_string(),
                            executor: executor_name,
                        }),
                    };
                    *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some((port_result, outcome));
                    observation
                }
            })
        };
        let expected_provenance = operation.provenance();

        let report = boundary
            .around_single_use_effect(&identity, &self.ctx.parent.authored(), operation)
            .await;
        let (outcome, control_events) = match report.into_parts(&expected_provenance) {
            Ok(parts) => parts,
            Err(err) => {
                // A report for another capability has no authority over this
                // invocation, including no authority to inject control
                // events. If this operation nevertheless reached its settle
                // slot, the committed outcome remains terminal and must win;
                // an already-committed transaction cannot be reclassified by
                // a boundary contract violation.
                let settled = settle_slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take();
                if let Some((port_result, outcome)) = settled {
                    if let Some(prepared) = outcome.as_ref() {
                        self.commit_deferred_transactional_outcome(&cursor, prepared, Vec::new())
                            .await?;
                    }
                    let result = self.settle_transactional::<E>(
                        executor,
                        output_ordinal,
                        port_result,
                        outcome,
                    );
                    self.observe_effect_result(E::EFFECT_TYPE, &result);
                    return result;
                }

                // No transaction ran. Record the fail-closed provenance error
                // under this cursor so strict replay reproduces the rejection.
                if let Some(output_ordinal) = output_ordinal {
                    self.restore_output_ordinal(output_ordinal);
                }
                self.append_failed_record(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    EffectObservation::ExecutionRejected {},
                    &err,
                )
                .await?;
                let result: Result<E::Outcome, EffectError> = Err(err);
                self.observe_effect_result(E::EFFECT_TYPE, &result);
                return result;
            }
        };
        match outcome {
            SingleUseEffectBoundaryOutcome::Executed(_execution) => {
                let (port_result, outcome) = settle_slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                    .ok_or_else(|| {
                        EffectError::Execution(
                            "transactional boundary reported execution without a settled outcome"
                                .to_string(),
                        )
                    })?;
                let Some(prepared) = outcome.as_ref() else {
                    if let Some(output_ordinal) = output_ordinal {
                        self.restore_output_ordinal(output_ordinal);
                    }
                    let observed_return = if port_result.is_ok() {
                        EffectPortReturn::Succeeded
                    } else {
                        EffectPortReturn::Failed
                    };
                    let err = match port_result {
                        Err(err) => err,
                        Ok(()) => EffectError::TransactionalCommitMissing {
                            effect_type: E::EFFECT_TYPE.to_string(),
                            executor: executor.to_string(),
                        },
                    };
                    self.append_failed_record_with_control_events(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        EffectObservation::ExecutionUnresolved {
                            reason: EffectUnresolvedReason::MissingTransactionalSettlement {
                                port_return: observed_return,
                            },
                            external_completion: ExternalCompletion::Unknown,
                        },
                        &err,
                        control_events,
                    )
                    .await?;
                    let result: Result<E::Outcome, EffectError> = Err(err);
                    self.observe_effect_result(E::EFFECT_TYPE, &result);
                    return result;
                };
                self.commit_deferred_transactional_outcome(&cursor, prepared, control_events)
                    .await?;
                let result =
                    self.settle_transactional::<E>(executor, output_ordinal, port_result, outcome);
                self.observe_effect_result(E::EFFECT_TYPE, &result);
                result
            }
            SingleUseEffectBoundaryOutcome::Aborted(reason) => {
                if let Some(output_ordinal) = output_ordinal {
                    self.restore_output_ordinal(output_ordinal);
                }
                let result = self
                    .record_boundary_abort_with_control_events(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        reason,
                        control_events,
                    )
                    .await;
                self.observe_effect_result(E::EFFECT_TYPE, &result);
                result
            }
        }
    }

    async fn commit_deferred_transactional_outcome<T>(
        &self,
        cursor: &EffectCursor,
        outcome: &PreparedEffectOutcome<T>,
        control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let entries = match outcome {
            PreparedEffectOutcome::Success {
                kind,
                events,
                persisted,
                ..
            } => {
                if *persisted {
                    if control_events.is_empty() {
                        return Ok(());
                    }
                    return Err(EffectError::Execution(
                        "transactional outcome was persisted before terminal control evidence"
                            .to_string(),
                    ));
                }
                events
                    .iter()
                    .cloned()
                    .map(|event| AtomicCommitEntry {
                        event,
                        options: match kind {
                            EffectOutcomeKind::DomainFacts => CommitOptions {
                                count_output: true,
                                validate_output_contract: true,
                            },
                            EffectOutcomeKind::RecordedReply => CommitOptions::default(),
                        },
                        intent: match kind {
                            EffectOutcomeKind::DomainFacts => StageAppendIntent::NormalStageData,
                            EffectOutcomeKind::RecordedReply => StageAppendIntent::NonDataStageFact,
                        },
                    })
                    .collect()
            }
            PreparedEffectOutcome::Failure {
                event, persisted, ..
            } => {
                if *persisted {
                    if control_events.is_empty() {
                        return Ok(());
                    }
                    return Err(EffectError::Execution(
                        "transactional failure was persisted before terminal control evidence"
                            .to_string(),
                    ));
                }
                vec![AtomicCommitEntry {
                    event: event.as_ref().clone(),
                    options: CommitOptions::default(),
                    intent: StageAppendIntent::NonDataStageFact,
                }]
            }
        };
        self.commit_terminal_group(cursor, entries, control_events, RowLineage::Live)
            .await
    }

    /// Settle a transactional attempt from the committed record, the single
    /// source of truth (FLOWIP-120a).
    fn settle_transactional<E>(
        &mut self,
        executor: &'static str,
        output_ordinal: Option<EffectOutputOrdinal>,
        port_result: Result<(), EffectError>,
        outcome: Option<PreparedEffectOutcome<E::Outcome>>,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let Some(outcome) = outcome else {
            // No commit through the handle. Surface the port's own error if it produced
            // one, otherwise the contract-violation error.
            if let Some(output_ordinal) = output_ordinal {
                self.restore_output_ordinal(output_ordinal);
            }
            return Err(match port_result {
                Err(err) => err,
                Ok(()) => EffectError::TransactionalCommitMissing {
                    effect_type: E::EFFECT_TYPE.to_string(),
                    executor: executor.to_string(),
                },
            });
        };

        match outcome {
            PreparedEffectOutcome::Success {
                output,
                kind,
                public_fact_count,
                events,
                ..
            } => {
                match (kind, output_ordinal) {
                    (EffectOutcomeKind::DomainFacts, Some(output_ordinal)) => {
                        self.advance_output_ordinals_after_reserved_base(
                            output_ordinal,
                            public_fact_count,
                        )?;
                        self.committed_facts.extend(events);
                    }
                    (EffectOutcomeKind::RecordedReply, None) => {}
                    (EffectOutcomeKind::DomainFacts, None) => {
                        return Err(EffectError::Execution(
                            "domain-fact transactional success has no output ordinal".to_string(),
                        ));
                    }
                    (EffectOutcomeKind::RecordedReply, Some(_)) => {
                        return Err(EffectError::Execution(
                            "recorded-reply transactional success reserved an output ordinal"
                                .to_string(),
                        ));
                    }
                }
                Ok(output)
            }
            PreparedEffectOutcome::Failure { outcome, .. } => {
                if let Some(output_ordinal) = output_ordinal {
                    self.restore_output_ordinal(output_ordinal);
                }
                recorded_failure_from_outcome(&outcome)
            }
        }
    }

    /// Roll back a single-slot output reservation nothing consumed.
    ///
    /// Failure records key off the effect cursor, not output ordinals, so a
    /// transactional attempt that committed no facts must not leave a live
    /// ordinal gap: replay's record-driven paths never reserve this slot, and
    /// a handler `emit` after this effect would otherwise compute a different
    /// deterministic identity live than under reconstruction.
    fn restore_output_ordinal(&mut self, reserved_base: EffectOutputOrdinal) {
        if let Some(reserved_next) = reserved_base.checked_add(1) {
            if self.next_output_ordinal == reserved_next {
                self.next_output_ordinal = reserved_base;
            }
        }
    }

    fn binding_fault_for_error<E: Effect>(
        &self,
        declaration: &EffectDeclaration,
        error: &EffectError,
    ) -> Option<BindingAuthorityFault> {
        if let EffectError::BindingAuthority { fault } = error {
            return Some(fault.clone());
        }

        let (binding, _, _) = declaration.binding().named_parts()?;
        match error {
            EffectError::EffectTargetInvariantViolation { slot } => {
                let slot = declaration.binding().declared_slot_label(slot.as_str())?;
                Some(BindingAuthorityFault::target_invariant_violation(
                    E::EFFECT_TYPE,
                    binding.clone(),
                    slot,
                    false,
                ))
            }
            EffectError::RecordedFailure {
                detail: Some(detail),
                ..
            } => match detail.as_ref() {
                EffectFailureDetail::PortBindingInvariantViolation { port, .. } => {
                    let slot = declaration.binding().declared_slot_label(port)?;
                    Some(BindingAuthorityFault::target_invariant_violation(
                        E::EFFECT_TYPE,
                        binding.clone(),
                        slot,
                        true,
                    ))
                }
            },
            _ => None,
        }
    }

    fn live_effect_contexts(
        &self,
        declaration: &EffectDeclaration,
    ) -> Result<(EffectPortMetadataContext, EffectContext), EffectError> {
        let views = match declaration.binding().named_parts() {
            None => super::ports::EffectPortViews::default(),
            Some((binding, registration, slots)) => self
                .ctx
                .effect_ports
                .scoped_view(registration, slots)
                .map_err(|error| {
                    let fault = match error {
                        super::ports::EffectPortViewBuildError::MissingRegistration => {
                            BindingAuthorityFault::registration_missing(
                                declaration.effect_type(),
                                binding.clone(),
                            )
                        }
                        super::ports::EffectPortViewBuildError::Resolver { slot, .. } => {
                            BindingAuthorityFault::resolution_failed(
                                declaration.effect_type(),
                                binding.clone(),
                                slot,
                            )
                        }
                    };
                    EffectError::BindingAuthority { fault }
                })?,
        };
        let metadata_context = EffectPortMetadataContext {
            metadata: views.metadata,
        };
        let binding_context = EffectContext {
            is_replaying: false,
            flow_id: self.ctx.flow_id,
            stage_key: self.ctx.stage_key.clone(),
            input_seq: self.ctx.input_seq,
            ports: views.ports,
        };
        Ok((metadata_context, binding_context))
    }

    async fn append_record(
        &self,
        record: EffectRecord,
        lineage: RowLineage<'_>,
    ) -> Result<(), EffectError> {
        let mut event = build_effect_record_event(
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            self.ctx.lineage,
        )?;
        self.mark_lineage(&mut event, lineage);
        crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            append_effect_record(
                &self.ctx.data_journal,
                event,
                &self.ctx.parent,
                &self.ctx.backpressure_writer,
            ),
        )
        .await
    }

    async fn append_success(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        success: PreparedEffectSuccess,
        origin: Option<EffectFactOrigin>,
    ) -> Result<(), EffectError> {
        match success {
            PreparedEffectSuccess::DomainFacts(facts) => {
                self.append_success_facts(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    facts,
                    origin,
                    RowLineage::Live,
                )
                .await
            }
            PreparedEffectSuccess::RecordedReply(output) => {
                self.append_record(
                    EffectRecord {
                        observation: EffectObservation::returned_success(),
                        cursor,
                        descriptor_hash,
                        descriptor,
                        outcome: EffectOutcomePayload::Succeeded { output },
                        origin: None,
                    },
                    RowLineage::Live,
                )
                .await
            }
        }
    }

    async fn append_success_with_control_events(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        success: PreparedEffectSuccess,
        origin: Option<EffectFactOrigin>,
        control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        match success {
            PreparedEffectSuccess::DomainFacts(facts) => {
                self.append_success_facts_with_control_events(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    facts,
                    origin,
                    control_events,
                )
                .await
            }
            PreparedEffectSuccess::RecordedReply(output) => {
                let record = EffectRecord {
                    observation: EffectObservation::returned_success(),
                    cursor: cursor.clone(),
                    descriptor_hash,
                    descriptor,
                    outcome: EffectOutcomePayload::Succeeded { output },
                    origin: None,
                };
                let event = build_effect_record_event(
                    self.ctx.writer_id,
                    &self.ctx.parent,
                    record,
                    self.ctx.lineage,
                )?;
                self.commit_terminal_group(
                    &cursor,
                    vec![AtomicCommitEntry {
                        event,
                        options: CommitOptions::default(),
                        intent: StageAppendIntent::NonDataStageFact,
                    }],
                    control_events,
                    RowLineage::Live,
                )
                .await
            }
        }
    }

    async fn append_success_facts(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        facts: Vec<TypedFact>,
        origin: Option<EffectFactOrigin>,
        lineage: RowLineage<'_>,
    ) -> Result<(), EffectError> {
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "effect success output must author at least one fact".to_string(),
            ));
        }
        let routed_fact_count = self.count_routed_facts(&facts);
        self.ensure_routed_fanout_capacity(routed_fact_count)?;
        let output_ordinal = self.reserve_output_ordinals(facts.len())?;
        let mut events = build_domain_effect_success_facts(
            self.ctx.writer_id,
            &self.ctx.parent,
            cursor,
            descriptor_hash,
            descriptor,
            facts,
            output_ordinal,
            origin,
            self.ctx.lineage,
        )?;
        for event in &mut events {
            self.mark_lineage(event, lineage);
        }
        let committed_events = crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            append_domain_effect_success_facts(
                &self.ctx.data_journal,
                self.ctx.flow_context.as_ref(),
                self.ctx.instrumentation.as_ref(),
                self.ctx.heartbeat_state.as_ref(),
                Some(&self.ctx.output_contract),
                &self.ctx.backpressure_writer,
                &self.ctx.parent,
                events,
            ),
        )
        .await?;
        self.routed_output_fact_count = self
            .routed_output_fact_count
            .checked_add(routed_fact_count)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        self.committed_facts.extend(committed_events);
        Ok(())
    }

    async fn append_success_facts_with_control_events(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        facts: Vec<TypedFact>,
        origin: Option<EffectFactOrigin>,
        control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "effect success output must author at least one fact".to_string(),
            ));
        }
        let routed_fact_count = self.count_routed_facts(&facts);
        self.ensure_routed_fanout_capacity(routed_fact_count)?;
        let output_ordinal = self.reserve_output_ordinals(facts.len())?;
        let committed_events = build_domain_effect_success_facts(
            self.ctx.writer_id,
            &self.ctx.parent,
            cursor.clone(),
            descriptor_hash,
            descriptor,
            facts,
            output_ordinal,
            origin,
            self.ctx.lineage,
        )?;
        let outcome_entries = committed_events
            .iter()
            .cloned()
            .map(|event| AtomicCommitEntry {
                event,
                options: CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
                intent: StageAppendIntent::NormalStageData,
            })
            .collect();
        self.commit_terminal_group(&cursor, outcome_entries, control_events, RowLineage::Live)
            .await?;
        self.routed_output_fact_count = self
            .routed_output_fact_count
            .checked_add(routed_fact_count)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        self.committed_facts.extend(committed_events);
        Ok(())
    }

    async fn commit_terminal_group(
        &self,
        cursor: &EffectCursor,
        mut outcome_entries: Vec<AtomicCommitEntry>,
        control_events: Vec<ChainEvent>,
        lineage: RowLineage<'_>,
    ) -> Result<(), EffectError> {
        outcome_entries.extend(control_events.into_iter().map(|event| AtomicCommitEntry {
            event,
            options: CommitOptions::default(),
            intent: StageAppendIntent::FrameworkObservability,
        }));
        for entry in &mut outcome_entries {
            self.mark_lineage(&mut entry.event, lineage);
        }
        let group_id = effect_outcome_group_id(cursor);
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),

            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.output_contract),
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        };
        if let Err(error) = crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            committer.commit_atomic_group(
                group_id.as_str(),
                outcome_entries,
                Some(&self.ctx.parent),
            ),
        )
        .await
        {
            self.ctx
                .runtime_execution
                .effect_cursor_coordinator()
                .poison(cursor.clone());
            return Err(EffectError::Journal(error.to_string()));
        }
        Ok(())
    }

    async fn commit_escape_control_group(
        &self,
        cursor: &EffectCursor,
        attempt: EffectAttemptOrdinal,
        control_events: Vec<ChainEvent>,
        observer_scope: obzenflow_core::MiddlewareExecutionScope,
        lineage: RowLineage<'_>,
    ) -> Result<(), EffectError> {
        if control_events.is_empty() {
            return Ok(());
        }
        if control_events.iter().any(ChainEvent::consumes_data_credit) {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "escape-control batch for cursor {cursor:?} attempt {attempt} contains Data"
            )));
        }
        let entries = control_events
            .into_iter()
            .map(|mut event| {
                self.mark_lineage(&mut event, lineage);
                AtomicCommitEntry {
                    event,
                    options: CommitOptions::default(),
                    intent: StageAppendIntent::FrameworkObservability,
                }
            })
            .collect();
        let group_id = effect_escape_controls_group_id(cursor, attempt);
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),

            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: None,
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observer_scope,
        };
        if let Err(error) = crate::supervised_base::publication::with_snapshot(
            self.causal.clone(),
            committer.commit_atomic_group(group_id.as_str(), entries, Some(&self.ctx.parent)),
        )
        .await
        {
            self.ctx
                .runtime_execution
                .effect_cursor_coordinator()
                .poison(cursor.clone());
            return Err(EffectError::Journal(error.to_string()));
        }
        Ok(())
    }

    async fn append_replayed_prefix(
        &mut self,
        cursor: &EffectCursor,
        history: &EffectCursorHistory,
        descriptor: EffectDescriptor,
    ) -> Result<(), EffectError> {
        for started in &history.attempts {
            let mut event = build_effect_attempt_started_event(
                self.ctx.writer_id,
                &self.ctx.parent,
                started.clone(),
                descriptor.clone(),
                self.ctx.lineage,
            )?;
            let archived = self.history_shape(
                history.attempt_events.get(&started.attempt).ok_or_else(|| {
                    EffectError::EffectProvenanceMismatch(format!(
                        "effect cursor {cursor:?} Start({}) lacks its archived event identity",
                        started.attempt
                    ))
                }),
            )?;
            self.history_shape(restore_archived_effect_identity(&mut event, archived))?;
            self.reauthor_recorded_crossings(super::ActRow::Effect {
                cursor: cursor.clone(),
                attempt: Some(started.attempt),
            })
            .await?;
            self.mark_lineage(&mut event, RowLineage::Reproduced(Some(history)));
            self.commit_framework_effect_event(event).await?;
            if let Some(control_events) = history.escape_control_batches.get(&started.attempt) {
                self.commit_escape_control_group(
                    cursor,
                    started.attempt,
                    control_events.to_vec(),
                    obzenflow_core::MiddlewareExecutionScope::StrictReplayHandler,
                    RowLineage::Reproduced(Some(history)),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn append_replayed_history(
        &mut self,
        history: &EffectCursorHistory,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        materialization: EffectRecordMaterialization,
    ) -> Result<(), EffectError> {
        self.append_replayed_prefix(&cursor, history, descriptor.clone())
            .await?;

        let mut terminal_control_events = Vec::new();
        for event in &history.terminal_group_events {
            if EffectRecoveryAbandoned::matches_event_type(&event.event_type()) {
                continue;
            }
            if self
                .history_shape(effect_record_from_event(event))?
                .is_some()
            {
                continue;
            }
            if event.consumes_data_credit() {
                return Err(self.refuse(ReconstructionRefusal::HistoryShape(
                    EffectError::EffectProvenanceMismatch(format!(
                        "terminal control evidence for cursor {cursor:?} contains unrecognised Data"
                    )),
                )));
            }
            terminal_control_events.push(event.clone());
        }

        let terminal_attempt = history.terminal_attempt.flatten();
        let mut entries = Vec::new();
        let mut committed_domain_events = Vec::new();
        let mut routed_fact_count = 0_usize;
        match materialization {
            EffectRecordMaterialization::DomainFacts {
                facts,
                origin: recorded_origin,
            } => {
                routed_fact_count = self.count_routed_facts(&facts);
                self.ensure_routed_fanout_capacity(routed_fact_count)?;
                let output_ordinal = self.reserve_output_ordinals(facts.len())?;
                let mut events = build_domain_effect_success_facts(
                    self.ctx.writer_id,
                    &self.ctx.parent,
                    cursor.clone(),
                    descriptor_hash,
                    descriptor.clone(),
                    facts,
                    output_ordinal,
                    recorded_origin.or(Some(EffectFactOrigin::Effect)),
                    self.ctx.lineage,
                )?;
                for event in &mut events {
                    event
                        .effect_provenance
                        .as_mut()
                        .ok_or_else(|| {
                            EffectError::EffectProvenanceMismatch(
                                "replayed effect fact is missing effect provenance".to_string(),
                            )
                        })?
                        .attempt = terminal_attempt;
                    self.history_shape(restore_archived_terminal_identity(event, history))?;
                }
                entries.extend(events.iter().cloned().map(|event| AtomicCommitEntry {
                    event,
                    options: CommitOptions {
                        count_output: true,
                        validate_output_contract: true,
                    },
                    intent: StageAppendIntent::NormalStageData,
                }));
                committed_domain_events = events;
            }
            EffectRecordMaterialization::FrameworkRecords(records) => {
                for record in records {
                    let mut event = build_effect_record_event(
                        self.ctx.writer_id,
                        &self.ctx.parent,
                        record,
                        self.ctx.lineage,
                    )?;
                    event
                        .effect_provenance
                        .as_mut()
                        .ok_or_else(|| {
                            EffectError::EffectProvenanceMismatch(
                                "replayed framework record is missing effect provenance"
                                    .to_string(),
                            )
                        })?
                        .attempt = terminal_attempt;
                    self.history_shape(restore_archived_terminal_identity(&mut event, history))?;
                    entries.push(AtomicCommitEntry {
                        event,
                        options: CommitOptions::default(),
                        intent: StageAppendIntent::NonDataStageFact,
                    });
                }
            }
        }

        if let Some(abandoned) = history.abandonment.clone() {
            let mut event = build_effect_recovery_abandoned_event(
                self.ctx.writer_id,
                &self.ctx.parent,
                abandoned,
                descriptor,
                self.ctx.lineage,
            )?;
            self.history_shape(restore_archived_terminal_identity(&mut event, history))?;
            entries.push(AtomicCommitEntry {
                event,
                options: CommitOptions::default(),
                intent: StageAppendIntent::NonDataStageFact,
            });
        }

        self.reauthor_recorded_crossings(super::ActRow::Effect {
            cursor: cursor.clone(),
            attempt: terminal_attempt,
        })
        .await?;
        self.commit_terminal_group(
            &cursor,
            entries,
            terminal_control_events,
            RowLineage::Reproduced(Some(history)),
        )
        .await?;
        self.routed_output_fact_count = self
            .routed_output_fact_count
            .checked_add(routed_fact_count)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        self.committed_facts.extend(committed_domain_events);
        Ok(())
    }

    async fn append_replayed_records(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        materialization: EffectRecordMaterialization,
    ) -> Result<(), EffectError> {
        self.reauthor_recorded_crossings(super::ActRow::Effect {
            cursor: cursor.clone(),
            attempt: None,
        })
        .await?;
        match materialization {
            EffectRecordMaterialization::DomainFacts {
                facts,
                origin: recorded_origin,
            } => {
                // The recorded provenance wins. Older records without an
                // explicit origin came from the effect itself.
                let origin = recorded_origin.or(Some(EffectFactOrigin::Effect));
                self.append_success_facts(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    facts,
                    origin,
                    RowLineage::Reproduced(None),
                )
                .await
            }
            EffectRecordMaterialization::FrameworkRecords(records) => {
                for record in records {
                    self.append_record(record, RowLineage::Reproduced(None))
                        .await?;
                }
                Ok(())
            }
        }
    }
}
