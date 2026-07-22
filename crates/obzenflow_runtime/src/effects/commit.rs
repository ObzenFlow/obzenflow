// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

pub struct EffectCommitHandle<T> {
    inner: Arc<EffectCommitHandleInner<T>>,
}

impl<T> Clone for EffectCommitHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct EffectCommitHandleInner<T> {
    writer_id: WriterId,
    data_journal: Arc<dyn Journal<ChainEvent>>,
    flow_context: Option<FlowContext>,
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    instrumentation: Option<Arc<StageInstrumentation>>,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    output_contract: StageOutputContract,
    backpressure_writer: BackpressureWriter,
    parent: EventEnvelope<ChainEvent>,
    cursor: EffectCursor,
    descriptor_hash: EffectDescriptorHash,
    descriptor: EffectDescriptor,
    output_ordinal: EffectOutputOrdinal,
    lineage: obzenflow_core::config::LineagePolicy,
    defer_persistence: bool,
    state: Mutex<EffectCommitState<T>>,
    _marker: PhantomData<T>,
}

pub(super) struct EffectCommitHandleParams {
    pub(super) writer_id: WriterId,
    pub(super) data_journal: Arc<dyn Journal<ChainEvent>>,
    pub(super) flow_context: Option<FlowContext>,
    pub(super) system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    pub(super) instrumentation: Option<Arc<StageInstrumentation>>,
    pub(super) heartbeat_state: Option<Arc<HeartbeatState>>,
    pub(super) output_contract: StageOutputContract,
    pub(super) backpressure_writer: BackpressureWriter,
    pub(super) parent: EventEnvelope<ChainEvent>,
    pub(super) cursor: EffectCursor,
    pub(super) descriptor_hash: EffectDescriptorHash,
    pub(super) descriptor: EffectDescriptor,
    pub(super) output_ordinal: EffectOutputOrdinal,
    pub(super) lineage: obzenflow_core::config::LineagePolicy,
    pub(super) defer_persistence: bool,
}

#[derive(Clone)]
pub(super) enum PreparedEffectOutcome<T> {
    Success {
        output: T,
        fact_count: usize,
        events: Vec<ChainEvent>,
        persisted: bool,
    },
    Failure {
        outcome: EffectOutcomePayload,
        event: Box<ChainEvent>,
        persisted: bool,
    },
}

enum EffectCommitState<T> {
    Available,
    InProgress,
    Settled(Box<PreparedEffectOutcome<T>>),
}

impl<T> EffectCommitHandle<T>
where
    T: TypedFactSet + Clone + Send + Sync + 'static,
{
    pub(super) fn new(params: EffectCommitHandleParams) -> Self {
        Self {
            inner: Arc::new(EffectCommitHandleInner {
                writer_id: params.writer_id,
                data_journal: params.data_journal,
                flow_context: params.flow_context,
                system_journal: params.system_journal,
                instrumentation: params.instrumentation,
                heartbeat_state: params.heartbeat_state,
                output_contract: params.output_contract,
                backpressure_writer: params.backpressure_writer,
                parent: params.parent,
                cursor: params.cursor,
                descriptor_hash: params.descriptor_hash,
                descriptor: params.descriptor,
                output_ordinal: params.output_ordinal,
                lineage: params.lineage,
                defer_persistence: params.defer_persistence,
                state: Mutex::new(EffectCommitState::Available),
                _marker: PhantomData,
            }),
        }
    }

    pub async fn commit_success(&self, output: &T) -> Result<(), EffectError> {
        self.ensure_available()?;

        let facts = output.clone().into_facts().map_err(effect_fact_set_error)?;
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "effect success output must author at least one fact".to_string(),
            ));
        }
        let fact_count = facts.len();
        let events = build_domain_effect_success_facts(
            self.inner.writer_id,
            &self.inner.parent,
            self.inner.cursor.clone(),
            self.inner.descriptor_hash.clone(),
            self.inner.descriptor.clone(),
            facts,
            self.inner.output_ordinal,
            Some(EffectFactOrigin::Effect),
            self.inner.lineage,
        )?;
        self.begin_commit()?;
        let persisted = !self.inner.defer_persistence;
        if persisted {
            let committer = OutputCommitter {
                data_journal: &self.inner.data_journal,
                flow_context: self.inner.flow_context.as_ref(),
                system_journal: self.inner.system_journal.as_ref(),
                instrumentation: self.inner.instrumentation.as_ref(),
                heartbeat_state: self.inner.heartbeat_state.as_ref(),
                output_contract: Some(&self.inner.output_contract),
                backpressure_writer: Some(&self.inner.backpressure_writer),
                observers: None,
                observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
            };
            let entries = events
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
            if let Err(error) = committer
                .commit_atomic_group(
                    effect_outcome_group_id(&self.inner.cursor).as_str(),
                    entries,
                    Some(&self.inner.parent),
                )
                .await
            {
                self.reset_failed_commit();
                return Err(EffectError::Journal(error.to_string()));
            }
        }

        self.finish_commit(PreparedEffectOutcome::Success {
            output: output.clone(),
            fact_count,
            events,
            persisted,
        })?;
        Ok(())
    }

    pub async fn commit_failure(&self, error: &EffectError) -> Result<(), EffectError> {
        self.commit_outcome(
            EffectOutcomePayload::Failed {
                error_type: error.error_type(),
                error_message: error.error_message(),
                retry: error.retry_disposition(),
                cause: error.failure_cause(),
            },
            Some(error),
        )
        .await
    }

    async fn commit_outcome(
        &self,
        outcome: EffectOutcomePayload,
        source_error: Option<&EffectError>,
    ) -> Result<(), EffectError> {
        self.ensure_available()?;
        if source_error.is_none() {
            return Err(EffectError::Execution(
                "domain success outcomes must be committed through commit_success".to_string(),
            ));
        }
        let record = EffectRecord {
            cursor: self.inner.cursor.clone(),
            descriptor_hash: self.inner.descriptor_hash.clone(),
            descriptor: self.inner.descriptor.clone(),
            outcome: outcome.clone(),
            origin: None,
        };

        let event = build_effect_record_event(
            self.inner.writer_id,
            &self.inner.parent,
            record,
            self.inner.lineage,
        )?;
        self.begin_commit()?;
        let persisted = !self.inner.defer_persistence;
        if persisted {
            let committer = OutputCommitter {
                data_journal: &self.inner.data_journal,
                flow_context: None,
                system_journal: None,
                instrumentation: None,
                heartbeat_state: None,
                output_contract: None,
                backpressure_writer: Some(&self.inner.backpressure_writer),
                observers: None,
                observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
            };
            if let Err(error) = committer
                .commit_prebuilt(
                    event.clone(),
                    Some(&self.inner.parent),
                    CommitOptions::default(),
                )
                .await
            {
                self.reset_failed_commit();
                return Err(EffectError::Journal(error.to_string()));
            }
        }

        self.finish_commit(PreparedEffectOutcome::Failure {
            outcome,
            event: Box::new(event),
            persisted,
        })?;

        Ok(())
    }

    fn ensure_available(&self) -> Result<(), EffectError> {
        if matches!(
            &*self
                .inner
                .state
                .lock()
                .expect("effect commit handle lock poisoned"),
            EffectCommitState::Available
        ) {
            Ok(())
        } else {
            Err(commit_handle_reuse_error())
        }
    }

    fn begin_commit(&self) -> Result<(), EffectError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("effect commit handle lock poisoned");
        if !matches!(&*state, EffectCommitState::Available) {
            return Err(commit_handle_reuse_error());
        }
        *state = EffectCommitState::InProgress;
        Ok(())
    }

    fn reset_failed_commit(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("effect commit handle lock poisoned");
        if matches!(&*state, EffectCommitState::InProgress) {
            *state = EffectCommitState::Available;
        }
    }

    fn finish_commit(&self, outcome: PreparedEffectOutcome<T>) -> Result<(), EffectError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("effect commit handle lock poisoned");
        if !matches!(&*state, EffectCommitState::InProgress) {
            return Err(commit_handle_reuse_error());
        }
        *state = EffectCommitState::Settled(Box::new(outcome));
        Ok(())
    }

    /// The outcome settled through this affine handle. With an effect boundary,
    /// the events are prepared but intentionally remain unpersisted until the
    /// controller supplies its terminal evidence for the atomic journal group.
    pub(super) fn settled_outcome(&self) -> Option<PreparedEffectOutcome<T>> {
        match &*self
            .inner
            .state
            .lock()
            .expect("effect commit handle lock poisoned")
        {
            EffectCommitState::Settled(outcome) => Some((**outcome).clone()),
            EffectCommitState::Available | EffectCommitState::InProgress => None,
        }
    }
}

fn commit_handle_reuse_error() -> EffectError {
    EffectError::Execution("transactional effect commit handle used more than once".to_string())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_domain_effect_success_facts(
    writer_id: WriterId,
    parent: &EventEnvelope<ChainEvent>,
    cursor: EffectCursor,
    descriptor_hash: EffectDescriptorHash,
    descriptor: EffectDescriptor,
    facts: Vec<TypedFact>,
    base_output_ordinal: EffectOutputOrdinal,
    origin: Option<EffectFactOrigin>,
    lineage: obzenflow_core::config::LineagePolicy,
) -> Result<Vec<ChainEvent>, EffectError> {
    if facts.is_empty() {
        return Err(EffectError::Execution(
            "domain effect outcome append requires at least one fact".to_string(),
        ));
    }

    // FLOWIP-120q: the full fact set is in hand here, so stamp the group
    // cardinality on every fact. Sized completeness checks read this back.
    let outcome_fact_count = OutcomeFactCount::new(u32::try_from(facts.len()).map_err(|_| {
        EffectError::Execution("effect outcome fact count exceeds u32 range".to_string())
    })?);

    let mut events = Vec::with_capacity(facts.len());
    for (index, fact) in facts.into_iter().enumerate() {
        let ordinal = OutcomeFactOrdinal::try_from(index).map_err(|_| {
            EffectError::Execution("effect outcome fact ordinal exceeds u32 range".to_string())
        })?;
        let output_ordinal = base_output_ordinal
            .checked_add(ordinal.get())
            .ok_or_else(|| EffectError::Execution("effect output ordinal overflow".to_string()))?;
        let record = EffectRecord {
            cursor: cursor.clone(),
            descriptor_hash: descriptor_hash.clone(),
            descriptor: descriptor.clone(),
            outcome: EffectOutcomePayload::SucceededFact {
                event_type: fact.event_type.clone(),
                output: fact.payload.clone(),
                outcome_fact_ordinal: ordinal,
                outcome_fact_count,
            },
            origin: origin.clone(),
        };

        let mut event = ChainEventFactory::derived_data_event(
            writer_id,
            &parent.event,
            fact.event_type.as_str(),
            fact.payload,
            lineage,
        );
        event.id = deterministic_event_id(
            record.cursor.recorded_flow_id.as_str(),
            record.cursor.stage_key.as_str(),
            StageInputPosition(record.cursor.input_seq.get()),
            output_ordinal,
        );
        event.processing_info.event_time = deterministic_event_time(
            StageInputPosition(record.cursor.input_seq.get()),
            output_ordinal,
        );
        // `from_record` copies the record's origin (FLOWIP-120m), so the
        // provenance carries it without a separate assignment.
        let mut provenance = EffectProvenance::from_record(&record, EffectFactOwner::User);
        provenance.outcome_fact_ordinal = Some(ordinal);
        provenance.outcome_fact_count = Some(outcome_fact_count);
        event = event.with_effect_provenance(provenance);

        events.push(event);
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_domain_effect_success_facts(
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    flow_context: Option<&FlowContext>,
    system_journal: Option<&Arc<dyn Journal<SystemEvent>>>,
    instrumentation: Option<&Arc<StageInstrumentation>>,
    heartbeat_state: Option<&Arc<HeartbeatState>>,
    output_contract: Option<&StageOutputContract>,
    backpressure_writer: &BackpressureWriter,
    writer_id: WriterId,
    parent: &EventEnvelope<ChainEvent>,
    cursor: EffectCursor,
    descriptor_hash: EffectDescriptorHash,
    descriptor: EffectDescriptor,
    facts: Vec<TypedFact>,
    base_output_ordinal: EffectOutputOrdinal,
    origin: Option<EffectFactOrigin>,
    lineage: obzenflow_core::config::LineagePolicy,
) -> Result<Vec<ChainEvent>, EffectError> {
    let events = build_domain_effect_success_facts(
        writer_id,
        parent,
        cursor,
        descriptor_hash,
        descriptor,
        facts,
        base_output_ordinal,
        origin,
        lineage,
    )?;
    let committer = OutputCommitter {
        data_journal,
        flow_context,
        system_journal,
        instrumentation,
        heartbeat_state,
        output_contract,
        backpressure_writer: Some(backpressure_writer),
        observers: None,
        observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
    };

    for event in &events {
        committer
            .commit_prebuilt(
                event.clone(),
                Some(parent),
                CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
            )
            .await
            .map_err(|e| EffectError::Journal(e.to_string()))?;
    }
    Ok(events)
}

// FLOWIP-120a: this append is intentionally write-time-unchecked for duplicate
// cursors. Callers reach it only after `history.find(&cursor)` returned `None`
// (see `Effects::perform`), and the cursor is deterministic per
// `(recorded_flow_id, stage_key, input_seq, effect_ordinal)`, where `effect_ordinal`
// is the per-input monotonic counter, unique while it has not saturated `u32`. A
// second append at the same cursor can therefore arise only from archive corruption,
// a non-atomic partial write, or ordinal saturation, each of which
// `EffectHistory::from_records` detects fail-loud on the next load (the same load
// that feeds replay). A read-before-write duplicate check is deliberately avoided
// here because it would add a TOCTOU window without removing one. Load-bearing
// ordering invariant: every live append on resume is preceded by
// `EffectHistory::load` (the transform, stateful, and sink FSMs load effect history
// before processing); if a future stage appended live before loading history, this
// fail-loud backstop would no longer cover it.
pub(super) async fn append_effect_record(
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    writer_id: WriterId,
    parent: &EventEnvelope<ChainEvent>,
    record: EffectRecord,
    lineage: obzenflow_core::config::LineagePolicy,
    backpressure_writer: &BackpressureWriter,
) -> Result<(), EffectError> {
    let event = build_effect_record_event(writer_id, parent, record, lineage)?;

    // Framework effect/capture records are physical Data rows even though the
    // transport filter hides them from handlers. Track them around the durable
    // append so downstream filtering can complete the same physical credit.
    let committer = OutputCommitter {
        data_journal,
        flow_context: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        output_contract: None,
        backpressure_writer: Some(backpressure_writer),
        observers: None,
        observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
    };
    committer
        .commit_prebuilt(event, Some(parent), CommitOptions::default())
        .await
        .map_err(|e| EffectError::Journal(e.to_string()))?;
    Ok(())
}

pub(super) fn build_effect_record_event(
    writer_id: WriterId,
    parent: &EventEnvelope<ChainEvent>,
    record: EffectRecord,
    lineage: obzenflow_core::config::LineagePolicy,
) -> Result<ChainEvent, EffectError> {
    let event_type = framework_effect_event_type(&record.descriptor.effect_type);
    let provenance = EffectProvenance::from_record(&record, EffectFactOwner::Framework);
    let payload =
        serde_json::to_value(&record).map_err(|e| EffectError::Serialization(e.to_string()))?;
    let mut event = ChainEventFactory::derived_data_event(
        writer_id,
        &parent.event,
        event_type,
        payload,
        lineage,
    )
    .with_effect_provenance(provenance);
    event.id = deterministic_effect_record_event_id(&record.cursor, event_type);
    event.processing_info.event_time = deterministic_effect_record_event_time(&record.cursor);
    if let EffectOutcomePayload::Failed { error_message, .. } = &record.outcome {
        event.processing_info.status =
            obzenflow_core::event::status::processing_status::ProcessingStatus::error_with_kind(
                error_message.clone(),
                Some(obzenflow_core::event::status::processing_status::ErrorKind::Remote),
            );
    }

    Ok(event)
}
