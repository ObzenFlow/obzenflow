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
    parent: EventEnvelope<ChainEvent>,
    cursor: EffectCursor,
    descriptor_hash: EffectDescriptorHash,
    descriptor: EffectDescriptor,
    output_ordinal: EffectOutputOrdinal,
    lineage: obzenflow_core::config::LineagePolicy,
    committed: Mutex<Option<CommittedEffectOutcome<T>>>,
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
    pub(super) parent: EventEnvelope<ChainEvent>,
    pub(super) cursor: EffectCursor,
    pub(super) descriptor_hash: EffectDescriptorHash,
    pub(super) descriptor: EffectDescriptor,
    pub(super) output_ordinal: EffectOutputOrdinal,
    pub(super) lineage: obzenflow_core::config::LineagePolicy,
}

#[derive(Clone)]
pub(super) enum CommittedEffectOutcome<T> {
    Success {
        output: T,
        fact_count: usize,
        events: Vec<ChainEvent>,
    },
    Failure(EffectOutcomePayload),
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
                parent: params.parent,
                cursor: params.cursor,
                descriptor_hash: params.descriptor_hash,
                descriptor: params.descriptor,
                output_ordinal: params.output_ordinal,
                lineage: params.lineage,
                committed: Mutex::new(None),
                _marker: PhantomData,
            }),
        }
    }

    pub async fn commit_success(&self, output: &T) -> Result<(), EffectError> {
        {
            let committed = self
                .inner
                .committed
                .lock()
                .expect("effect commit handle lock poisoned");
            if committed.is_some() {
                return Err(EffectError::Execution(
                    "transactional effect commit handle used more than once".to_string(),
                ));
            }
        }

        let facts = output.clone().into_facts().map_err(effect_fact_set_error)?;
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "effect success output must author at least one fact".to_string(),
            ));
        }
        let fact_count = facts.len();
        let committed_events = append_domain_effect_success_facts(
            &self.inner.data_journal,
            self.inner.flow_context.as_ref(),
            self.inner.system_journal.as_ref(),
            self.inner.instrumentation.as_ref(),
            self.inner.heartbeat_state.as_ref(),
            Some(&self.inner.output_contract),
            self.inner.writer_id,
            &self.inner.parent,
            self.inner.cursor.clone(),
            self.inner.descriptor_hash.clone(),
            self.inner.descriptor.clone(),
            facts,
            self.inner.output_ordinal,
            Some(EffectFactOrigin::Effect),
            self.inner.lineage,
        )
        .await?;

        let mut committed = self
            .inner
            .committed
            .lock()
            .expect("effect commit handle lock poisoned");
        if committed.is_some() {
            return Err(EffectError::Execution(
                "transactional effect commit handle used more than once".to_string(),
            ));
        }
        *committed = Some(CommittedEffectOutcome::Success {
            output: output.clone(),
            fact_count,
            events: committed_events,
        });
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
        {
            let mut committed = self
                .inner
                .committed
                .lock()
                .expect("effect commit handle lock poisoned");
            if committed.is_some() {
                return Err(EffectError::Execution(
                    "transactional effect commit handle used more than once".to_string(),
                ));
            }
            *committed = Some(CommittedEffectOutcome::Failure(outcome.clone()));
        }

        let record = EffectRecord {
            cursor: self.inner.cursor.clone(),
            descriptor_hash: self.inner.descriptor_hash.clone(),
            descriptor: self.inner.descriptor.clone(),
            outcome: outcome.clone(),
            origin: None,
        };

        if source_error.is_none() {
            return Err(EffectError::Execution(
                "domain success outcomes must be committed through commit_success".to_string(),
            ));
        }
        let append_result = append_effect_record(
            &self.inner.data_journal,
            self.inner.writer_id,
            &self.inner.parent,
            record,
            self.inner.lineage,
        )
        .await;

        if let Err(err) = append_result {
            let mut committed = self
                .inner
                .committed
                .lock()
                .expect("effect commit handle lock poisoned");
            *committed = None;
            return Err(err);
        }

        Ok(())
    }

    /// The outcome committed through this handle, or `None` if the port never
    /// committed. This is the source of truth for the live transactional return
    /// value, so a live run decodes the same outcome a replay would.
    pub(super) fn committed_outcome(&self) -> Option<CommittedEffectOutcome<T>> {
        self.inner
            .committed
            .lock()
            .expect("effect commit handle lock poisoned")
            .clone()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_domain_effect_success_facts(
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    flow_context: Option<&FlowContext>,
    system_journal: Option<&Arc<dyn Journal<SystemEvent>>>,
    instrumentation: Option<&Arc<StageInstrumentation>>,
    heartbeat_state: Option<&Arc<HeartbeatState>>,
    output_contract: Option<&StageOutputContract>,
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

    let committer = OutputCommitter {
        data_journal,
        flow_context,
        system_journal,
        instrumentation,
        heartbeat_state,
        output_contract,
        observers: None,
        observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
    };

    let mut committed_events = Vec::new();
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

        let committed_event = event.clone();
        committer
            .commit_prebuilt(
                event,
                Some(parent),
                CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
            )
            .await
            .map_err(|e| EffectError::Journal(e.to_string()))?;
        committed_events.push(committed_event);
    }
    Ok(committed_events)
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
) -> Result<(), EffectError> {
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

    // FLOWIP-120b Step 1: route the framework effect/capture record append
    // through the shared OutputCommitter. With only the journal handle present,
    // this is the same credit-free, unenriched, unmirrored append it was before,
    // but it now shares the one commit path the supervisor drain uses.
    let committer = OutputCommitter {
        data_journal,
        flow_context: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        output_contract: None,
        observers: None,
        observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
    };
    committer
        .commit_prebuilt(event, Some(parent), CommitOptions::default())
        .await
        .map_err(|e| EffectError::Journal(e.to_string()))?;
    Ok(())
}
