// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::boundary::PhysicalCallOutcome;
use super::*;

/// Slot a guarded execution future fills with the real typed outcome, so
/// `perform` records from its own state rather than boundary-returned events.
type ExecutedOutcomeSlot<O> = Arc<Mutex<Option<(O, Vec<TypedFact>)>>>;

/// Slot the guarded transactional future fills for settlement: the port's
/// result and the outcome committed through the handle, if any.
type TransactionalSettleSlot<O> =
    Arc<Mutex<Option<(Result<(), EffectError>, Option<PreparedEffectOutcome<O>>)>>>;

/// The erased effectful authoring core: every runtime declaration, output
/// contract, replay, and commit check lives here, unchanged by the typed
/// facade (FLOWIP-120z). `pub(crate)` deliberately: the unit tests exercise
/// these defence-in-depth checks through this seam, and no untyped public
/// escape exists.
pub(crate) struct EffectsCore {
    ctx: EffectInvocationContext,
    next_effect_ordinal: EffectOrdinal,
    next_output_ordinal: EffectOutputOrdinal,
    routed_output_fact_count: usize,
    committed_facts: Vec<ChainEvent>,
}

impl EffectsCore {
    pub(crate) fn new(ctx: EffectInvocationContext) -> Self {
        Self {
            ctx,
            next_effect_ordinal: EffectOrdinal::new(0),
            next_output_ordinal: EffectOutputOrdinal::new(0),
            routed_output_fact_count: 0,
            committed_facts: Vec::new(),
        }
    }

    /// Evidence of every user fact committed by this invocation, in commit
    /// order: direct emissions, effect-outcome facts, and transactional
    /// commits. Captures never appear (no `Data` fact). Read by
    /// `StageCompletion` construction (FLOWIP-120z).
    pub(crate) fn committed_fact_evidence(
        &self,
    ) -> (usize, Vec<obzenflow_core::event::types::EventType>) {
        let types = self
            .committed_facts
            .iter()
            .map(|event| obzenflow_core::event::types::EventType::from(event.event_type()))
            .collect();
        (self.committed_facts.len(), types)
    }

    /// The stage key for completion diagnostics (FLOWIP-120z).
    pub(crate) fn stage_key(&self) -> &str {
        &self.ctx.stage_key
    }

    pub(crate) fn is_replaying(&self) -> bool {
        self.ctx
            .runtime_execution
            .is_reconstructing(crate::execution::ExecutionPosition {
                stage_id: self.ctx.stage_id,
                position: self.ctx.input_seq,
                // The effect context carries no generation; the effect-miss
                // decision is positional (FLOWIP-120n F7).
                generation: None,
            })
    }

    pub(crate) fn drain_committed_facts(&mut self) -> Vec<ChainEvent> {
        std::mem::take(&mut self.committed_facts)
    }

    async fn observe_effect_outcome(
        &self,
        effect_type: &str,
        outcome: crate::stages::observer::EffectObserverOutcome,
    ) -> Result<(), EffectError> {
        let Some(observers) = self.ctx.observers.as_ref() else {
            return Ok(());
        };
        if observers.effect.is_none() {
            return Ok(());
        }
        let scope = if matches!(
            outcome,
            crate::stages::observer::EffectObserverOutcome::SuppressedByReplay
        ) {
            self.ctx
                .runtime_execution
                .scope_at(crate::execution::ExecutionPosition {
                    stage_id: self.ctx.stage_id,
                    position: self.ctx.input_seq,
                    // The effect context carries no generation; the effect-miss
                    // decision is positional (FLOWIP-120n F7).
                    generation: None,
                })
        } else {
            obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary
        };
        crate::stages::observer::dispatch::run_effect_observers(
            observers,
            self.ctx.stage_id,
            &self.ctx.stage_key,
            self.ctx.flow_context.as_ref(),
            scope,
            effect_type,
            outcome,
            &self.ctx.data_journal,
            self.ctx.instrumentation.as_ref(),
            Some(&self.ctx.parent),
        )
        .await
        .map_err(|e| EffectError::Journal(e.to_string()))
    }

    async fn observe_effect_result<T>(
        &self,
        effect_type: &str,
        result: &Result<T, EffectError>,
    ) -> Result<(), EffectError> {
        let outcome = match result {
            Ok(_) => crate::stages::observer::EffectObserverOutcome::Succeeded,
            Err(err) => crate::stages::observer::EffectObserverOutcome::Failed {
                message: err.error_message(),
            },
        };
        self.observe_effect_outcome(effect_type, outcome).await
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
                is_routable_output_fact(Some(&self.ctx.output_contract), fact.event_type.as_str())
            })
            .count()
    }

    pub(crate) async fn emit<T>(&mut self, fact: T) -> Result<(), EffectError>
    where
        T: TypedPayload,
    {
        if !self.ctx.emit_enabled {
            return Err(EffectError::EmitUnsupported {
                stage_key: self.ctx.stage_key.clone(),
            });
        }

        let event_type = T::versioned_event_type();
        if !self.ctx.output_contract.is_empty()
            && !self.ctx.output_contract.contains_event_type(&event_type)
        {
            return Err(EffectError::UndeclaredOutput {
                stage_key: self.ctx.stage_key.clone(),
                event_type,
            });
        }
        let routed_fact =
            is_routable_output_fact(Some(&self.ctx.output_contract), &event_type) as usize;
        self.ensure_routed_fanout_capacity(routed_fact)?;

        let recorded_flow_id = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id().to_string())
            .unwrap_or_else(|| self.ctx.flow_id.to_string());
        let output_ordinal = self.reserve_output_ordinal()?;
        let event = deterministic_typed_output_event(
            self.ctx.writer_id,
            &self.ctx.parent.event,
            fact,
            &recorded_flow_id,
            &self.ctx.stage_key,
            self.ctx.input_seq,
            output_ordinal,
            self.ctx.lineage,
        )?;
        let committed_event = event.clone();
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),
            system_journal: self.ctx.system_journal.as_ref(),
            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.output_contract),
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observers: None,
            observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        };
        committer
            .commit_prebuilt(
                event,
                Some(&self.ctx.parent),
                CommitOptions {
                    count_output: true,
                    validate_output_contract: true,
                },
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
        let declaration = self.ctx.effect_declaration(E::EFFECT_TYPE)?;
        // FLOWIP-120c G10: the missing-key check is a deterministic
        // validation error, so it sits above the effect-history lookup and
        // the boundary consult. Live and replay recompute the same error,
        // nothing is recorded under the cursor, and admission is never
        // charged for a call that can never execute.
        if matches!(E::SAFETY, EffectSafety::NonIdempotentRequiresKey)
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
        )?;
        let descriptor_hash = descriptor_hash(&descriptor)?;
        let recorded_flow_id = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id().to_string())
            .unwrap_or_else(|| self.ctx.flow_id.to_string());
        let cursor = EffectCursor::new(
            recorded_flow_id,
            self.ctx.stage_key.clone(),
            self.ctx.input_seq.0,
            effect_ordinal,
        );

        if let Some(history) = &self.ctx.effect_history {
            if let Some(records) = history.find_group(&cursor) {
                let output_result = self.replay_records_output::<E::Outcome>(
                    &records,
                    cursor.clone(),
                    descriptor_hash.clone(),
                );
                let output = match output_result {
                    Ok(output) => output,
                    Err(err @ EffectError::RecordedFailure { .. }) => {
                        let materialization = effect_record_group_materialization(&records)?;
                        self.append_replayed_records(
                            cursor,
                            descriptor_hash,
                            descriptor,
                            materialization,
                        )
                        .await?;
                        self.observe_effect_outcome(
                            E::EFFECT_TYPE,
                            crate::stages::observer::EffectObserverOutcome::SuppressedByReplay,
                        )
                        .await?;
                        return Err(err);
                    }
                    Err(err) => return Err(err),
                };
                let materialization = effect_record_group_materialization(&records)?;
                self.append_replayed_records(cursor, descriptor_hash, descriptor, materialization)
                    .await?;
                self.observe_effect_outcome(
                    E::EFFECT_TYPE,
                    crate::stages::observer::EffectObserverOutcome::SuppressedByReplay,
                )
                .await?;
                return Ok(output);
            }

            if self.ctx.runtime_execution.missing_outcome_is_corruption(
                crate::execution::ExecutionPosition {
                    stage_id: self.ctx.stage_id,
                    position: self.ctx.input_seq,
                    // The effect context carries no generation; the effect-miss
                    // decision is positional (FLOWIP-120n F7).
                    generation: None,
                },
            ) {
                return Err(EffectError::MissingRecordedEffect { cursor });
            }
        }

        let identity = EffectIdentity {
            effect_type: E::EFFECT_TYPE,
            safety: E::SAFETY,
            cursor: cursor.clone(),
            idempotency_key: effect.idempotency_key(),
        };

        if matches!(E::SAFETY, EffectSafety::Transactional) {
            return self
                .perform_transactional(
                    effect,
                    declaration,
                    identity,
                    cursor,
                    descriptor_hash,
                    descriptor,
                )
                .await;
        }

        let Some(boundary) = self.ctx.effect_boundary.clone() else {
            // Unguarded path: execute directly and record the outcome.
            let mut effect_ctx = self.live_effect_context();
            return match Self::execute_into_facts(effect, &mut effect_ctx).await {
                Ok((output, facts)) => {
                    self.append_success_facts(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        facts,
                        Some(EffectFactOrigin::Effect),
                    )
                    .await?;
                    self.observe_effect_outcome(
                        E::EFFECT_TYPE,
                        crate::stages::observer::EffectObserverOutcome::Succeeded,
                    )
                    .await?;
                    Ok(output)
                }
                Err(err) => {
                    self.append_failed_record(cursor, descriptor_hash, descriptor, &err)
                        .await?;
                    self.observe_effect_outcome(
                        E::EFFECT_TYPE,
                        crate::stages::observer::EffectObserverOutcome::Failed {
                            message: err.error_message(),
                        },
                    )
                    .await?;
                    Err(err)
                }
            };
        };

        // Policy path: hand the boundary a policy-neutral
        // callable for one physical call. Each call clones the effect and a
        // pristine context, while the terminal successful outcome rides the
        // slot so only `perform` can record it after the boundary returns.
        let outcome_slot: ExecutedOutcomeSlot<E::Outcome> = Arc::new(Mutex::new(None));
        let operation = {
            let slot = outcome_slot.clone();
            let writer_id = self.ctx.writer_id;
            let parent_event = self.ctx.parent.event.clone();
            let lineage = self.ctx.lineage;
            let base_context = self.live_effect_context();
            RepeatableEffectOperation::new_with_lifecycle(move |lifecycle| {
                let effect = effect.clone();
                let mut effect_ctx = base_context.clone();
                let slot = slot.clone();
                let parent_event = parent_event.clone();
                async move {
                    lifecycle.mark_started();
                    let output = match effect.execute(&mut effect_ctx).await {
                        Ok(output) => {
                            lifecycle.mark_completed(PhysicalCallOutcome::Succeeded);
                            output
                        }
                        Err(err) => {
                            lifecycle.mark_completed(PhysicalCallOutcome::Failed);
                            return Err(err);
                        }
                    };
                    let facts = output.clone().into_facts().map_err(effect_fact_set_error)?;
                    if facts.is_empty() {
                        return Err(EffectError::Execution(
                            "effect success output must author at least one fact".to_string(),
                        ));
                    }
                    let observation = facts
                        .iter()
                        .map(|fact| {
                            ChainEventFactory::derived_data_event(
                                writer_id,
                                &parent_event,
                                fact.event_type.as_str(),
                                fact.payload.clone(),
                                lineage,
                            )
                        })
                        .collect();
                    *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some((output, facts));
                    Ok(observation)
                }
            })
        };

        let report = boundary
            .around_repeatable_effect(&identity, &self.ctx.parent.event, operation)
            .await;
        let control_events = report.control_events;

        match report.outcome {
            EffectBoundaryOutcome::Executed(Ok(_observation)) => {
                let (output, facts) = outcome_slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                    .ok_or_else(|| {
                        EffectError::Execution(
                            "effect boundary reported success without an executed outcome"
                                .to_string(),
                        )
                    })?;
                self.append_success_facts_with_control_events(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    facts,
                    Some(EffectFactOrigin::Effect),
                    control_events,
                )
                .await?;
                self.observe_effect_outcome(
                    E::EFFECT_TYPE,
                    crate::stages::observer::EffectObserverOutcome::Succeeded,
                )
                .await?;
                Ok(output)
            }
            EffectBoundaryOutcome::Executed(Err(err)) => {
                self.append_failed_record_with_control_events(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    &err,
                    control_events,
                )
                .await?;
                self.observe_effect_outcome(
                    E::EFFECT_TYPE,
                    crate::stages::observer::EffectObserverOutcome::Failed {
                        message: err.error_message(),
                    },
                )
                .await?;
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
                if let Err(err) = &result {
                    self.observe_effect_outcome(
                        E::EFFECT_TYPE,
                        crate::stages::observer::EffectObserverOutcome::Failed {
                            message: err.error_message(),
                        },
                    )
                    .await?;
                }
                result
            }
        }
    }

    /// Execute an effect and decompose its outcome into authored facts.
    ///
    /// Both the empty-outcome and decomposition failures depend on the live
    /// external result, so callers record them under the effect cursor like
    /// any other execution failure and strict replay reproduces them.
    async fn execute_into_facts<E>(
        effect: E,
        effect_ctx: &mut EffectContext,
    ) -> Result<(E::Outcome, Vec<TypedFact>), EffectError>
    where
        E: Effect,
    {
        let output = effect.execute(effect_ctx).await?;
        let facts = output.clone().into_facts().map_err(effect_fact_set_error)?;
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "effect success output must author at least one fact".to_string(),
            ));
        }
        Ok((output, facts))
    }

    async fn append_failed_record(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        err: &EffectError,
    ) -> Result<(), EffectError> {
        self.append_record(EffectRecord {
            cursor,
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: err.error_type(),
                error_message: err.error_message(),
                retry: err.retry_disposition(),
                cause: err.failure_cause(),
            },
            origin: None,
        })
        .await
    }

    async fn append_failed_record_with_control_events(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        err: &EffectError,
        control_events: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        let record = EffectRecord {
            cursor: cursor.clone(),
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: err.error_type(),
                error_message: err.error_message(),
                retry: err.retry_disposition(),
                cause: err.failure_cause(),
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
            cursor: cursor.clone(),
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: err.error_type(),
                error_message: err.error_message(),
                retry: reason.retry,
                cause: Some(reason.cause),
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
        let effect_ordinal = self.reserve_effect_ordinal()?;
        let descriptor = EffectDescriptor::new(
            "obzenflow.capture",
            label,
            1,
            self.ctx.stage_logic_version.clone(),
            hash_json_value(&Value::String(label.to_string()))?,
        );
        let descriptor_hash = descriptor_hash(&descriptor)?;
        let recorded_flow_id = self
            .ctx
            .effect_history
            .as_ref()
            .map(|history| history.recorded_flow_id().to_string())
            .unwrap_or_else(|| self.ctx.flow_id.to_string());
        let cursor = EffectCursor::new(
            recorded_flow_id,
            self.ctx.stage_key.clone(),
            self.ctx.input_seq.0,
            effect_ordinal,
        );

        if let Some(history) = &self.ctx.effect_history {
            if let Some(records) = history.find_group(&cursor) {
                let output_result =
                    self.replay_capture_output(&records, cursor.clone(), descriptor_hash.clone());
                let output = match output_result {
                    Ok(output) => output,
                    Err(err @ EffectError::RecordedFailure { .. }) => {
                        let materialization = effect_record_group_materialization(&records)?;
                        self.append_replayed_records(
                            cursor,
                            descriptor_hash,
                            descriptor,
                            materialization,
                        )
                        .await?;
                        self.observe_effect_outcome(
                            "obzenflow.capture",
                            crate::stages::observer::EffectObserverOutcome::SuppressedByReplay,
                        )
                        .await?;
                        return Err(err);
                    }
                    Err(err) => return Err(err),
                };
                let materialization = effect_record_group_materialization(&records)?;
                self.append_replayed_records(cursor, descriptor_hash, descriptor, materialization)
                    .await?;
                self.observe_effect_outcome(
                    "obzenflow.capture",
                    crate::stages::observer::EffectObserverOutcome::SuppressedByReplay,
                )
                .await?;
                return Ok(output);
            }

            if self.ctx.runtime_execution.missing_outcome_is_corruption(
                crate::execution::ExecutionPosition {
                    stage_id: self.ctx.stage_id,
                    position: self.ctx.input_seq,
                    // The effect context carries no generation; the effect-miss
                    // decision is positional (FLOWIP-120n F7).
                    generation: None,
                },
            ) {
                return Err(EffectError::MissingRecordedEffect { cursor });
            }
        }

        let output =
            serde_json::to_value(&value).map_err(|e| EffectError::Serialization(e.to_string()))?;
        self.append_record(EffectRecord {
            cursor,
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Succeeded { output },
            origin: None,
        })
        .await?;
        self.observe_effect_outcome(
            "obzenflow.capture",
            crate::stages::observer::EffectObserverOutcome::Succeeded,
        )
        .await?;
        Ok(value)
    }

    async fn perform_transactional<E>(
        &mut self,
        effect: E,
        declaration: EffectDeclaration,
        identity: EffectIdentity,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let executor =
            declaration
                .transactional_executor
                .ok_or_else(|| EffectError::MissingEffectPort {
                    type_name: std::any::type_name::<dyn TransactionalEffectPort<E>>(),
                    name: "<missing transactional executor>".to_string(),
                })?;
        let port = self
            .ctx
            .effect_ports
            .get::<dyn TransactionalEffectPort<E>>(executor)
            .ok_or_else(|| EffectError::MissingEffectPort {
                type_name: std::any::type_name::<dyn TransactionalEffectPort<E>>(),
                name: executor.to_string(),
            })?;

        let mut effect_ctx = self.live_effect_context();
        let output_ordinal = self.reserve_output_ordinal()?;
        let commit = EffectCommitHandle::new(EffectCommitHandleParams {
            writer_id: self.ctx.writer_id,
            data_journal: self.ctx.data_journal.clone(),
            flow_context: self.ctx.flow_context.clone(),
            system_journal: self.ctx.system_journal.clone(),
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
            self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
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
                        (_, Some(PreparedEffectOutcome::Success { events, .. })) => {
                            Ok(events.clone())
                        }
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
            .around_single_use_effect(&identity, &self.ctx.parent.event, operation)
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
                    self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
                    return result;
                }

                // No transaction ran. Record the fail-closed provenance error
                // under this cursor so strict replay reproduces the rejection.
                self.restore_output_ordinal(output_ordinal);
                self.append_failed_record(cursor, descriptor_hash, descriptor, &err)
                    .await?;
                let result: Result<E::Outcome, EffectError> = Err(err);
                self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
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
                    self.restore_output_ordinal(output_ordinal);
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
                        &err,
                        control_events,
                    )
                    .await?;
                    let result: Result<E::Outcome, EffectError> = Err(err);
                    self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
                    return result;
                };
                self.commit_deferred_transactional_outcome(&cursor, prepared, control_events)
                    .await?;
                let result =
                    self.settle_transactional::<E>(executor, output_ordinal, port_result, outcome);
                self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
                result
            }
            SingleUseEffectBoundaryOutcome::Aborted(reason) => {
                self.restore_output_ordinal(output_ordinal);
                let result = self
                    .record_boundary_abort_with_control_events(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        reason,
                        control_events,
                    )
                    .await;
                self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
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
        T: TypedFactSet + Clone + Send + Sync + 'static,
    {
        let entries = match outcome {
            PreparedEffectOutcome::Success {
                events, persisted, ..
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
                        options: CommitOptions {
                            count_output: true,
                            validate_output_contract: true,
                        },
                        intent: StageAppendIntent::NormalStageData,
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
        self.commit_terminal_group(cursor, entries, control_events)
            .await
    }

    /// Settle a transactional attempt from the committed record, the single
    /// source of truth (FLOWIP-120a).
    fn settle_transactional<E>(
        &mut self,
        executor: &'static str,
        output_ordinal: EffectOutputOrdinal,
        port_result: Result<(), EffectError>,
        outcome: Option<PreparedEffectOutcome<E::Outcome>>,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let Some(outcome) = outcome else {
            // No commit through the handle. Surface the port's own error if it produced
            // one, otherwise the contract-violation error.
            self.restore_output_ordinal(output_ordinal);
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
                fact_count,
                events,
                ..
            } => {
                self.advance_output_ordinals_after_reserved_base(output_ordinal, fact_count)?;
                self.committed_facts.extend(events);
                Ok(output)
            }
            PreparedEffectOutcome::Failure { outcome, .. } => {
                self.restore_output_ordinal(output_ordinal);
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

    fn replay_records_output<T>(
        &self,
        records: &[&EffectRecord],
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
    ) -> Result<T, EffectError>
    where
        T: TypedFactSet,
    {
        for record in records {
            if record.descriptor_hash != descriptor_hash {
                return Err(EffectError::DescriptorMismatch {
                    cursor,
                    expected: descriptor_hash.clone(),
                    recorded: record.descriptor_hash.clone(),
                });
            }
        }

        decode_effect_outcome_group::<T>(records)
    }

    fn replay_capture_output<T>(
        &self,
        records: &[&EffectRecord],
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
    ) -> Result<T, EffectError>
    where
        T: DeserializeOwned,
    {
        let [record] = records else {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "capture cursor {cursor:?} recorded {} outcome records",
                records.len()
            )));
        };
        if record.descriptor_hash != descriptor_hash {
            return Err(EffectError::DescriptorMismatch {
                cursor,
                expected: descriptor_hash.clone(),
                recorded: record.descriptor_hash.clone(),
            });
        }

        decode_effect_outcome(&record.outcome)
    }

    fn live_effect_context(&self) -> EffectContext {
        EffectContext {
            is_replaying: false,
            flow_id: self.ctx.flow_id,
            stage_key: self.ctx.stage_key.clone(),
            input_seq: self.ctx.input_seq,
            ports: self.ctx.effect_ports.clone(),
        }
    }

    async fn append_record(&self, record: EffectRecord) -> Result<(), EffectError> {
        append_effect_record(
            &self.ctx.data_journal,
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            self.ctx.lineage,
            &self.ctx.backpressure_writer,
        )
        .await
    }

    async fn append_success_facts(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        facts: Vec<TypedFact>,
        origin: Option<EffectFactOrigin>,
    ) -> Result<(), EffectError> {
        if facts.is_empty() {
            return Err(EffectError::Execution(
                "effect success output must author at least one fact".to_string(),
            ));
        }
        let routed_fact_count = self.count_routed_facts(&facts);
        self.ensure_routed_fanout_capacity(routed_fact_count)?;
        let output_ordinal = self.reserve_output_ordinals(facts.len())?;
        let committed_events = append_domain_effect_success_facts(
            &self.ctx.data_journal,
            self.ctx.flow_context.as_ref(),
            self.ctx.system_journal.as_ref(),
            self.ctx.instrumentation.as_ref(),
            self.ctx.heartbeat_state.as_ref(),
            Some(&self.ctx.output_contract),
            &self.ctx.backpressure_writer,
            self.ctx.writer_id,
            &self.ctx.parent,
            cursor,
            descriptor_hash,
            descriptor,
            facts,
            output_ordinal,
            origin,
            self.ctx.lineage,
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
        self.commit_terminal_group(&cursor, outcome_entries, control_events)
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
    ) -> Result<(), EffectError> {
        outcome_entries.extend(control_events.into_iter().map(|event| AtomicCommitEntry {
            event,
            options: CommitOptions::default(),
            intent: StageAppendIntent::FrameworkObservability,
        }));
        let group_id = effect_outcome_group_id(cursor);
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),
            system_journal: self.ctx.system_journal.as_ref(),
            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.output_contract),
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observers: None,
            observer_scope: obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        };
        committer
            .commit_atomic_group(group_id.as_str(), outcome_entries, Some(&self.ctx.parent))
            .await
            .map_err(|error| EffectError::Journal(error.to_string()))?;
        Ok(())
    }

    async fn append_replayed_records(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        materialization: EffectRecordMaterialization,
    ) -> Result<(), EffectError> {
        match materialization {
            EffectRecordMaterialization::DomainFacts {
                facts,
                origin: recorded_origin,
            } => {
                // The recorded provenance wins. Older records without an
                // explicit origin came from the effect itself.
                let origin = recorded_origin.or(Some(EffectFactOrigin::Effect));
                self.append_success_facts(cursor, descriptor_hash, descriptor, facts, origin)
                    .await
            }
            EffectRecordMaterialization::FrameworkRecords(records) => {
                for record in records {
                    self.append_record(record).await?;
                }
                Ok(())
            }
        }
    }
}
