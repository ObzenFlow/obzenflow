// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

/// Slot a guarded execution future fills with the real typed outcome, so
/// `perform` records from its own state rather than boundary-returned events.
type ExecutedOutcomeSlot<O> = Arc<Mutex<Option<(O, Vec<TypedFact>)>>>;

/// Slot the guarded transactional future fills for settlement: the port's
/// result and the outcome committed through the handle, if any.
type TransactionalSettleSlot<O> =
    Arc<Mutex<Option<(Result<(), EffectError>, Option<CommittedEffectOutcome<O>>)>>>;

pub struct Effects {
    ctx: EffectInvocationContext,
    next_effect_ordinal: EffectOrdinal,
    next_output_ordinal: EffectOutputOrdinal,
    routed_output_fact_count: usize,
    committed_facts: Vec<ChainEvent>,
}

impl Effects {
    pub fn new(ctx: EffectInvocationContext) -> Self {
        Self {
            ctx,
            next_effect_ordinal: EffectOrdinal::new(0),
            next_output_ordinal: EffectOutputOrdinal::new(0),
            routed_output_fact_count: 0,
            committed_facts: Vec::new(),
        }
    }

    pub fn is_replaying(&self) -> bool {
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

    pub fn drain_committed_facts(&mut self) -> Vec<ChainEvent> {
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

    pub async fn emit<T>(&mut self, fact: T) -> Result<(), EffectError>
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

    pub async fn perform<E>(&mut self, effect: E) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        let declaration = self.ctx.effect_declaration(E::EFFECT_TYPE)?;
        self.validate_typed_outcome_coordination::<E>()?;
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

        // Guarded path (FLOWIP-120c): hand the execution to the boundary as a
        // future so policies wrap it. The future owns its captures; on success
        // it yields observation-grade derived copies for the policies, and the
        // real outcome rides the slot so the record is written from `perform`'s
        // own state, never from boundary-returned events.
        let outcome_slot: ExecutedOutcomeSlot<E::Outcome> = Arc::new(Mutex::new(None));
        let execute: EffectExecution = {
            let slot = outcome_slot.clone();
            let writer_id = self.ctx.writer_id;
            let parent_event = self.ctx.parent.event.clone();
            let lineage = self.ctx.lineage;
            let mut effect_ctx = self.live_effect_context();
            Box::pin(async move {
                let (output, facts) = Self::execute_into_facts(effect, &mut effect_ctx).await?;
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
            })
        };

        let report = boundary
            .around_effect(&identity, &self.ctx.parent.event, execute)
            .await;
        self.ctx.push_boundary_control_events(report.control_events);

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
            EffectBoundaryOutcome::Executed(Err(err)) => {
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
            EffectBoundaryOutcome::Skipped { results, source } => {
                let result = self
                    .record_boundary_skip::<E>(cursor, descriptor_hash, descriptor, results, source)
                    .await;
                match &result {
                    Ok(_) => {
                        self.observe_effect_outcome(
                            E::EFFECT_TYPE,
                            crate::stages::observer::EffectObserverOutcome::Succeeded,
                        )
                        .await?;
                    }
                    Err(err) => {
                        self.observe_effect_outcome(
                            E::EFFECT_TYPE,
                            crate::stages::observer::EffectObserverOutcome::Failed {
                                message: err.error_message(),
                            },
                        )
                        .await?;
                    }
                }
                result
            }
            EffectBoundaryOutcome::Aborted(reason) => {
                let result = self
                    .record_boundary_abort(cursor, descriptor_hash, descriptor, reason)
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

    async fn record_boundary_skip<E>(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        results: Vec<ChainEvent>,
        source: Option<String>,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect,
    {
        if results.is_empty() {
            // An empty skip must still record an outcome under the
            // effect cursor; otherwise strict replay of this input
            // fails with MissingRecordedEffect, a false corruption
            // signal for a rejection the live run made deliberately.
            let err = EffectError::BoundaryRejected {
                rejected_by: EffectFailureSource::new("effect_boundary"),
                code: EffectFailureCode::new("skip_without_facts"),
                message: "effect boundary skipped without fallback output facts".to_string(),
                retry: RetryDisposition::NotRetryable,
            };
            self.append_failed_record(cursor, descriptor_hash, descriptor, &err)
                .await?;
            return Err(err);
        }
        let facts = results
            .iter()
            .map(|event| {
                TypedFact::from_event(event).ok_or_else(|| {
                    EffectError::Execution(
                        "effect boundary fallback output must be Data facts".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = E::Outcome::try_from_facts(&facts).map_err(effect_fact_set_error)?;
        let origin = Some(EffectFactOrigin::MiddlewareSynthesized {
            label: source.unwrap_or_else(|| "effect_boundary".to_string()),
        });
        self.append_success_facts(cursor, descriptor_hash, descriptor, facts, origin)
            .await?;
        Ok(output)
    }

    async fn record_boundary_abort<T>(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        reason: EffectAbortReason,
    ) -> Result<T, EffectError> {
        let err = EffectError::BoundaryRejected {
            rejected_by: reason.cause.source.clone(),
            code: reason.cause.code.clone(),
            message: reason.message.clone(),
            retry: reason.retry,
        };
        self.append_record(EffectRecord {
            cursor,
            descriptor_hash,
            descriptor,
            outcome: EffectOutcomePayload::Failed {
                error_type: err.error_type(),
                error_message: err.error_message(),
                retry: reason.retry,
                cause: Some(reason.cause),
            },
            origin: None,
        })
        .await?;
        Err(err)
    }

    /// FLOWIP-120h/120m: validate wrapper coordination before any I/O.
    ///
    /// Branch-shaped registrations (FLOWIP-120h) require the `Guarded`
    /// wrapper, so a breaker branch always has a carrier that can decode it.
    /// Outcome-shaped registrations (FLOWIP-120m) require the inverse: the
    /// middleware synthesizes the effect's own outcome facts, so the handler
    /// performs the plain effect and a `Guarded` wrapper is rejected.
    /// Transactional effects route through the boundary for admission and
    /// observation only (FLOWIP-120c H5, rejection-only in v1), so neither
    /// shape can protect a transactional effect.
    fn validate_typed_outcome_coordination<E>(&self) -> Result<(), EffectError>
    where
        E: Effect,
    {
        let synthesized = E::Outcome::synthesized_fact_types();
        let registration = self.ctx.synthesized_outcome_registration(E::EFFECT_TYPE);

        match (synthesized.is_empty(), registration) {
            (true, None) => Ok(()),
            (true, Some(registration))
                if registration.kind == SynthesizedOutcomeKind::OutcomeShaped =>
            {
                if matches!(E::SAFETY, EffectSafety::Transactional) {
                    return Err(EffectError::TypedOutcomeCoordination {
                        stage_key: self.ctx.stage_key.clone(),
                        effect_type: E::EFFECT_TYPE.to_string(),
                        message: "transactional effects cannot be protected by an \
                                  outcome-shaped fallback; the transactional path runs \
                                  before the effect boundary"
                            .to_string(),
                    });
                }
                Ok(())
            }
            (true, Some(registration)) => Err(EffectError::TypedOutcomeCoordination {
                stage_key: self.ctx.stage_key.clone(),
                effect_type: E::EFFECT_TYPE.to_string(),
                message: format!(
                    "stage registers typed-outcome middleware '{}'; perform the guarded \
                     wrapper so its branch facts can decode",
                    registration.source_label,
                ),
            }),
            (false, None) => Err(EffectError::TypedOutcomeCoordination {
                stage_key: self.ctx.stage_key.clone(),
                effect_type: E::EFFECT_TYPE.to_string(),
                message: "guarded effect performed on a stage with no typed-outcome \
                          middleware registration; declare the breaker in the \
                          output_middleware: lane or perform the inner effect directly"
                    .to_string(),
            }),
            (false, Some(registration))
                if registration.kind == SynthesizedOutcomeKind::OutcomeShaped =>
            {
                Err(EffectError::TypedOutcomeCoordination {
                    stage_key: self.ctx.stage_key.clone(),
                    effect_type: E::EFFECT_TYPE.to_string(),
                    message: "outcome-shaped fallback uses the plain perform; drop the \
                              Guarded wrapper, because every branch resumes the handler \
                              with the effect's own outcome carrier"
                        .to_string(),
                })
            }
            (false, Some(registration)) => {
                if matches!(E::SAFETY, EffectSafety::Transactional) {
                    return Err(EffectError::TypedOutcomeCoordination {
                        stage_key: self.ctx.stage_key.clone(),
                        effect_type: E::EFFECT_TYPE.to_string(),
                        message: "transactional effects cannot be guarded; the transactional \
                                  path runs before the effect boundary"
                            .to_string(),
                    });
                }
                for fact_type in &registration.fact_types {
                    if !synthesized
                        .iter()
                        .any(|member| member.event_type == fact_type.event_type)
                    {
                        return Err(EffectError::TypedOutcomeCoordination {
                            stage_key: self.ctx.stage_key.clone(),
                            effect_type: E::EFFECT_TYPE.to_string(),
                            message: format!(
                                "registered branch fact '{}' is not a member of the guarded \
                                 carrier's synthesized fact set",
                                fact_type.event_type,
                            ),
                        });
                    }
                }
                Ok(())
            }
        }
    }

    pub async fn capture<T>(&mut self, label: &'static str, value: T) -> Result<T, EffectError>
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
            parent: self.ctx.parent.clone(),
            cursor: cursor.clone(),
            descriptor_hash: descriptor_hash.clone(),
            descriptor: descriptor.clone(),
            output_ordinal,
            lineage: self.ctx.lineage,
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
            let outcome = commit_observer.committed_outcome();
            let result =
                self.settle_transactional::<E>(executor, output_ordinal, port_result, outcome);
            self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
            return result;
        };

        // FLOWIP-120c H5: transactional effects route through the boundary.
        // Admission runs before `execute_and_commit`, observation after
        // `committed_outcome()`; rejection-only in v1, no fallback synthesis.
        let settle_slot: TransactionalSettleSlot<E::Outcome> = Arc::new(Mutex::new(None));
        let execute: EffectExecution = {
            let slot = settle_slot.clone();
            let observer = commit_observer.clone();
            let executor_name = executor.to_string();
            Box::pin(async move {
                let port_result = port
                    .execute_and_commit(effect, &mut effect_ctx, commit)
                    .await
                    .map(|_| ());
                let outcome = observer.committed_outcome();
                // The boundary observes a faithful classification of how the
                // attempt ended; the precise error the caller sees is settled
                // from the slot, never from this observation result.
                let observation = match (&port_result, &outcome) {
                    (_, Some(CommittedEffectOutcome::Success { events, .. })) => Ok(events.clone()),
                    (_, Some(CommittedEffectOutcome::Failure(payload))) => {
                        Err(match recorded_failure_from_outcome::<E::Outcome>(payload) {
                            Err(err) => err,
                            Ok(_) => EffectError::EffectProvenanceMismatch(
                                "expected recorded effect failure".to_string(),
                            ),
                        })
                    }
                    (Err(err), None) => Err(EffectError::Execution(err.to_string())),
                    (Ok(()), None) => Err(EffectError::TransactionalCommitMissing {
                        effect_type: E::EFFECT_TYPE.to_string(),
                        executor: executor_name,
                    }),
                };
                *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some((port_result, outcome));
                observation
            })
        };

        let report = boundary
            .around_effect(&identity, &self.ctx.parent.event, execute)
            .await;
        self.ctx.push_boundary_control_events(report.control_events);

        match report.outcome {
            EffectBoundaryOutcome::Executed(_) => {
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
                let result =
                    self.settle_transactional::<E>(executor, output_ordinal, port_result, outcome);
                self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
                result
            }
            EffectBoundaryOutcome::Skipped { source, .. } => {
                // H5 v1: no fallback synthesis for transactional effects; the
                // skip is recorded as a failed outcome so replay reproduces it.
                self.restore_output_ordinal(output_ordinal);
                let err = EffectError::BoundaryRejected {
                    rejected_by: EffectFailureSource::new(
                        source.as_deref().unwrap_or("effect_boundary"),
                    ),
                    code: EffectFailureCode::new("transactional_fallback_unsupported"),
                    message: "transactional effects accept no boundary fallback synthesis"
                        .to_string(),
                    retry: RetryDisposition::NotRetryable,
                };
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
            EffectBoundaryOutcome::Aborted(reason) => {
                self.restore_output_ordinal(output_ordinal);
                let result = self
                    .record_boundary_abort(cursor, descriptor_hash, descriptor, reason)
                    .await;
                self.observe_effect_result(E::EFFECT_TYPE, &result).await?;
                result
            }
        }
    }

    /// Settle a transactional attempt from the committed record, the single
    /// source of truth (FLOWIP-120a).
    fn settle_transactional<E>(
        &mut self,
        executor: &'static str,
        output_ordinal: EffectOutputOrdinal,
        port_result: Result<(), EffectError>,
        outcome: Option<CommittedEffectOutcome<E::Outcome>>,
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
            CommittedEffectOutcome::Success {
                output,
                fact_count,
                events,
            } => {
                self.advance_output_ordinals_after_reserved_base(output_ordinal, fact_count)?;
                self.committed_facts.extend(events);
                Ok(output)
            }
            CommittedEffectOutcome::Failure(outcome) => {
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
                // The recorded origin wins (FLOWIP-120m): it rides the fact's
                // provenance, so replay reconstructs the branch origin without
                // consulting registrations, which is ambiguous once an
                // outcome-shaped fallback synthesizes the effect's own facts.
                // Registration-based derivation remains only as the fallback
                // for pre-120h journals whose records carry no origin.
                let origin = recorded_origin.or_else(|| self.derive_replayed_origin(&facts));
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

    fn derive_replayed_origin(&self, facts: &[TypedFact]) -> Option<EffectFactOrigin> {
        let synthesized = self.ctx.synthesized_outcomes.iter().find(|registration| {
            facts.iter().all(|fact| {
                registration
                    .fact_types
                    .iter()
                    .any(|branch| branch.event_type == fact.event_type)
            })
        });
        Some(match synthesized {
            Some(registration) => EffectFactOrigin::MiddlewareSynthesized {
                label: registration.source_label.clone(),
            },
            None => EffectFactOrigin::Effect,
        })
    }
}
