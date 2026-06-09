// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

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
        !matches!(self.ctx.effect_runtime_mode, EffectRuntimeMode::Live)
    }

    pub fn drain_committed_facts(&mut self) -> Vec<ChainEvent> {
        std::mem::take(&mut self.committed_facts)
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
        )?;
        let committed_event = event.clone();
        let committer = OutputCommitter {
            data_journal: &self.ctx.data_journal,
            flow_context: self.ctx.flow_context.as_ref(),
            system_journal: self.ctx.system_journal.as_ref(),
            instrumentation: self.ctx.instrumentation.as_ref(),
            heartbeat_state: self.ctx.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.output_contract),
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

    pub async fn perform<E>(&mut self, effect: E) -> Result<E::Output, EffectError>
    where
        E: Effect,
    {
        let declaration = self.ctx.effect_declaration(E::EFFECT_TYPE)?;
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
                let output = self.replay_records_output::<E::Output>(
                    &records,
                    cursor.clone(),
                    descriptor_hash.clone(),
                )?;
                if let Some(facts) = effect_record_group_to_facts(&records)? {
                    self.append_success_facts(cursor, descriptor_hash, descriptor, facts)
                        .await?;
                }
                return Ok(output);
            }

            if matches!(
                self.ctx.effect_runtime_mode,
                EffectRuntimeMode::ReplayStrict
            ) {
                return Err(EffectError::MissingRecordedEffect { cursor });
            }
        }

        if matches!(E::SAFETY, EffectSafety::Transactional) {
            return self
                .perform_transactional(effect, declaration, cursor, descriptor_hash, descriptor)
                .await;
        }

        let boundary_start = if let Some(boundary) = &self.ctx.effect_boundary {
            let EffectBoundaryStart {
                action,
                context,
                control_events,
            } = boundary.before_effect(&self.ctx.parent.event);
            match action {
                EffectBoundaryAction::Continue => Some(context),
                EffectBoundaryAction::Skip(results) => {
                    if results.is_empty() {
                        return Err(EffectError::Execution(
                            "effect boundary skipped without fallback output facts".to_string(),
                        ));
                    }
                    let facts = results
                        .iter()
                        .map(|event| {
                            TypedFact::from_event(event).ok_or_else(|| {
                                EffectError::Execution(
                                    "effect boundary fallback output must be Data facts"
                                        .to_string(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    self.ctx.push_boundary_control_events(control_events);
                    let output =
                        E::Output::try_from_facts(&facts).map_err(effect_fact_set_error)?;
                    self.append_success_facts(cursor, descriptor_hash, descriptor, facts)
                        .await?;
                    return Ok(output);
                }
                EffectBoundaryAction::Abort => {
                    self.ctx.push_boundary_control_events(control_events);
                    let err =
                        EffectError::Execution("effect boundary aborted execution".to_string());
                    self.append_record(
                        EffectRecord {
                            cursor,
                            descriptor_hash,
                            descriptor,
                            outcome: EffectOutcomePayload::Failed {
                                error_type: err.error_type(),
                                error_message: err.error_message(),
                                retry: err.retry_disposition(),
                            },
                        },
                        Some(&err),
                    )
                    .await?;
                    return Err(err);
                }
            }
        } else {
            None
        };

        if matches!(E::SAFETY, EffectSafety::NonIdempotentRequiresKey)
            && effect.idempotency_key().is_none()
        {
            return Err(EffectError::MissingIdempotencyKey {
                effect_type: E::EFFECT_TYPE.to_string(),
            });
        }

        let mut effect_ctx = self.live_effect_context();

        match effect.execute(&mut effect_ctx).await {
            Ok(output) => {
                let facts = output.clone().into_facts().map_err(effect_fact_set_error)?;
                if facts.is_empty() {
                    return Err(EffectError::Execution(
                        "effect success output must author at least one fact".to_string(),
                    ));
                }
                if let (Some(boundary), Some(boundary_context)) =
                    (&self.ctx.effect_boundary, boundary_start)
                {
                    let output_events = facts
                        .iter()
                        .map(|fact| {
                            ChainEventFactory::derived_data_event(
                                self.ctx.writer_id,
                                &self.ctx.parent.event,
                                fact.event_type.as_str(),
                                fact.payload.clone(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let control_events = boundary.after_effect(
                        boundary_context,
                        &self.ctx.parent.event,
                        &output_events,
                    );
                    self.ctx.push_boundary_control_events(control_events);
                }
                self.append_success_facts(cursor, descriptor_hash, descriptor, facts)
                    .await?;
                Ok(output)
            }
            Err(err) => {
                if let (Some(boundary), Some(boundary_context)) =
                    (&self.ctx.effect_boundary, boundary_start)
                {
                    let error_event = self.ctx.parent.event.clone().mark_as_error(
                        err.to_string(),
                        obzenflow_core::event::status::processing_status::ErrorKind::Remote,
                    );
                    let control_events = boundary.after_effect(
                        boundary_context,
                        &self.ctx.parent.event,
                        std::slice::from_ref(&error_event),
                    );
                    self.ctx.push_boundary_control_events(control_events);
                }
                self.append_record(
                    EffectRecord {
                        cursor,
                        descriptor_hash,
                        descriptor,
                        outcome: EffectOutcomePayload::Failed {
                            error_type: err.error_type(),
                            error_message: err.error_message(),
                            retry: err.retry_disposition(),
                        },
                    },
                    Some(&err),
                )
                .await?;
                Err(err)
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
                return self.replay_capture_output(&records, cursor, descriptor_hash);
            }

            if matches!(
                self.ctx.effect_runtime_mode,
                EffectRuntimeMode::ReplayStrict
            ) {
                return Err(EffectError::MissingRecordedEffect { cursor });
            }
        }

        let output =
            serde_json::to_value(&value).map_err(|e| EffectError::Serialization(e.to_string()))?;
        self.append_record(
            EffectRecord {
                cursor,
                descriptor_hash,
                descriptor,
                outcome: EffectOutcomePayload::Succeeded { output },
            },
            None,
        )
        .await?;
        Ok(value)
    }

    async fn perform_transactional<E>(
        &mut self,
        effect: E,
        declaration: EffectDeclaration,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
    ) -> Result<E::Output, EffectError>
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
            cursor,
            descriptor_hash,
            descriptor,
            output_ordinal,
        });
        let commit_observer = commit.clone();
        // The port commits its outcome through the handle. Its returned value is
        // intentionally not used as the live result: the committed record is the single
        // source of truth, so a live run decodes the same journaled outcome a replay
        // would, and a port that commits one outcome but returns another cannot make
        // live output diverge from replay output (FLOWIP-120a).
        let port_result = port
            .execute_and_commit(effect, &mut effect_ctx, commit)
            .await;

        let Some(outcome) = commit_observer.committed_outcome() else {
            // No commit through the handle. Surface the port's own error if it produced
            // one, otherwise the contract-violation error.
            return Err(match port_result {
                Err(err) => err,
                Ok(_) => EffectError::TransactionalCommitMissing {
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
            CommittedEffectOutcome::Failure(outcome) => recorded_failure_from_outcome(&outcome),
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

    async fn append_record(
        &self,
        record: EffectRecord,
        source_error: Option<&EffectError>,
    ) -> Result<(), EffectError> {
        append_effect_record(
            &self.ctx.data_journal,
            self.ctx.writer_id,
            &self.ctx.parent,
            record,
            source_error,
        )
        .await
    }

    async fn append_success_facts(
        &mut self,
        cursor: EffectCursor,
        descriptor_hash: EffectDescriptorHash,
        descriptor: EffectDescriptor,
        facts: Vec<TypedFact>,
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
        )
        .await?;
        self.routed_output_fact_count = self
            .routed_output_fact_count
            .checked_add(routed_fact_count)
            .ok_or_else(|| EffectError::Execution("routed output fanout overflow".to_string()))?;
        self.committed_facts.extend(committed_events);
        Ok(())
    }
}
