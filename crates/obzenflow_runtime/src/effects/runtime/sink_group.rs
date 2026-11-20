// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::messaging::DeliveredRecord;
use crate::stages::common::handler_error::HandlerError;
use obzenflow_core::TypedPayload;

type Member = (AtomicCommitEntry, DeliveredRecord<ChainPayload>);

struct Pending<R: SinkRequest> {
    index: usize,
    declaration: EffectDeclaration,
    call: Call<R>,
    live: LiveCall,
    permit: SinkPermit,
}

fn sink_attempt(prior: usize, started: u32) -> Result<u32, EffectError> {
    u32::try_from(prior)
        .ok()
        .and_then(|n| n.checked_add(started))
        .ok_or_else(|| EffectError::Execution("sink attempt ordinal exhausted".into()))
}

fn prepare_reply<R: CallRequest>(value: &R::Outcome) -> Result<PreparedEffectSuccess, EffectError> {
    R::Records::prepare_success(value)
}

impl AffineStart {
    fn member(&self, attempt: u32) -> Result<Member, EffectError> {
        Ok((
            AtomicCommitEntry {
                event: self.event(attempt)?,
                options: CommitOptions::default(),
                intent: StageAppendIntent::NonDataStageFact,
            },
            self.parent.clone(),
        ))
    }
}

impl EffectsCore {
    fn sink_committer(&self) -> OutputCommitter<'_> {
        OutputCommitter {
            data_journal: &self.ctx.stage.data_journal,
            flow_context: self.ctx.stage.flow_context.as_ref(),
            instrumentation: self.ctx.stage.instrumentation.as_ref(),
            heartbeat_state: self.ctx.stage.heartbeat_state.as_ref(),
            output_contract: Some(&self.ctx.stage.output_contract),
            backpressure_writer: Some(&self.ctx.backpressure_writer),
            observer_scope: MiddlewareExecutionScope::LiveEffectBoundary,
        }
    }

    async fn publish_sink_members(
        &self,
        prefix: &str,
        members: Vec<Member>,
    ) -> Result<(), EffectError> {
        if let Some((entry, _)) = members.first() {
            let id = format!("{prefix}{}", entry.event.id);
            self.sink_committer()
                .commit_group_with_parents(id, members)
                .await
                .map_err(|e| EffectError::Journal(e.to_string()))?;
        }
        Ok(())
    }

    async fn sink_reply<R: SinkRequest>(
        &mut self,
        effect: PreparedLiveEffect,
        reply: Result<R::Outcome, EffectError>,
        attempt: Option<EffectAttemptOrdinal>,
        controls: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        let prepared = reply
            .map_err(|e| Box::new((EffectObservation::returned_failure(), e)))
            .and_then(|value| {
                prepare_reply::<R>(&value)
                    .map_err(|e| Box::new((EffectObservation::OutcomePreparationFailed {}, e)))
            });
        let PreparedLiveEffect {
            cursor,
            descriptor_hash,
            descriptor,
            ..
        } = effect;
        match (prepared, attempt) {
            (Ok(success), Some(n)) => {
                self.append_affine_success(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    success,
                    n,
                    controls,
                )
                .await
            }
            (Ok(success), None) => {
                self.append_success_with_control_events(
                    cursor,
                    descriptor_hash,
                    descriptor,
                    success,
                    None,
                    controls,
                )
                .await
            }
            (Err(failure), attempt) => {
                let (observation, error) = *failure;
                if let Some(n) = attempt {
                    self.append_affine_failed_record(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        (observation, &error),
                        n,
                        controls,
                    )
                    .await?;
                } else {
                    self.append_failed_record_with_control_events(
                        cursor,
                        descriptor_hash,
                        descriptor,
                        observation,
                        &error,
                        controls,
                    )
                    .await?;
                }
                Err(error)
            }
        }
    }

    async fn sink_refused(
        &mut self,
        effect: &PreparedLiveEffect,
        prior: &[EffectAttemptStarted],
        started: u32,
        reason: EffectAbortReason,
        controls: Vec<ChainEvent>,
    ) -> Result<(), EffectError> {
        if is_affine(effect.identity.safety) && (started > 0 || !prior.is_empty()) {
            self.record_recovery_abandonment(RecoveryAbandonment {
                cursor: effect.cursor.clone(),
                descriptor_hash: effect.descriptor_hash.clone(),
                descriptor: effect.descriptor.clone(),
                causal_input_id: prior
                    .first()
                    .map_or(self.ctx.parent.view().id, |s| s.causal_input_id),
                highest_started_attempt: EffectAttemptOrdinal::new(sink_attempt(
                    prior.len(),
                    started,
                )?),
                reason,
                control_events: controls,
            })
            .await
        } else {
            self.record_boundary_abort_with_control_events(
                effect.cursor.clone(),
                effect.descriptor_hash.clone(),
                effect.descriptor.clone(),
                reason,
                controls,
            )
            .await
        }
    }
}

pub(in crate::effects) struct SinkGroup<C: SinkRole<In>, In> {
    role: Arc<C>,
    boundary: Arc<dyn EffectBoundary>,
    cores: Vec<EffectsCore>,
    results: Vec<Result<(), HandlerError>>,
    guards: Vec<(super::super::history::EffectCursorGuard, EffectCursor)>,
    admitted: Vec<Pending<C::Request>>,
    marker: PhantomData<fn(In)>,
}

impl<C: SinkRole<In>, In> super::super::sink::private::Sealed for SinkGroup<C, In> {}

impl<C: SinkRole<In>, In> SinkGroup<C, In> {
    pub(in crate::effects) fn new(role: Arc<C>, boundary: Arc<dyn EffectBoundary>) -> Self {
        Self {
            role,
            boundary,
            cores: vec![],
            results: vec![],
            guards: vec![],
            admitted: vec![],
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<C: SinkRole<In>, In: TypedPayload + Send + Sync + 'static> OpenSinkBatch for SinkGroup<C, In> {
    fn admission_wait(&self) -> std::time::Duration {
        if Call::<C::Request>::SAFETY == EffectSafety::Transactional {
            return std::time::Duration::ZERO;
        }
        self.boundary.sink_admission_wait(C::Request::EFFECT_TYPE)
    }

    async fn admit(&mut self, mut ctx: EffectInvocationContext) -> Result<bool, HandlerError> {
        let index = self.cores.len();
        let first = self
            .cores
            .first()
            .map_or(ctx.input_seq.0, |core| core.ctx.input_seq.0);
        let input = In::try_from_event(ctx.parent.view())
            .map_err(|e| HandlerError::Deserialization(e.to_string()));
        ctx.effect_boundary = Some(self.boundary.clone());
        let mut core = EffectsCore::new(ctx);
        core.first_group_input = Some(first);
        let mut close = false;
        let mut history_error = None;
        let result = async {
            let request = self.role.prepare(&input?)?;
            let call = Call::new(request, core.project_named_effect::<Call<C::Request>>()?);
            let declaration = core.authorise_call(&call)?;
            let selected = core.select_call(&call, declaration.clone()).await;
            let live =
                match core.check_binding_result::<Call<C::Request>, _>(&declaration, selected) {
                    Ok(SelectedCall::Live(live)) => *live,
                    Ok(SelectedCall::Reproduced(_)) => return Ok(()),
                    Err(e) if e.is_replay_rejection() => return Ok(()),
                    Err(e @ (EffectError::Journal(_) | EffectError::ReplayArchive(_))) => {
                        history_error = Some(e);
                        return Ok(());
                    }
                    Err(e) => return Err(e.into()),
                };
            core.sink_outcomes = Some(Mutex::default());
            let mut permit = SinkPermit {
                closes_group: false,
                started: None,
                finished: None,
            };
            if live.effect.identity.safety != EffectSafety::Transactional
                && self.boundary.supports_sink_groups()
            {
                match self
                    .boundary
                    .admit_sink_request(&live.effect.identity, core.ctx.parent.view())
                    .await
                {
                    SinkAdmission::Admitted(admitted) => permit = admitted,
                    SinkAdmission::Refused(reason, controls) => {
                        self.guards.push((live.guard, live.effect.cursor.clone()));
                        return core
                            .sink_refused(&live.effect, &live.prior_attempts, 0, reason, controls)
                            .await
                            .map_err(Into::into);
                    }
                }
            }
            close = permit.closes_group;
            self.admitted.push(Pending {
                index,
                declaration,
                call,
                live,
                permit,
            });
            Ok(())
        }
        .await;
        if let Some(error) = history_error {
            return Err(error.into());
        }
        if let Some(fatal) = core.authoring_halt_fatal() {
            return Err(HandlerError::Fatal(fatal));
        }
        self.cores.push(core);
        self.results.push(result);
        Ok(close)
    }

    async fn finish(self: Box<Self>) -> Result<Vec<Result<(), HandlerError>>, HandlerError> {
        let Self {
            mut cores,
            mut results,
            mut guards,
            admitted,
            ..
        } = *self;
        if let Some(head) = admitted.first() {
            let core = &cores[head.index];
            let group = head.live.effect.binding_context.ports.group::<C::Request>();
            let safety = head.live.effect.identity.safety;
            if safety != EffectSafety::Transactional
                && core
                    .ctx
                    .effect_boundary
                    .as_ref()
                    .is_none_or(|b| b.supports_sink_groups())
            {
                let prestarted =
                    group.is_none() && safety == EffectSafety::NonIdempotentAtLeastOnce;
                if prestarted {
                    let starts = admitted
                        .iter()
                        .map(|p| {
                            AffineStart::new(
                                &cores[p.index],
                                &p.live.effect,
                                &p.live.prior_attempts,
                            )
                            .member(sink_attempt(p.live.prior_attempts.len(), 1)?)
                        })
                        .collect::<Result<_, _>>()?;
                    core.publish_sink_members("sink-starts:v1:", starts).await?;
                }
                let mut remaining = admitted.into_iter();
                while let Some(first) = remaining.next() {
                    let mut calls = vec![first];
                    if group.is_some() {
                        calls.extend(&mut remaining);
                    }
                    let mut physical =
                        shared_call(&cores, &mut calls, group.clone(), prestarted).await?;
                    for mut p in calls {
                        let core = &mut cores[p.index];
                        let mut controls = p
                            .permit
                            .finished
                            .take()
                            .map(|finish| finish(&physical.outcome))
                            .unwrap_or_default();
                        controls.append(&mut physical.controls);
                        let LiveCall {
                            effect,
                            prior_attempts,
                            guard,
                        } = p.live;
                        guards.push((guard, effect.cursor.clone()));
                        let result = match &physical.outcome {
                            EffectBoundaryOutcome::Aborted(reason) => {
                                core.sink_refused(
                                    &effect,
                                    &prior_attempts,
                                    physical.start,
                                    reason.clone(),
                                    controls,
                                )
                                .await
                            }
                            EffectBoundaryOutcome::Executed(outcome) => {
                                let reply = match outcome {
                                    Err(e) => Err(e.clone()),
                                    Ok(_) => Ok(physical.replies.next().ok_or_else(|| {
                                        HandlerError::ContractViolation("sink reply missing".into())
                                    })?),
                                };
                                let attempt = is_affine(effect.identity.safety)
                                    .then(|| {
                                        sink_attempt(prior_attempts.len(), physical.start)
                                            .map(EffectAttemptOrdinal::new)
                                    })
                                    .transpose()?;
                                core.sink_reply::<C::Request>(effect, reply, attempt, controls)
                                    .await
                            }
                        };
                        results[p.index] = core
                            .check_binding_result::<Call<C::Request>, _>(&p.declaration, result)
                            .map_err(Into::into);
                    }
                }
            } else {
                for p in admitted {
                    let core = &mut cores[p.index];
                    let LiveCall {
                        effect: live,
                        prior_attempts,
                        guard,
                    } = p.live;
                    guards.push((guard, live.cursor.clone()));
                    let result = core
                        .execute_live(p.call, live, prior_attempts)
                        .await
                        .map(drop);
                    results[p.index] = core
                        .check_binding_result::<Call<C::Request>, _>(&p.declaration, result)
                        .map_err(Into::into);
                }
            }
        }
        let mut members = Vec::new();
        let mut observed = Vec::new();
        for (index, core) in cores.iter_mut().enumerate() {
            if let Some(buffer) = core.sink_outcomes.take() {
                observed.push(index);
                let entries = buffer.into_inner().expect("sink outcome buffer");
                if entries.is_empty() {
                    return Err(results[index].clone().err().unwrap_or_else(|| {
                        HandlerError::ContractViolation("sink call lacks a terminal record".into())
                    }));
                }
                members.extend(entries.into_iter().map(|e| (e, core.ctx.parent.clone())));
            }
        }
        if let Some(core) = cores.first() {
            if let Err(e) = core
                .publish_sink_members(super::super::history::SINK_OUTCOME_PREFIX, members)
                .await
            {
                for (_, cursor) in &guards {
                    core.ctx
                        .stage
                        .runtime_execution
                        .effect_cursor_coordinator()
                        .poison(cursor.clone());
                }
                return Err(e.into());
            }
        }
        for index in observed {
            use crate::stages::observer::EffectObserverOutcome::{Failed, Succeeded};
            cores[index].observe_effect_outcome(
                C::Request::EFFECT_TYPE,
                if results[index].is_ok() {
                    Succeeded
                } else {
                    Failed
                },
            );
        }
        drop(guards);
        for (core, result) in cores.iter_mut().zip(results.iter_mut()) {
            if let Some(fatal) = core.authoring_halt_fatal() {
                *result = Err(HandlerError::Fatal(fatal));
                continue;
            }
            core.settle_without_action().await?;
            if let Some(fatal) = core.settlement_divergence().await {
                *result = Err(HandlerError::Fatal(fatal));
            }
        }
        Ok(results)
    }
}

struct SharedResult<T> {
    outcome: EffectBoundaryOutcome,
    replies: std::vec::IntoIter<T>,
    controls: Vec<ChainEvent>,
    start: u32,
}

struct PhysicalWrite<R: SinkRequest> {
    system: Option<Arc<dyn SinkGroupSystem<R>>>,
    reservations: Mutex<Vec<Box<dyn FnOnce() + Send + Sync>>>,
    requests: Vec<R>,
    ctx: EffectContext,
    starts: Vec<(u32, AffineStart)>,
    published: std::sync::atomic::AtomicU32,
    replies: Mutex<Option<Result<Vec<R::Outcome>, EffectError>>>,
}

impl<R: SinkRequest> PhysicalWrite<R> {
    async fn run(
        &self,
        ordinal: u32,
        lifecycle: super::super::boundary::PhysicalCallLifecycle,
    ) -> Result<Vec<ChainEvent>, EffectError> {
        self.replies.lock().expect("sink replies").take();
        if let Some((_, first)) = self.starts.first() {
            if self.published.load(Ordering::Acquire) != ordinal {
                let mut members = Vec::with_capacity(self.starts.len());
                for (prior, start) in &self.starts {
                    let attempt = prior.checked_add(ordinal).ok_or_else(|| {
                        EffectError::Execution("sink attempt ordinal exhausted".into())
                    })?;
                    members.push(start.member(attempt)?);
                }
                let id = format!("sink-starts:v1:{}", members[0].0.event.id);
                let committer = OutputCommitter {
                    data_journal: &first.data_journal,
                    flow_context: first.flow_context.as_ref(),
                    instrumentation: first.instrumentation.as_ref(),
                    heartbeat_state: first.heartbeat_state.as_ref(),
                    output_contract: None,
                    backpressure_writer: Some(&first.backpressure_writer),
                    observer_scope: MiddlewareExecutionScope::LiveEffectBoundary,
                };
                committer
                    .commit_group_with_parents(id, members)
                    .await
                    .map_err(|e| EffectError::Journal(e.to_string()))?;
                self.published.store(ordinal, Ordering::Release);
            }
        }
        for charge in self
            .reservations
            .lock()
            .expect("sink reservations")
            .drain(..)
        {
            charge();
        }
        lifecycle.mark_started();
        let result = match &self.system {
            Some(system) => system.write_group(&self.requests, &self.ctx).await,
            None => {
                let port = self.ctx.port(super::super::call::call_port_slot::<R>())?;
                self.requests[0]
                    .call(&port, &mut self.ctx.clone())
                    .await
                    .map(|reply| vec![reply])
            }
        };
        lifecycle.mark_completed(if result.is_ok() {
            PhysicalCallOutcome::Succeeded
        } else {
            PhysicalCallOutcome::Failed
        });
        let observed = result.as_ref().map(|_| Vec::new()).map_err(Clone::clone);
        *self.replies.lock().expect("sink replies") = Some(result);
        observed
    }
}

async fn shared_call<R: SinkRequest>(
    cores: &[EffectsCore],
    pending: &mut [Pending<R>],
    system: Option<Arc<dyn SinkGroupSystem<R>>>,
    prestarted: bool,
) -> Result<SharedResult<R::Outcome>, EffectError> {
    let reservations = Mutex::new(
        pending
            .iter_mut()
            .filter_map(|p| p.permit.started.take())
            .collect(),
    );
    let head = &pending[0];
    let first = &cores[head.index];
    let mut identity = head.live.effect.identity.clone();
    identity.additional = Some(
        pending
            .iter()
            .skip(1)
            .map(|p| p.live.effect.identity.first.clone())
            .collect(),
    );
    let mut starts = Vec::new();
    if is_affine(identity.safety) {
        for p in pending.iter() {
            starts.push((
                sink_attempt(p.live.prior_attempts.len(), 0)?,
                AffineStart::new(&cores[p.index], &p.live.effect, &p.live.prior_attempts),
            ));
        }
    }
    let write = Arc::new(PhysicalWrite {
        system,
        reservations,
        requests: pending.iter().map(|p| p.call.request.clone()).collect(),
        ctx: head.live.effect.binding_context.clone(),
        starts,
        published: u32::from(prestarted).into(),
        replies: Mutex::default(),
    });
    let (holds, progress) =
        first.effect_recovery(&identity.first.cursor, head.live.effect.committed_waits);
    let boundary = first.ctx.effect_boundary.as_ref();
    let parent = first.ctx.parent.view();
    let (outcome, controls) = if is_affine(identity.safety) {
        let physical = write.clone();
        let previous = write
            .starts
            .iter()
            .map(|(prior, _)| *prior)
            .max()
            .unwrap_or(0);
        let operation = AffineEffectOperation::new_with_lifecycle(
            previous,
            identity.safety,
            move |prior, lifecycle| {
                let physical = physical.clone();
                async move {
                    let attempt = prior
                        .checked_sub(previous)
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| {
                            EffectError::Execution("sink attempt ordinal exhausted".into())
                        })?;
                    physical.run(attempt, lifecycle).await
                }
            },
        )
        .with_recovery(holds, progress.clone());
        let provenance = operation.provenance();
        let report = match boundary {
            Some(b) => b.around_affine_effect(&identity, parent, operation).await,
            None => operation.execute().await.into_report(Vec::new()),
        };
        let (result, controls) = report.into_parts(&provenance)?;
        (
            match result {
                SingleUseEffectBoundaryOutcome::Executed(execution) => {
                    EffectBoundaryOutcome::Executed(execution.result().clone())
                }
                SingleUseEffectBoundaryOutcome::Aborted(reason) => {
                    EffectBoundaryOutcome::Aborted(reason)
                }
            },
            controls,
        )
    } else {
        let physical = write.clone();
        let mut operation = RepeatableEffectOperation::new_with_lifecycle(move |lifecycle| {
            let physical = physical.clone();
            async move { physical.run(0, lifecycle).await }
        })
        .with_input_holds(holds)
        .with_progress(progress.clone());
        let report = match boundary {
            Some(b) => {
                b.around_repeatable_effect(&identity, parent, operation)
                    .await
            }
            None => EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
                control_events: Vec::new(),
            },
        };
        (report.outcome, report.control_events)
    };
    if let Some(error) = progress.failure() {
        return Err(error);
    }
    let actual = write.replies.lock().expect("sink replies").take();
    let (outcome, replies) = match (outcome, actual) {
        (_, Some(Ok(replies))) if replies.len() == pending.len() => {
            (EffectBoundaryOutcome::Executed(Ok(Vec::new())), replies)
        }
        (_, Some(Err(e))) => (EffectBoundaryOutcome::Executed(Err(e)), Vec::new()),
        (EffectBoundaryOutcome::Aborted(reason), None) => {
            (EffectBoundaryOutcome::Aborted(reason), vec![])
        }
        (EffectBoundaryOutcome::Executed(Err(e)), None) => return Err(e),
        _ => {
            return Err(EffectError::Execution(
                "group system returned the wrong number of sink replies".into(),
            ))
        }
    };
    Ok(SharedResult {
        outcome,
        replies: replies.into_iter(),
        controls,
        start: write.published.load(Ordering::Acquire),
    })
}
