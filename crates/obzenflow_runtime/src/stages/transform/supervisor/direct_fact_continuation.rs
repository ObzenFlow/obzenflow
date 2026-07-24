// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Supervisor-owned parked continuation for descriptor-bounded generated
//! direct effects.

use super::TransformSupervisor;
use crate::backpressure::DirectFactAdmission;
use crate::effects::EffectInvocationContext;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{
    acquire_direct_fact_lease, drain_one_pending, DrainOutcome, PendingOutput,
};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::output_committer::{
    commit_framework_observability_events, is_framework_middleware_observability_event,
    FrameworkObservabilityCommit,
};
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::observer::dispatch::{
    run_after_handler_observers, run_before_handler_observers,
};
use crate::stages::transform::fsm::{
    DirectFactContinuation, DirectFactPollState, TransformContext, TransformEvent,
};
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, EventEnvelope, MiddlewareExecutionScope, StageId};
use std::collections::VecDeque;
use std::task::Poll;

fn direct_fatal(
    code: StageFatalCode,
    reason: StageFatalReason,
    detail: impl Into<String>,
) -> StageFatal {
    StageFatal::new(code, reason, detail)
}

/// Install a frozen continuation when this exact physical input is named by
/// the descriptor's generated direct-fact plan.
pub(super) async fn start_if_eligible<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut TransformContext<H>,
    envelope: &EventEnvelope<ChainEvent>,
    upstream_stage: Option<StageId>,
    input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
    scope: MiddlewareExecutionScope,
    flow_context: &FlowContext,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    if matches!(
        envelope.event.processing_info.status,
        ProcessingStatus::Error { .. }
    ) {
        return Ok(false);
    }
    let Some(bound) = ctx.direct_fact_plan.bound_for(&envelope.event.event_type()) else {
        return Ok(false);
    };
    if ctx.direct_fact_continuation.is_some() {
        return Err("generated direct-fact continuation slot is already occupied".into());
    }
    let input_position = input_position.ok_or_else(|| {
        "generated direct-fact input has no deterministic stage input position".to_string()
    })?;
    let writer_id = ctx
        .writer_id
        .ok_or_else(|| "generated direct-fact input has no stage writer id".to_string())?;

    run_before_handler_observers(
        &ctx.observers,
        ctx.stage_id,
        &ctx.stage_name,
        flow_context,
        scope,
        &envelope.event,
        Some(input_position.0),
        &ctx.data_journal,
        &ctx.instrumentation,
        envelope,
    )
    .await?;

    let admission = DirectFactAdmission::new(envelope.event.event_type().into(), bound);
    let effect_writer = ctx
        .backpressure_writer
        .clone()
        .with_direct_fact_admission(admission.clone());
    let handler = ctx.handler.clone();
    let handler_heartbeat_state = ctx
        .heartbeat
        .as_ref()
        .map(|heartbeat| heartbeat.state.clone());
    let instrumentation = ctx.instrumentation.clone();
    let future_envelope = envelope.clone();
    let error_envelope = envelope.clone();
    let event = envelope.event.clone();
    let event_id = event.id;
    let effect_context = EffectInvocationContext {
        flow_id: ctx.flow_id,
        stage_id: ctx.stage_id,
        stage_key: ctx.stage_name.clone(),
        writer_id,
        input_seq: input_position,
        lineage: ctx.lineage_policy,
        stage_logic_version: handler.stage_logic_version().to_string(),
        data_journal: ctx.data_journal.clone(),
        flow_context: Some(flow_context.clone()),
        observers: Some(ctx.observers.clone()),
        system_journal: Some(ctx.system_journal.clone()),
        instrumentation: Some(ctx.instrumentation.clone()),
        heartbeat_state: handler_heartbeat_state.clone(),
        parent: future_envelope,
        effect_history: ctx.effect_history.clone(),
        runtime_execution: ctx.runtime_execution.clone(),
        effect_ports: ctx.effect_ports.clone(),
        effect_declarations: ctx.effect_declarations.clone(),
        output_contract: ctx.output_contract.clone(),
        backpressure_writer: effect_writer,
        emit_enabled: true,
        effect_boundary: None,
    };
    let instrumentation_for_future = instrumentation.clone();
    let future = Box::pin(async move {
        process_with_instrumentation(&instrumentation_for_future, || async move {
            let _processing = handler_heartbeat_state.as_ref().map(|state| {
                HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id)
            });
            match handler.process(event, Some(effect_context), scope).await {
                Ok(outputs) => {
                    if let Some(state) = &handler_heartbeat_state {
                        state.record_last_consumed(event_id);
                    }
                    Ok(outputs)
                }
                Err(error) if error.is_fatal() => {
                    Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(error) => {
                    let reason = format!("Transform handler error: {error:?}");
                    let error_event = error_envelope
                        .event
                        .clone()
                        .mark_as_error(reason, error.kind());
                    if let Some(state) = &handler_heartbeat_state {
                        state.record_last_consumed(event_id);
                    }
                    Ok(vec![error_event])
                }
            }
        })
        .await
    });

    let poll_state = if scope == MiddlewareExecutionScope::LiveHandler {
        DirectFactPollState::FreshUnpolled
    } else {
        DirectFactPollState::DrivingReconstruction
    };
    ctx.direct_fact_continuation = Some(DirectFactContinuation::new(
        envelope.clone(),
        upstream_stage,
        Some(input_position),
        scope,
        admission,
        poll_state,
        future,
    ));
    Ok(true)
}

async fn close_and_track<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    continuation: &DirectFactContinuation,
) -> Result<(), String> {
    let committed = continuation.admission.close()?;
    if let Some(subscription) = sup.subscription.as_mut() {
        for _ in 0..committed {
            subscription.track_output_event();
        }
    }
    Ok(())
}

fn acknowledge<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    ctx: &TransformContext<H>,
    upstream: Option<StageId>,
) {
    let Some(upstream) = upstream else {
        return;
    };
    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
        reader.ack_consumed(1);
    } else {
        tracing::warn!(
            stage_name = %ctx.stage_name,
            upstream = ?upstream,
            "Generated continuation acknowledgement skipped: missing reader handle"
        );
    }
}

async fn fail<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    sup: &mut TransformSupervisor<H>,
    ctx: &mut TransformContext<H>,
    continuation: DirectFactContinuation,
    fatal: StageFatal,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    close_and_track(sup, &continuation).await?;
    let writer_id = ctx
        .writer_id
        .ok_or_else(|| "fatal generated input has no stage writer id".to_string())?;
    record_stage_fatal(
        &fatal,
        StageFatalCommit {
            error_journal: &ctx.error_journal,
            writer_id,
            stage_id: ctx.stage_id,
            stage_key: &ctx.stage_name,
            input_position: continuation.input_position,
            parent: Some(&continuation.envelope),
            lineage: ctx.lineage_policy,
        },
    )
    .await?;
    acknowledge(ctx, continuation.upstream_stage);
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat
            .state
            .record_last_consumed(continuation.envelope.event.id);
    }
    Ok(EventLoopDirective::Transition(TransformEvent::Error(
        fatal.detail,
    )))
}

async fn finish_success<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    ctx: &mut TransformContext<H>,
    continuation: DirectFactContinuation,
    flow_context: &FlowContext,
    mut transformed_events: Vec<ChainEvent>,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Err(error) = run_after_handler_observers(
        &ctx.observers,
        ctx.stage_id,
        &ctx.stage_name,
        flow_context,
        continuation.scope,
        &continuation.envelope.event,
        continuation.input_position.map(|position| position.0),
        transformed_events.as_mut_slice(),
        &ctx.data_journal,
        &ctx.instrumentation,
        &continuation.envelope,
    )
    .await
    {
        return fail(
            sup,
            ctx,
            continuation,
            direct_fatal(
                StageFatalCode::Coordination,
                StageFatalReason::CoordinationFailure,
                format!("generated after-handler observer failed: {error}"),
            ),
        )
        .await;
    }

    let mut pending = VecDeque::<PendingOutput>::new();
    for event in transformed_events {
        if event.is_data() {
            return fail(
                sup,
                ctx,
                continuation,
                direct_fatal(
                    StageFatalCode::Protocol,
                    StageFatalReason::ProtocolInputIntegrity,
                    format!(
                        "generated direct handler returned Data '{}' outside its affine lease",
                        event.event_type()
                    ),
                ),
            )
            .await;
        }
        if is_framework_middleware_observability_event(&event) {
            if let Err(error) = commit_framework_observability_events(
                vec![event],
                FrameworkObservabilityCommit {
                    flow_context,
                    data_journal: &ctx.data_journal,
                    system_journal: Some(&ctx.system_journal),
                    instrumentation: Some(&ctx.instrumentation),
                    heartbeat_state: ctx.heartbeat.as_ref().map(|heartbeat| &heartbeat.state),
                    backpressure_writer: &ctx.backpressure_writer,
                    parent: Some(&continuation.envelope),
                    observer_scope: continuation.scope,
                },
            )
            .await
            {
                return fail(
                    sup,
                    ctx,
                    continuation,
                    direct_fatal(
                        StageFatalCode::Journal,
                        StageFatalReason::JournalFailure,
                        format!("generated observability commit failed: {error}"),
                    ),
                )
                .await;
            }
            continue;
        }
        if let ProcessingStatus::Error { kind, .. } = &event.processing_info.status {
            ctx.instrumentation
                .record_error(kind.clone().unwrap_or(ErrorKind::Unknown));
        }
        if route_to_error_journal(&event) {
            if let Err(error) = ctx
                .error_journal
                .append(event, Some(&continuation.envelope))
                .await
            {
                return fail(
                    sup,
                    ctx,
                    continuation,
                    direct_fatal(
                        StageFatalCode::Journal,
                        StageFatalReason::JournalFailure,
                        format!("generated error-journal append failed: {error}"),
                    ),
                )
                .await;
            }
        } else {
            pending.push_back(PendingOutput {
                event,
                scope: continuation.scope,
            });
        }
    }

    while let Some(output) = pending.pop_front() {
        match drain_one_pending(
            output,
            flow_context,
            sup.stage_id,
            ctx.heartbeat
                .as_ref()
                .map(|heartbeat| heartbeat.state.clone()),
            &ctx.data_journal,
            &ctx.system_journal,
            Some(&continuation.envelope),
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_stall,
            Some(&ctx.output_contract),
            Some(&ctx.observers),
            &mut pending,
        )
        .await
        {
            Ok(DrainOutcome::Committed { was_data: false }) => {}
            Ok(DrainOutcome::Committed { was_data: true }) => {
                unreachable!("generated returned Data was rejected before the pending drain")
            }
            Ok(DrainOutcome::BackedOff) => {
                unreachable!("non-Data generated output cannot be blocked by backpressure")
            }
            Err(error) => {
                return fail(
                    sup,
                    ctx,
                    continuation,
                    direct_fatal(
                        StageFatalCode::Journal,
                        StageFatalReason::JournalFailure,
                        format!("generated non-Data output commit failed: {error}"),
                    ),
                )
                .await
            }
        }
    }

    close_and_track(sup, &continuation).await?;
    acknowledge(ctx, continuation.upstream_stage);
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat
            .state
            .record_last_consumed(continuation.envelope.event.id);
    }
    Ok(EventLoopDirective::Continue)
}

/// Drive the one occupied generated slot. Both Running and Draining call this
/// before polling another subscription event.
pub(super) async fn service<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    ctx: &mut TransformContext<H>,
    flow_context: &FlowContext,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut continuation = ctx
        .direct_fact_continuation
        .take()
        .expect("service is called only for an occupied generated continuation");
    if continuation.admission.event_type().as_str() != continuation.envelope.event.event_type() {
        let observed = continuation.envelope.event.event_type();
        let expected = continuation.admission.event_type().as_str().to_string();
        return fail(
            sup,
            ctx,
            continuation,
            direct_fatal(
                StageFatalCode::Configuration,
                StageFatalReason::ConfigurationInvariant,
                format!(
                    "generated direct-fact plan/input mismatch: expected '{expected}', observed '{observed}'"
                ),
            ),
        )
        .await;
    }

    loop {
        match continuation.poll_state {
            DirectFactPollState::FreshUnpolled | DirectFactPollState::ResumeAtLiveBarrier => {
                let acquisition = acquire_direct_fact_lease(
                    &ctx.backpressure_writer,
                    continuation.admission.bound(),
                    ctx.stage_id,
                    flow_context,
                    &ctx.data_journal,
                    &ctx.instrumentation,
                    &mut ctx.backpressure_pulse,
                    &mut ctx.backpressure_stall,
                )
                .await;
                let lease = match acquisition {
                    Ok(Some(lease)) => lease,
                    Ok(None) => {
                        ctx.direct_fact_continuation = Some(continuation);
                        return Ok(EventLoopDirective::Continue);
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let (code, reason) = if message.contains("OBZENFLOW_BACKPRESSURE_DISABLED")
                        {
                            (
                                StageFatalCode::Configuration,
                                StageFatalReason::ConfigurationInvariant,
                            )
                        } else if message.contains("backpressure.stalled") {
                            (
                                StageFatalCode::Resource,
                                StageFatalReason::ResourceExhaustion,
                            )
                        } else {
                            (
                                StageFatalCode::Coordination,
                                StageFatalReason::CoordinationFailure,
                            )
                        };
                        return fail(sup, ctx, continuation, direct_fatal(code, reason, message))
                            .await;
                    }
                };
                if let Err(message) = continuation.admission.grant(lease) {
                    return fail(
                        sup,
                        ctx,
                        continuation,
                        direct_fatal(
                            StageFatalCode::Coordination,
                            StageFatalReason::CoordinationFailure,
                            message,
                        ),
                    )
                    .await;
                }
                continuation.poll_state = DirectFactPollState::LiveLeased;
            }
            DirectFactPollState::DrivingReconstruction | DirectFactPollState::LiveLeased => {
                match continuation.poll_once().await {
                    Poll::Ready(Ok(events)) => {
                        return finish_success(sup, ctx, continuation, flow_context, events).await;
                    }
                    Poll::Ready(Err(error)) => {
                        let fatal = error
                            .downcast_ref::<HandlerError>()
                            .and_then(HandlerError::as_fatal)
                            .cloned()
                            .unwrap_or_else(|| {
                                direct_fatal(
                                    StageFatalCode::Coordination,
                                    StageFatalReason::CoordinationFailure,
                                    format!("generated handler future failed: {error}"),
                                )
                            });
                        return fail(sup, ctx, continuation, fatal).await;
                    }
                    Poll::Pending => {
                        if continuation.admission.is_requested() {
                            continuation.poll_state = DirectFactPollState::ResumeAtLiveBarrier;
                            continue;
                        }
                        continuation.wait_one_ready_chunk().await;
                        ctx.direct_fact_continuation = Some(continuation);
                        return Ok(EventLoopDirective::Continue);
                    }
                }
            }
        }
    }
}
