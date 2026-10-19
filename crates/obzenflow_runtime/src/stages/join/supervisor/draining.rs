// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::DeliveredRecord;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::supervision::backpressure_drain::PendingOutput;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::ChainPayload;
use std::sync::atomic::Ordering;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::fsm::{DrainStep, JoinDrain, JoinEvent, JoinResources};

type DrainDirective =
    Result<EventLoopDirective<JoinEvent>, Box<dyn std::error::Error + Send + Sync>>;

pub(super) async fn dispatch_draining<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    drain: &JoinDrain,
    ctx: &mut JoinResources<H>,
) -> DrainDirective {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }
    common::ensure_subscriptions(sup, ctx);
    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    match &drain.step {
        DrainStep::CheckInputs { .. } => {
            if let Some(subscription) = sup.reference_subscription.as_mut() {
                subscription
                    .check_contracts(&mut ctx.reference_contract_state[..])
                    .await
                    .into_result()?;
            }
            if let Some(subscription) = sup.stream_subscription.as_mut() {
                subscription
                    .check_contracts(&mut ctx.stream_contract_state[..])
                    .await
                    .into_result()?;
            }
            Ok(EventLoopDirective::Transition(JoinEvent::DrainStepEnded))
        }
        DrainStep::Finalize { final_eof } => {
            if let FlushOutcome::Blocked = common::flush_pending_outputs(sup, ctx).await? {
                return Ok(EventLoopDirective::Continue);
            }
            finalize(ctx, drain.worst_eof, final_eof).await
        }
        DrainStep::Flush => {
            if let FlushOutcome::Blocked = common::flush_pending_outputs(sup, ctx).await? {
                return Ok(EventLoopDirective::Continue);
            }
            Ok(EventLoopDirective::Transition(JoinEvent::DrainComplete))
        }
    }
}

async fn finalize<H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    ctx: &mut JoinResources<H>,
    worst_eof: Option<EofKind>,
    final_eof: &DeliveredRecord<ChainPayload>,
) -> DrainDirective {
    match worst_eof.unwrap_or(EofKind::Natural) {
        EofKind::Natural | EofKind::Poison => {}
        EofKind::Truncated => {
            tracing::info!(
                stage_name = %ctx.stage_name,
                "terminal EOF is truncated; skipping end-of-input finalization"
            );
            return Ok(EventLoopDirective::Transition(JoinEvent::DrainStepEnded));
        }
    }

    let stream_source_id = ctx
        .stream_subscription_factory
        .upstream_stage_ids()
        .into_iter()
        .next()
        .ok_or_else(|| {
            obzenflow_fsm::FsmError::HandlerError(
                "No stream subscription source available".to_string(),
            )
        })?;
    let writer_id = ctx.writer_id.ok_or_else(|| {
        obzenflow_fsm::FsmError::HandlerError("No writer ID available".to_string())
    })?;
    let handler = ctx
        .handler
        .as_ref()
        .expect("handler available before cleanup")
        .clone();
    let empty_state = handler.initial_state();
    let mut final_state = std::mem::replace(&mut ctx.handler_state, empty_state);

    let mut events = match handler.on_stream_eof(
        &mut final_state,
        final_eof.authored(),
        stream_source_id,
        writer_id,
    ) {
        Ok(events) => events,
        Err(err) => {
            ctx.handler_state = final_state;
            ctx.instrumentation.record_error(err.kind());
            let detail = if let Some(fatal) = err.as_fatal() {
                common::record_join_stage_fatal(ctx, fatal, Some(final_eof), None).await?;
                fatal.detail.clone()
            } else {
                err.to_string()
            };
            return Ok(EventLoopDirective::Transition(JoinEvent::Error(detail)));
        }
    };
    let parent = final_eof.authored();
    match handler.drain(&final_state, Some(&parent)).await {
        Ok(drained) => events.extend(drained),
        Err(err) => {
            ctx.handler_state = final_state;
            ctx.instrumentation.record_error(err.kind());
            let detail = if let Some(fatal) = err.as_fatal() {
                common::record_join_stage_fatal(ctx, fatal, Some(final_eof), None).await?;
                fatal.detail.clone()
            } else {
                err.to_string()
            };
            return Ok(EventLoopDirective::Transition(JoinEvent::Error(detail)));
        }
    }
    ctx.handler_state = final_state;

    let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
    ctx.pending_outputs
        .extend(events.into_iter().map(|event| PendingOutput {
            causal: crate::supervised_base::publication::capture(),
            event,
            scope,
        }));
    if !ctx.pending_outputs.is_empty() {
        ctx.pending_parent = Some(final_eof.clone());
    }
    Ok(EventLoopDirective::Transition(JoinEvent::DrainStepEnded))
}
