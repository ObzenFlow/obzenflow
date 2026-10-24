// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::{PollResult, UpstreamSubscription};
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::StateVariant;
use std::time::Duration;

use super::TransformSupervisor;
use crate::stages::transform::fsm::{TransformEvent, TransformResources, TransformState};

pub(super) async fn dispatch_closing_cycle<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    state: &TransformState<H>,
    ctx: &mut TransformResources<H>,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }

    if sup.subscription.is_none() {
        sup.subscription = ctx.subscription.take();
    }
    let Some(subscription) = sup.subscription.as_mut() else {
        return Ok(EventLoopDirective::Transition(TransformEvent::CycleClosed));
    };

    let poll_result = subscription
        .poll_next_with_state(state.variant_name(), Some(&mut ctx.contract_state[..]))
        .await;

    match poll_result {
        PollResult::Event(envelope) => {
            let upstream = subscription
                .last_delivered_upstream_stage()
                .expect("delivered event must identify its upstream stage");
            ctx.instrumentation.record_consumed(&envelope, upstream);
            if envelope.consumes_data_credit() {
                return Ok(EventLoopDirective::Transition(TransformEvent::Error(
                    row_after_cycle_closed(&ctx.stage_name, subscription, upstream),
                )));
            }
            if envelope.is_eof() {
                subscription
                    .check_contracts(&mut ctx.contract_state[..])
                    .await
                    .into_result()?;
            }
            Ok(EventLoopDirective::Continue)
        }
        PollResult::CursorAdvanced {
            upstream,
            completed_data_rows,
        } => {
            if completed_data_rows > 0 {
                return Ok(EventLoopDirective::Transition(TransformEvent::Error(
                    row_after_cycle_closed(&ctx.stage_name, subscription, upstream),
                )));
            }
            Ok(EventLoopDirective::Continue)
        }
        PollResult::NoEvents => {
            if subscription.all_readers_eof() {
                subscription
                    .check_contracts(&mut ctx.contract_state[..])
                    .await
                    .into_result()?;
                return Ok(EventLoopDirective::Transition(TransformEvent::CycleClosed));
            }

            if let Some(status) = subscription
                .maybe_check_contracts_tick(
                    &mut ctx.contract_state[..],
                    &mut ctx.last_contract_check,
                )
                .await
            {
                status.into_result()?;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
        PollResult::Error(e) => Ok(EventLoopDirective::Transition(TransformEvent::Error(
            format!("Drain error: {e}"),
        ))),
    }
}

fn row_after_cycle_closed(
    stage_name: &str,
    subscription: &UpstreamSubscription<ChainEvent>,
    upstream: StageId,
) -> String {
    let upstream = subscription
        .upstream_key(upstream)
        .map_or_else(|| upstream.to_string(), ToString::to_string);
    format!("transform '{stage_name}' read a row from '{upstream}' after its cycle closed")
}
