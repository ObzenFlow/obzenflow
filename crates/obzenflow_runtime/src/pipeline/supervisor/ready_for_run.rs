// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::bootstrap::startup_mode_manual;
use crate::id_conversions::StageIdExt;
use crate::messaging::{PollResult, SubscriptionPoller};
use crate::supervised_base::EventLoopDirective;
use std::time::Duration;

pub(super) async fn dispatch_ready_for_run(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    let subscription = context
        .completion_subscription
        .as_mut()
        .ok_or("No subscription available - should have been initialized during materialization")?;

    match subscription.poll_next().await {
        PollResult::Event(envelope) => {
            let event = &envelope.event;
            context.last_system_event_id_seen = Some(event.id);

            if let obzenflow_core::event::SystemEventType::StageLifecycle {
                stage_id,
                event: lifecycle_event,
            } = &event.event
            {
                let stage_info = context
                    .topology
                    .stages()
                    .find(|s| s.id == stage_id.to_topology_id());
                let stage_name = stage_info
                    .map(|s| s.name.clone())
                    .unwrap_or_else(|| "unknown".to_string());

                match lifecycle_event {
                    obzenflow_core::event::StageLifecycleEvent::Failed {
                        error, metrics, ..
                    } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!(
                                "Stage '{stage_name}' failed while ready for run: {error}"
                            ),
                        }));
                    }
                    obzenflow_core::event::StageLifecycleEvent::Cancelled { reason, metrics } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!(
                                "Stage '{stage_name}' cancelled while ready for run: {reason}"
                            ),
                        }));
                    }
                    _ => {}
                }
            }
        }
        PollResult::NoEvents => {}
        PollResult::Error(e) => {
            tracing::error!("Error polling system journal in ReadyForRun: {}", e);
            return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                message: format!("System journal error: {e}"),
            }));
        }
    }

    if startup_mode_manual() {
        if supervisor.should_log_manual_wait() {
            tracing::info!(
                "Pipeline is ready for Run (startup_mode=manual); waiting for external Run"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(EventLoopDirective::Continue)
    } else {
        tracing::info!("Pipeline is ready for Run (startup_mode=auto); starting pipeline");
        Ok(EventLoopDirective::Transition(PipelineEvent::Run))
    }
}
