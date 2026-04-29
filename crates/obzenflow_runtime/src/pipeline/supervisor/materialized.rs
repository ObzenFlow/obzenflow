// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::id_conversions::StageIdExt;
use crate::messaging::SubscriptionPoller;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::StageId;

/// Drives the "built but not ready" phase of the pipeline lifecycle.
///
/// `Materialized` means stage resources exist and non-source stages have been
/// asked to start. It does not mean source stages may run yet. This dispatcher
/// waits until every non-source stage has reported `Running`, then emits
/// `StageReadinessComplete` so the FSM can enter `ReadyForRun`.
pub(super) async fn dispatch_materialized(
    _supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // First check if any stages are already running (they might have published before we subscribed).
    let supervisors = &context.stage_supervisors;
    for (stage_id, stage) in supervisors.iter() {
        if stage.is_ready()
            && !context
                .topology
                .upstream_stages(stage_id.to_topology_id())
                .is_empty()
        {
            // This is a non-source stage that's already running.
            if context.running_stages.insert(*stage_id) {
                tracing::info!("Stage '{}' was already running", stage.stage_name());
            }
        }
    }

    let topology = &context.topology;
    let non_source_stages: std::collections::HashSet<_> = topology
        .stages()
        .filter(|stage_info| !topology.upstream_stages(stage_info.id).is_empty())
        .map(|stage_info| StageId::from_topology_id(stage_info.id))
        .collect();

    if non_source_stages.is_empty() {
        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
            message: "source-only topologies are unsupported by the readiness barrier".to_string(),
        }));
    }

    // Poll for stage running events to know when all non-source stages are ready.
    let subscription = context
        .completion_subscription
        .as_mut()
        .ok_or("No subscription available - should have been initialized during materialization")?;

    // Check for stage running events.
    use crate::messaging::PollResult;
    match subscription.poll_next().await {
        PollResult::Event(envelope) => {
            // Process stage running event.
            let event = &envelope.event;
            // Track last system event ID for tail reconciliation.
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
                    obzenflow_core::event::StageLifecycleEvent::Running => {
                        // Track this stage as running.
                        context.running_stages.insert(*stage_id);

                        // Log based on stage type.
                        if context
                            .topology
                            .upstream_stages(stage_id.to_topology_id())
                            .is_empty()
                        {
                            tracing::debug!(
                                "Source stage '{}' is running (waiting for pipeline signal)",
                                stage_name
                            );
                        } else {
                            tracing::info!("Stage '{}' is now running", stage_name);
                        }
                    }
                    obzenflow_core::event::StageLifecycleEvent::Failed {
                        error, metrics, ..
                    } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!(
                                "Stage '{stage_name}' failed before readiness: {error}"
                            ),
                        }));
                    }
                    obzenflow_core::event::StageLifecycleEvent::Cancelled { reason, metrics } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!(
                                "Stage '{stage_name}' cancelled before readiness: {reason}"
                            ),
                        }));
                    }
                    _ => {}
                }
            }
        }
        PollResult::NoEvents => {
            // No new events, but that's OK: stages might already be running.
            // Add a brief sleep to avoid busy loop.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        PollResult::Error(e) => {
            tracing::error!("Error polling system journal in Awaiting: {}", e);
            return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                message: format!("System journal error: {e}"),
            }));
        }
    }

    // Check if all non-source stages are running.
    let running_stages = &context.running_stages;
    // Check if all non-source stages are in the running set.
    let all_ready = non_source_stages
        .iter()
        .all(|stage_id| running_stages.contains(stage_id));

    if all_ready {
        tracing::info!(
            "All {} non-source stages are running; pipeline is ready for Run",
            non_source_stages.len()
        );
        Ok(EventLoopDirective::Transition(
            PipelineEvent::StageReadinessComplete,
        ))
    } else {
        // Still waiting for stages to report running.
        let waiting_for = non_source_stages.difference(running_stages).count();
        if waiting_for > 0 {
            tracing::debug!(
                "Waiting for {} more stage(s) to report running ({}/{} ready)",
                waiting_for,
                running_stages.len(),
                non_source_stages.len()
            );
        }
        Ok(EventLoopDirective::Continue)
    }
}
