// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::common::idle_backoff;
use super::{BoxError, ContractEdgeStatus, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::id_conversions::StageIdExt;
use crate::messaging::SubscriptionPoller;
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_running(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // Poll for completion events from stages (system journal).
    let subscription = context
        .completion_subscription
        .as_mut()
        .ok_or("No subscription available - should have been initialized during materialization")?;

    // Use poll_next for non-blocking event polling.
    use crate::messaging::PollResult;
    match subscription.poll_next().await {
        PollResult::Event(envelope) => {
            let event = &envelope.event;

            // Track last system event ID for tail reconciliation.
            context.last_system_event_id_seen = Some(event.id);

            match &event.event {
                obzenflow_core::event::SystemEventType::StageLifecycle {
                    stage_id,
                    event: lifecycle_event,
                } => match lifecycle_event {
                    obzenflow_core::event::StageLifecycleEvent::Running => {
                        // Get stage name from topology.
                        let stage_info = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id());
                        let stage_name = stage_info
                            .map(|s| s.name.clone())
                            .unwrap_or_else(|| "unknown".to_string());
                        tracing::info!("Stage '{}' is now running", stage_name);
                        Ok(EventLoopDirective::Continue)
                    }
                    obzenflow_core::event::StageLifecycleEvent::Draining { metrics } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        let stage_info = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id());
                        let stage_name = stage_info
                            .map(|s| s.name.clone())
                            .unwrap_or_else(|| "unknown".to_string());
                        tracing::info!("Stage '{}' is draining", stage_name);
                        Ok(EventLoopDirective::Continue)
                    }
                    obzenflow_core::event::StageLifecycleEvent::Drained => {
                        let stage_info = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id());
                        let stage_name = stage_info
                            .map(|s| s.name.clone())
                            .unwrap_or_else(|| "unknown".to_string());
                        tracing::info!("Stage '{}' is drained", stage_name);
                        Ok(EventLoopDirective::Continue)
                    }
                    obzenflow_core::event::StageLifecycleEvent::Completed { metrics } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        // Stage has fully completed.
                        Ok(EventLoopDirective::Transition(
                            PipelineEvent::StageCompleted {
                                envelope: Box::new(envelope),
                            },
                        ))
                    }
                    obzenflow_core::event::StageLifecycleEvent::Cancelled { reason, metrics } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        let stage_info = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id());
                        let stage_name = stage_info
                            .map(|s| s.name.clone())
                            .unwrap_or_else(|| "unknown".to_string());

                        if context.stop_intent.requested {
                            tracing::info!(
                                stage_name = %stage_name,
                                reason = %reason,
                                "Stage cancelled"
                            );
                            Ok(EventLoopDirective::Continue)
                        } else {
                            Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                                message: format!("Stage '{stage_name}' cancelled: {reason}"),
                            }))
                        }
                    }
                    obzenflow_core::event::StageLifecycleEvent::Failed {
                        error, metrics, ..
                    } => {
                        if let Some(m) = metrics {
                            context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                        }
                        let stage_info = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id());
                        let stage_name = stage_info
                            .map(|s| s.name.clone())
                            .unwrap_or_else(|| "unknown".to_string());

                        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!("Stage '{stage_name}' failed: {error}"),
                        }))
                    }
                },
                obzenflow_core::event::SystemEventType::PipelineLifecycle(event) => {
                    if let obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted {
                        ..
                    } = event
                    {
                        tracing::info!("Received AllStagesCompleted event!");
                        if let Some(abort_directive) = supervisor.missing_contract_abort(context) {
                            return Ok(abort_directive);
                        }
                        Ok(EventLoopDirective::Transition(
                            PipelineEvent::AllStagesCompleted,
                        ))
                    } else {
                        Ok(EventLoopDirective::Continue)
                    }
                }
                obzenflow_core::event::SystemEventType::ContractStatus {
                    upstream,
                    reader,
                    pass,
                    reader_seq,
                    advertised_writer_seq,
                    reason,
                } => {
                    // A ContractStatus is used for two distinct purposes:
                    // 1) Mid-flight gating/abort (pass=false).
                    // 2) EOF-time shutdown gating for sources (pass=true/false in warn-only mode).
                    //
                    // Continuous contract evaluation can emit mid-flight evidence for observability
                    // (e.g. ContractResult heartbeats). Those MUST NOT be interpreted as
                    // "source completed; begin draining".
                    //
                    // The runtime only learns the upstream writer's final sequence at EOF (writer_seq),
                    // so `advertised_writer_seq.is_some()` is our best proxy for "final/EOF-time status".
                    let is_source = context.expected_sources.contains(upstream);
                    let is_final = advertised_writer_seq.is_some();

                    // Record per-edge contract status.
                    if *pass {
                        context.contract_pairs.insert(
                            (*upstream, *reader),
                            ContractEdgeStatus::passed(*reader_seq, *advertised_writer_seq),
                        );
                    } else {
                        context.contract_pairs.insert(
                            (*upstream, *reader),
                            ContractEdgeStatus::failed(
                                reason.clone(),
                                *reader_seq,
                                *advertised_writer_seq,
                            ),
                        );
                    }

                    if !pass {
                        let is_source = context.expected_sources.contains(upstream);
                        let mode = super::source_contract_mode();

                        let should_abort = super::is_gating_edge_for_contract(is_source, mode);

                        tracing::error!(
                            ?upstream,
                            ?reader,
                            ?reason,
                            is_source,
                            mode = ?mode,
                            "Contract status failure"
                        );

                        if should_abort {
                            return Ok(EventLoopDirective::Transition(PipelineEvent::Abort {
                                reason: reason.clone().unwrap_or_else(|| {
                                    obzenflow_core::event::types::ViolationCause::Other(
                                        "contract_failed".into(),
                                    )
                                }),
                                upstream: Some(*upstream),
                            }));
                        } else {
                            // Warn-only for source contracts: treat as
                            // "completed" for shutdown gating but do not abort.
                            if is_source && is_final {
                                context.contract_status.insert(*upstream, true);
                            }
                            return Ok(EventLoopDirective::Continue);
                        }
                    }

                    // Mark upstream source as having passed its contract (EOF-time only).
                    if !(is_source && is_final) {
                        return Ok(EventLoopDirective::Continue);
                    }

                    context.contract_status.insert(*upstream, true);
                    let expected = &context.expected_sources;
                    let all_pass = !expected.is_empty()
                        && expected
                            .iter()
                            .all(|src| context.contract_status.get(src).copied().unwrap_or(false));

                    if all_pass {
                        tracing::info!("All source contracts passed; initiating drain");
                        Ok(EventLoopDirective::Transition(PipelineEvent::Shutdown))
                    } else {
                        Ok(EventLoopDirective::Continue)
                    }
                }
                _ => Ok(EventLoopDirective::Continue),
            }
        }
        PollResult::NoEvents => {
            // No events available right now: sleep briefly to avoid busy loop.
            idle_backoff().await;
            Ok(EventLoopDirective::Continue)
        }
        PollResult::Error(e) => {
            tracing::error!("Error polling system journal: {}", e);
            Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                message: format!("System journal error: {e}"),
            }))
        }
    }
}
