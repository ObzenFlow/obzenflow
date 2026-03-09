// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::common::idle_backoff;
use super::{
    BoxError, ContractEdgeStatus, FlowStopMode, PipelineContext, PipelineEvent, PipelineSupervisor,
    SourceContractStrictMode, DRAIN_LIVENESS_MAX_IDLE,
};
use crate::id_conversions::StageIdExt;
use crate::messaging::SubscriptionPoller;
use crate::stages::common::stage_handle::STOP_REASON_TIMEOUT;
use crate::supervised_base::EventLoopDirective;
use std::time::Instant;

pub(super) async fn dispatch_draining(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // If Stop initiated this drain, enforce a bounded timeout so the
    // pipeline terminates deterministically even if some stage never
    // reports completion.
    if let Some(deadline) = context.stop_intent.deadline {
        if Instant::now() >= deadline {
            tracing::warn!(
                pipeline = %supervisor.name,
                "Graceful stop timeout expired; escalating to cancel"
            );
            context
                .stop_intent
                .apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.to_string()));
            return Ok(EventLoopDirective::Transition(
                PipelineEvent::StopRequested {
                    mode: FlowStopMode::Cancel,
                    reason: Some(STOP_REASON_TIMEOUT.to_string()),
                },
            ));
        }
    }

    // Continue polling for completion events during drain.
    let subscription = context
        .completion_subscription
        .as_mut()
        .ok_or("No subscription available")?;

    use crate::messaging::PollResult;
    match subscription.poll_next().await {
        PollResult::Event(envelope) => {
            let event = &envelope.event;

            // Track last system event ID for tail reconciliation.
            context.last_system_event_id_seen = Some(event.id);

            match &event.event {
                obzenflow_core::event::SystemEventType::StageLifecycle {
                    stage_id,
                    event: obzenflow_core::event::StageLifecycleEvent::Completed { metrics },
                } => {
                    if let Some(m) = metrics {
                        context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                    }
                    // Process stage completion immediately.
                    Ok(EventLoopDirective::Transition(
                        PipelineEvent::StageCompleted {
                            envelope: Box::new(envelope),
                        },
                    ))
                }
                obzenflow_core::event::SystemEventType::StageLifecycle {
                    stage_id,
                    event: obzenflow_core::event::StageLifecycleEvent::Failed { error, metrics, .. },
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
                obzenflow_core::event::SystemEventType::ContractStatus {
                    upstream,
                    reader,
                    pass,
                    reader_seq,
                    advertised_writer_seq,
                    reason,
                } => {
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
                        let is_source = context.expected_sources.contains(&upstream.clone());
                        let mode = super::source_contract_mode();

                        let should_abort =
                            !is_source || matches!(mode, SourceContractStrictMode::Abort);

                        tracing::error!(
                            ?upstream,
                            ?reader,
                            ?reason,
                            is_source,
                            mode = ?mode,
                            "Contract status failure during drain"
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
                            // Warn-only for source contracts during drain:
                            // mark as completed for barrier gating but do not abort.
                            context.contract_status.insert(*upstream, true);
                            return Ok(EventLoopDirective::Continue);
                        }
                    } else {
                        context.contract_status.insert(*upstream, true);
                    }
                    Ok(EventLoopDirective::Continue)
                }
                obzenflow_core::event::SystemEventType::PipelineLifecycle(
                    obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted { .. },
                ) => {
                    tracing::info!("All stages have completed - transitioning to drained");
                    if let Some(abort_directive) = supervisor.missing_contract_abort(context) {
                        return Ok(abort_directive);
                    }
                    Ok(EventLoopDirective::Transition(
                        PipelineEvent::AllStagesCompleted,
                    ))
                }
                _ => {
                    // Log other events.
                    tracing::debug!("Received event during drain: {:?}", event.event);
                    Ok(EventLoopDirective::Continue)
                }
            }
        }
        PollResult::NoEvents => {
            // No events available right now: sleep briefly to avoid busy loop.
            idle_backoff().await;
            if supervisor.should_log_barrier() {
                let snapshot = supervisor.barrier_snapshot(context);
                tracing::debug!(
                    pending_stages = ?snapshot.pending_stages,
                    missing_contracts = ?snapshot.missing_contracts,
                    completed_stages = snapshot.completed,
                    total_stages = snapshot.total,
                    satisfied_contracts = snapshot.satisfied_contracts,
                    total_contracts = snapshot.total_contracts,
                    "Drain barrier snapshot (no events)"
                );
            }
            if supervisor.all_stages_and_contracts_complete(context) {
                // Synthesize AllStagesCompleted when everything is done.
                if let Err(e) = supervisor.write_all_stages_completed(context).await {
                    tracing::error!(error = %e, "Failed to write synthetic AllStagesCompleted");
                }
                Ok(EventLoopDirective::Transition(
                    PipelineEvent::AllStagesCompleted,
                ))
            } else {
                supervisor.drain_idle_iters = supervisor.drain_idle_iters.saturating_add(1);
                // Soft warning when drain is taking unusually long.
                // This does NOT cause an abort: 080o-part-2 semantics require
                // explicit contract failures for aborts, not elapsed time.
                // Future work (090a) may introduce configurable, rate-aware
                // liveness bounds as a separate concern from transport contracts.
                if supervisor.drain_idle_iters == DRAIN_LIVENESS_MAX_IDLE {
                    let snapshot = supervisor.barrier_snapshot(context);
                    tracing::warn!(
                        pending_stages = ?snapshot.pending_stages,
                        missing_contracts = ?snapshot.missing_contracts,
                        completed_stages = snapshot.completed,
                        total_stages = snapshot.total,
                        idle_iterations = supervisor.drain_idle_iters,
                        "Drain taking unusually long; waiting for stages to complete. \
                         No abort will occur - only explicit contract failures cause aborts. \
                         See FLOWIP-080o-part-2 and FLOWIP-090a for liveness semantics."
                    );
                }
                Ok(EventLoopDirective::Continue)
            }
        }
        PollResult::Error(e) => {
            tracing::error!("Error polling system journal during drain: {}", e);
            Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                message: format!("System journal error during drain: {e}"),
            }))
        }
    }
}
