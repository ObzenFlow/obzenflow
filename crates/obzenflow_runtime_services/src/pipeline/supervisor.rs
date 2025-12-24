//! Pipeline Supervisor - Manages the lifecycle of a pipeline and its stages
//!
//! The supervisor pattern provides:
//! - Hierarchical ownership of FSMs
//! - Message bus for inter-FSM communication
//! - Clean separation between supervision and business logic

use super::fsm::{stop_drain_timeout, PipelineAction, PipelineContext, PipelineEvent, PipelineState};
use crate::id_conversions::StageIdExt;
use crate::messaging::SubscriptionPoller;
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use obzenflow_core::event::types::{SeqNo, ViolationCause};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{id::SystemId, StageId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const IDLE_BACKOFF_MS: u64 = 10;
const DRAIN_LIVENESS_MAX_IDLE: u64 = 100;

/// Pipeline supervisor - manages the lifecycle of a pipeline
pub(crate) struct PipelineSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// System ID for this pipeline (used for writer_id and lifecycle events)
    pub(crate) system_id: SystemId,

    /// System journal for pipeline orchestration events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Throttled logging for barrier snapshots during drain
    pub(crate) last_barrier_log: Option<Instant>,

    /// Idle iterations observed during draining (for liveness guard)
    pub(crate) drain_idle_iters: u64,
}

/// Strictness mode for source at-least-once contracts.
///
/// This is a minimal, flow-wide toggle for how contract failures on
/// *source* edges influence pipeline behaviour:
/// - `Abort` (default): any failed source contract aborts the pipeline.
/// - `Warn`: failures are logged and surfaced via contract events, but
///   do not cause a pipeline abort. This is intended as a transitional
///   mode until full contract strictness plumbing lands in 090d.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceContractStrictMode {
    Abort,
    Warn,
}

fn source_contract_mode() -> SourceContractStrictMode {
    use std::sync::OnceLock;

    static MODE: OnceLock<SourceContractStrictMode> = OnceLock::new();

    *MODE.get_or_init(|| {
        match std::env::var("OBZENFLOW_SOURCE_CONTRACT_STRICT_MODE") {
            Ok(val) => match val.to_ascii_lowercase().as_str() {
                "warn" => SourceContractStrictMode::Warn,
                // Treat any other explicit value as Abort to avoid surprises.
                _ => SourceContractStrictMode::Abort,
            },
            Err(_) => SourceContractStrictMode::Abort,
        }
    })
}

/// Startup mode for the pipeline supervisor.
///
/// By default the supervisor will automatically transition from
/// Materialized → Running once all non-source stages report `Running`.
/// When OBZENFLOW_STARTUP_MODE=manual is set (by FlowApplication in
/// server/UI mode), the supervisor will *not* auto-run; it will remain
/// Materialized until an explicit `Run` event is received from the
/// external FlowHandle (e.g. via /api/flow/control Play).
#[inline]
fn startup_mode_manual() -> bool {
    use std::sync::OnceLock;

    static MANUAL: OnceLock<bool> = OnceLock::new();

    *MANUAL.get_or_init(|| match std::env::var("OBZENFLOW_STARTUP_MODE") {
        Ok(val) => val.eq_ignore_ascii_case("manual"),
        Err(_) => false,
    })
}

/// Helper used to decide whether a given edge should be treated as
/// gating for the purposes of contract-driven pipeline aborts.
#[inline]
fn is_gating_edge_for_contract(is_source: bool, mode: SourceContractStrictMode) -> bool {
    // Non-source edges are always gating; source edges are gating
    // only when strict mode is configured to Abort.
    !is_source || matches!(mode, SourceContractStrictMode::Abort)
}

impl crate::supervised_base::base::Supervisor for PipelineSupervisor {
    type State = PipelineState;
    type Event = PipelineEvent;
    type Context = PipelineContext;
    type Action = PipelineAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   PipelineState;
            event:   PipelineEvent;
            context: PipelineContext;
            action:  PipelineAction;
            initial: initial_state;

            state PipelineState::Created {
                on PipelineEvent::Materialize => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::info!("🔄 FSM: Created -> Materializing (Materialize event)");
                        Ok(Transition {
                            next_state: PipelineState::Materializing,
                            actions: vec![PipelineAction::CreateStages],
                        })
                    })
                };

                // Stop before materialization is a no-op.
                on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Created,
                            actions: vec![],
                        })
                    })
                };

                on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::error!("🚨 FATAL: Received Run event while in Created state!");
                        tracing::error!(
                            "🚨 This means pipeline supervisor never processed Materialize event"
                        );
                        tracing::error!("🚨 Pipeline supervisor task likely never executed!");
                        panic!("Run event received in Created state - pipeline supervisor not running");
                    })
                };
            }

            state PipelineState::Materializing {
                on PipelineEvent::MaterializationComplete => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::info!(
                            "🔄 FSM: Materializing -> Materialized (MaterializationComplete event)"
                        );
                        Ok(Transition {
                            next_state: PipelineState::Materialized,
                            actions: vec![
                                PipelineAction::StartCompletionSubscription,
                                PipelineAction::StartMetricsAggregator,
                                PipelineAction::NotifyStagesStart,
                            ],
                        })
                    })
                };

                on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Failed {
                                reason: "stop_requested_during_materialization".to_string(),
                                failure_cause: None,
                            },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    })
                };

                on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::error!("🚨 FATAL: Received Run event while in Materializing state!");
                        tracing::error!("🚨 Pipeline has not finished materializing yet");
                        tracing::error!(
                            "🚨 Check for race condition or missing MaterializationComplete"
                        );
                        panic!("Run event received in Materializing state - not ready yet");
                    })
                };

                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            Ok(Transition {
                                next_state: PipelineState::Failed { reason: message, failure_cause: None },
                                actions: vec![
                                    PipelineAction::DrainMetrics,
                                    PipelineAction::Cleanup,
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            state PipelineState::Materialized {
                on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::info!("🔄 FSM: Materialized -> Running (Run event)");
                        Ok(Transition {
                            next_state: PipelineState::Running,
                            actions: vec![PipelineAction::NotifySourceStart],
                        })
                    })
                };

                // Stop before Run: cancel the flow and cleanup stages/sources.
                on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Failed {
                                reason: "stop_requested_before_run".to_string(),
                                failure_cause: None,
                            },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    })
                };
            }

            state PipelineState::Running {
                on PipelineEvent::Abort => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Abort { reason, upstream } = event {
                            let reason_clone = reason.clone();
                            Ok(Transition {
                                next_state: PipelineState::AbortRequested {
                                    reason: reason.clone(),
                                    upstream,
                                },
                                actions: vec![
                                    PipelineAction::WritePipelineAbort { reason, upstream },
                                    PipelineAction::AbortTeardown {
                                        reason: reason_clone,
                                        upstream,
                                    },
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::Shutdown => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::SourceCompleted,
                            actions: vec![], // No actions yet - just track state
                        })
                    })
                };

                on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        if !ctx.stop_requested {
                            let mut has_active_sources = false;
                            let mut has_infinite_active_source = false;
                            for source in ctx.source_supervisors.values() {
                                if source.is_drained() {
                                    continue;
                                }
                                has_active_sources = true;
                                if source.stage_type().is_infinite_source() {
                                    has_infinite_active_source = true;
                                    break;
                                }
                            }
                            ctx.stop_requested = true;
                            ctx.stop_should_fail = has_active_sources && !has_infinite_active_source;
                            ctx.stop_deadline = Some(Instant::now() + stop_drain_timeout());
                        }

                        Ok(Transition {
                            next_state: PipelineState::SourceCompleted,
                            actions: vec![PipelineAction::StopSources],
                        })
                    })
                };

                on PipelineEvent::StageCompleted => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::StageCompleted { envelope } = event {
                            Ok(Transition {
                                next_state: PipelineState::Running,
                                actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            Ok(Transition {
                                next_state: PipelineState::Failed {
                                    reason: message,
                                    failure_cause: None,
                                },
                                actions: vec![
                                    PipelineAction::DrainMetrics,
                                    PipelineAction::Cleanup,
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::AllStagesCompleted => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Drained,
                            actions: vec![
                                PipelineAction::DrainMetrics,
                                PipelineAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state PipelineState::SourceCompleted {
                on PipelineEvent::BeginDrain => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![PipelineAction::BeginDrain],
                        })
                    })
                };

                on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::SourceCompleted,
                            actions: vec![],
                        })
                    })
                };
            }

            state PipelineState::Draining {
                on PipelineEvent::Abort => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Abort { reason, upstream } = event {
                            let reason_clone = reason.clone();
                            Ok(Transition {
                                next_state: PipelineState::AbortRequested {
                                    reason: reason.clone(),
                                    upstream,
                                },
                                actions: vec![
                                    PipelineAction::WritePipelineAbort { reason, upstream },
                                    PipelineAction::AbortTeardown {
                                        reason: reason_clone,
                                        upstream,
                                    },
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::StageCompleted => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::StageCompleted { envelope } = event {
                            Ok(Transition {
                                next_state: PipelineState::Draining,
                                actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::AllStagesCompleted => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Drained,
                            actions: vec![
                                PipelineAction::DrainMetrics,
                                PipelineAction::Cleanup,
                            ],
                        })
                    })
                };

                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            let failure_cause = if message == "stop_drain_timeout" {
                                Some(ViolationCause::Other("stop_drain_timeout".into()))
                            } else {
                                None
                            };
                            Ok(Transition {
                                next_state: PipelineState::Failed {
                                    reason: message,
                                    failure_cause,
                                },
                                actions: vec![PipelineAction::Cleanup],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![],
                        })
                    })
                };
            }

            state PipelineState::AbortRequested {
                on PipelineEvent::Error => |state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        match (state, event) {
                            (
                                PipelineState::AbortRequested { reason: abort_reason, .. },
                                PipelineEvent::Error { message },
                            ) => {
                                Ok(Transition {
                                    next_state: PipelineState::Failed {
                                        reason: message,
                                        failure_cause: Some(abort_reason),
                                    },
                                    actions: vec![
                                        PipelineAction::DrainMetrics,
                                        PipelineAction::Cleanup,
                                    ],
                                })
                            }
                            _ => Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            )),
                        }
                    })
                };

                on PipelineEvent::StopRequested => |state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let state = state.clone();
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: state,
                            actions: vec![],
                        })
                    })
                };
            }

            // Drained and Failed are terminal; no explicit transitions here.

            unhandled => |state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                let is_stop = matches!(event, PipelineEvent::StopRequested);
                Box::pin(async move {
                    if is_stop {
                        tracing::info!(
                            supervisor = "PipelineSupervisor",
                            state = %state_name,
                            event = %event_name,
                            "Ignoring StopRequested in current state"
                        );
                        return Ok(());
                    }

                    tracing::error!(
                        supervisor = "PipelineSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    Err(obzenflow_fsm::FsmError::UnhandledEvent {
                        state: state_name,
                        event: event_name,
                    })
                })
            };
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Implement SelfSupervised trait
#[async_trait::async_trait]
impl SelfSupervised for PipelineSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.system_id)
    }

    fn event_for_action_error(&self, msg: String) -> PipelineEvent {
        PipelineEvent::Error { message: msg }
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Terminal completion event is written by the FSM via PipelineAction::WritePipelineCompleted.
        // Here we emit a lightweight "drained" lifecycle marker for observability.
        let drained = SystemEvent::new(
            self.writer_id(),
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::Drained,
            ),
        );
        if let Err(e) = self.system_journal.append(drained, None).await {
            tracing::error!(
                pipeline = %self.name,
                journal_error = %e,
                "Failed to write pipeline drained event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut PipelineContext,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            PipelineState::Created => {
                // In Created state, we wait for external trigger to materialize
                // This would typically come from the FlowHandle
                tracing::info!("✅ Pipeline state in Created");
                Ok(EventLoopDirective::Continue)
            }

            PipelineState::Materializing => {
                // Check if all stages have been initialized
                let supervisors = &context.stage_supervisors;
                let source_supers = &context.source_supervisors;
                let expected_count = context.topology.stages().count();
                let initialized_count = supervisors.len() + source_supers.len();

                if initialized_count == expected_count && expected_count > 0 {
                    // All stages created, transition to Materialized
                    tracing::info!("✅ All stages initialized, transitioning to Materialized");
                    Ok(EventLoopDirective::Transition(
                        PipelineEvent::MaterializationComplete,
                    ))
                } else {
                    // Still waiting for stages to be created. Log details once before failing hard.
                    tracing::error!(
                        initialized_count,
                        expected_count,
                        "⚠️ MISMATCH DETECTED: supervisors vs topology stages"
                    );
                    tracing::debug!(
                        supervisors = ?supervisors
                            .keys()
                            .map(|id| format!("{:?}", id))
                            .collect::<Vec<_>>(),
                        sources = ?source_supers
                            .keys()
                            .map(|id| format!("{:?}", id))
                            .collect::<Vec<_>>(),
                        topology = ?context
                            .topology
                            .stages()
                            .map(|s| format!("{} ({:?})", s.name, s.id))
                            .collect::<Vec<_>>(),
                        "Materialization mismatch details"
                    );

                    panic!(
                        "Stage count mismatch: {} supervisors vs {} topology stages",
                        initialized_count, expected_count
                    );
                }
            }

            PipelineState::Materialized => {
                // First check if any stages are already running (they might have published before we subscribed)
                let supervisors = &context.stage_supervisors;
                for (stage_id, stage) in supervisors.iter() {
                    if stage.is_ready()
                        && !context
                            .topology
                            .upstream_stages(stage_id.to_topology_id())
                            .is_empty()
                    {
                        // This is a non-source stage that's already running
                        context.running_stages.insert(*stage_id);
                        tracing::info!("Stage '{}' was already running", stage.stage_name());
                    }
                }

                // Poll for stage running events to know when all non-source stages are ready
                let subscription = context.completion_subscription.as_mut().ok_or(
                    "No subscription available - should have been initialized during materialization",
                )?;

                // Check for stage running events
                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        // Process stage running event
                        let event = &envelope.event;
                        // Track last system event ID for tail reconciliation.
                        context.last_system_event_id_seen = Some(event.id.clone());
                        if let obzenflow_core::event::SystemEventType::StageLifecycle {
                            stage_id,
                            event: obzenflow_core::event::StageLifecycleEvent::Running,
                        } = &event.event
                        {
                            // Track this stage as running
                            context.running_stages.insert(stage_id.clone());

                            // Get stage name from topology
                            let stage_info = context
                                .topology
                                .stages()
                                .find(|s| s.id == stage_id.to_topology_id());
                            let stage_name = stage_info
                                .map(|s| s.name.clone())
                                .unwrap_or_else(|| "unknown".to_string());

                            // Log based on stage type
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
                    }
                    PollResult::NoEvents => {
                        // No new events, but that's OK - stages might already be running
                        // Add a brief sleep to avoid busy loop
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    PollResult::Error(e) => {
                        tracing::error!("Error polling system journal in Awaiting: {}", e);
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!("System journal error: {}", e),
                        }));
                    }
                }

                // Check if all non-source stages are running
                let running_stages = &context.running_stages;
                let topology = &context.topology;

                // Get all non-source stage IDs
                let non_source_stages: std::collections::HashSet<_> = topology
                    .stages()
                    .filter(|stage_info| !topology.upstream_stages(stage_info.id).is_empty())
                    .map(|stage_info| StageId::from_topology_id(stage_info.id))
                    .collect();

                // Check if all non-source stages are in the running set
                let all_ready = !non_source_stages.is_empty()
                    && non_source_stages
                        .iter()
                        .all(|stage_id| running_stages.contains(stage_id));

                if all_ready {
                    if startup_mode_manual() {
                        // In manual startup mode, we deliberately DO NOT auto-run
                        // the pipeline; instead we wait for an explicit Run event
                        // from FlowHandle (e.g. /api/flow/control Play).
                        tracing::info!(
                            "All {} non-source stages are running (startup_mode=manual); waiting for external Run",
                            non_source_stages.len()
                        );
                        Ok(EventLoopDirective::Continue)
                    } else {
                        tracing::info!(
                            "All {} non-source stages are running, starting pipeline",
                            non_source_stages.len()
                        );
                        Ok(EventLoopDirective::Transition(PipelineEvent::Run))
                    }
                } else {
                    // Still waiting for stages to report running
                    let waiting_for = non_source_stages.difference(&*running_stages).count();
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

            PipelineState::Running => {
                // Poll for completion events from stages (system journal)
                let subscription = context.completion_subscription.as_mut().ok_or(
                    "No subscription available - should have been initialized during materialization",
                )?;

                // Use poll_next for non-blocking event polling
                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        let event = &envelope.event;

                        // Track last system event ID for tail reconciliation.
                        context.last_system_event_id_seen = Some(event.id.clone());

                        return match &event.event {
                            obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, event: lifecycle_event } => {
                                match lifecycle_event {
                                    obzenflow_core::event::StageLifecycleEvent::Running => {
                                        // Get stage name from topology
                                        let stage_info = context.topology
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
                                            context
                                                .stage_lifecycle_metrics
                                                .insert(*stage_id, m.clone());
                                        }
                                        let stage_info = context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        tracing::info!("Stage '{}' is draining", stage_name);
                                        Ok(EventLoopDirective::Continue)
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Drained => {
                                        let stage_info = context.topology
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
                                            context
                                                .stage_lifecycle_metrics
                                                .insert(*stage_id, m.clone());
                                        }
                                        // Stage has fully completed
                                        Ok(EventLoopDirective::Transition(
                                            PipelineEvent::StageCompleted { envelope },
                                        ))
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Failed { error, metrics, .. } => {
                                        if let Some(m) = metrics {
                                            context
                                                .stage_lifecycle_metrics
                                                .insert(*stage_id, m.clone());
                                        }
                                        let stage_info = context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());

                                        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                                            message: format!("Stage '{}' failed: {}", stage_name, error),
                                        }))
                                    }
                                }
                            }
                            obzenflow_core::event::SystemEventType::PipelineLifecycle(event) => {
                                match event {
                                    obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted { .. } => {
                                        tracing::info!("Received AllStagesCompleted event!");
                                        if let Some(abort_directive) = self
                                            .missing_contract_abort(context)
                                        {
                                            return Ok(abort_directive);
                                        }
                                        Ok(EventLoopDirective::Transition(
                                            PipelineEvent::AllStagesCompleted,
                                        ))
                                    }
                                    _ => Ok(EventLoopDirective::Continue)
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
                                // Record per-edge contract status
                                if *pass {
                                    context.contract_pairs.insert(
                                        (*upstream, *reader),
                                        ContractEdgeStatus::passed(
                                            *reader_seq,
                                            *advertised_writer_seq,
                                        ),
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
                                    let mode = source_contract_mode();

                                    let should_abort =
                                        is_gating_edge_for_contract(is_source, mode);

                                    tracing::error!(
                                        ?upstream,
                                        ?reader,
                                        ?reason,
                                        is_source,
                                        mode = ?mode,
                                        "Contract status failure"
                                    );

                                    if should_abort {
                                        return Ok(EventLoopDirective::Transition(
                                            PipelineEvent::Abort {
                                                reason: reason.clone().unwrap_or_else(|| {
                                                    obzenflow_core::event::types::ViolationCause::Other(
                                                        "contract_failed".into(),
                                                    )
                                                }),
                                                upstream: Some(*upstream),
                                            },
                                        ));
                                    } else {
                                        // Warn-only for source contracts: treat as
                                        // "completed" for shutdown gating but do not abort.
                                        context.contract_status.insert(*upstream, true);
                                        return Ok(EventLoopDirective::Continue);
                                    }
                                }

                                // Mark upstream source as having passed its contract
                                context.contract_status.insert(*upstream, true);
                                let expected = &context.expected_sources;
                                let all_pass = !expected.is_empty()
                                    && expected.iter().all(|src| {
                                        context
                                            .contract_status
                                            .get(src)
                                            .copied()
                                            .unwrap_or(false)
                                    });

                                if all_pass {
                                    tracing::info!("All source contracts passed; initiating drain");
                                    Ok(EventLoopDirective::Transition(PipelineEvent::Shutdown))
                                } else {
                                    Ok(EventLoopDirective::Continue)
                                }
                            }
                            _ => Ok(EventLoopDirective::Continue)
                        };
                    }
                    PollResult::NoEvents => {
                        // No events available right now - sleep briefly to avoid busy loop
                        idle_backoff().await;
                        Ok(EventLoopDirective::Continue)
                    }
                    PollResult::Error(e) => {
                        tracing::error!("Error polling system journal: {}", e);
                        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!("System journal error: {}", e),
                        }))
                    }
                }
            }

            PipelineState::SourceCompleted => {
                // Source has completed - initiate Jonestown protocol
                tracing::info!("Source completed - beginning pipeline drain");

                // Immediately transition to start draining
                Ok(EventLoopDirective::Transition(PipelineEvent::BeginDrain))
            }

            PipelineState::Draining => {
                // If Stop initiated this drain, enforce a bounded timeout so the
                // pipeline terminates deterministically even if some stage never
                // reports completion.
                if let Some(deadline) = context.stop_deadline {
                    if Instant::now() >= deadline {
                        tracing::warn!(
                            pipeline = %self.name,
                            "Stop drain timeout expired; forcing pipeline failure"
                        );
                        context.stop_deadline = None;
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: "stop_drain_timeout".to_string(),
                        }));
                    }
                }

                // Continue polling for completion events during drain
                let subscription = context
                    .completion_subscription
                    .as_mut()
                    .ok_or("No subscription available")?;

                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        let event = &envelope.event;

                        // Track last system event ID for tail reconciliation.
                        context.last_system_event_id_seen = Some(event.id.clone());

                        return match &event.event {
                            obzenflow_core::event::SystemEventType::StageLifecycle {
                                stage_id,
                                event:
                                    obzenflow_core::event::StageLifecycleEvent::Completed { metrics },
                            } => {
                                if let Some(m) = metrics {
                                    context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                                }
                                // Process stage completion immediately
                                Ok(EventLoopDirective::Transition(
                                    PipelineEvent::StageCompleted { envelope },
                                ))
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
                                        ContractEdgeStatus::passed(
                                            *reader_seq,
                                            *advertised_writer_seq,
                                        ),
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
                                    let is_source =
                                        context.expected_sources.contains(&upstream.clone());
                                    let mode = source_contract_mode();

                                    let should_abort = !is_source
                                        || matches!(mode, SourceContractStrictMode::Abort);

                                    tracing::error!(
                                        ?upstream,
                                        ?reader,
                                        ?reason,
                                        is_source,
                                        mode = ?mode,
                                        "Contract status failure during drain"
                                    );

                                    if should_abort {
                                        return Ok(EventLoopDirective::Transition(
                                            PipelineEvent::Abort {
                                                reason: reason.clone().unwrap_or_else(|| {
                                                    obzenflow_core::event::types::ViolationCause::Other(
                                                        "contract_failed".into(),
                                                    )
                                                }),
                                                upstream: Some(upstream.clone()),
                                            },
                                        ));
                                    } else {
                                        // Warn-only for source contracts during drain:
                                        // mark as completed for barrier gating but do not abort.
                                        context.contract_status.insert(upstream.clone(), true);
                                        return Ok(EventLoopDirective::Continue);
                                    }
                                } else {
                                    context.contract_status.insert(upstream.clone(), true);
                                }
                                Ok(EventLoopDirective::Continue)
                            }
                            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted {
                                    ..
                                },
                            ) => {
                                tracing::info!(
                                    "All stages have completed - transitioning to drained"
                                );
                                if let Some(abort_directive) = self.missing_contract_abort(context)
                                {
                                    return Ok(abort_directive);
                                }
                                Ok(EventLoopDirective::Transition(
                                    PipelineEvent::AllStagesCompleted,
                                ))
                            }
                            _ => {
                                // Log other events
                                tracing::debug!("Received event during drain: {:?}", event.event);
                                Ok(EventLoopDirective::Continue)
                            }
                        };
                    }
                    PollResult::NoEvents => {
                        // No events available right now - sleep briefly to avoid busy loop
                        idle_backoff().await;
                        if self.should_log_barrier() {
                            let snapshot = self.barrier_snapshot(context);
                            tracing::info!(
                                pending_stages = ?snapshot.pending_stages,
                                missing_contracts = ?snapshot.missing_contracts,
                                completed_stages = snapshot.completed,
                                total_stages = snapshot.total,
                                satisfied_contracts = snapshot.satisfied_contracts,
                                total_contracts = snapshot.total_contracts,
                                "Drain barrier snapshot (no events)"
                            );
                        }
                        if self.all_stages_and_contracts_complete(context) {
                            // Synthesize AllStagesCompleted when everything is done
                            if let Err(e) = self.write_all_stages_completed(context).await {
                                tracing::error!(error = %e, "Failed to write synthetic AllStagesCompleted");
                            }
                            Ok(EventLoopDirective::Transition(
                                PipelineEvent::AllStagesCompleted,
                            ))
                        } else {
                            self.drain_idle_iters = self.drain_idle_iters.saturating_add(1);
                            // Soft warning when drain is taking unusually long.
                            // This does NOT cause an abort - 080o-part-2 semantics require
                            // explicit contract failures for aborts, not elapsed time.
                            // Future work (090a) may introduce configurable, rate-aware
                            // liveness bounds as a separate concern from transport contracts.
                            if self.drain_idle_iters == DRAIN_LIVENESS_MAX_IDLE {
                                let snapshot = self.barrier_snapshot(context);
                                tracing::warn!(
                                    pending_stages = ?snapshot.pending_stages,
                                    missing_contracts = ?snapshot.missing_contracts,
                                    completed_stages = snapshot.completed,
                                    total_stages = snapshot.total,
                                    idle_iterations = self.drain_idle_iters,
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
                            message: format!("System journal error during drain: {}", e),
                        }))
                    }
                }
            }

            PipelineState::Drained => {
                // Terminal success: write flow_completed with duration + rollup metrics,
                // then terminate. We also keep the lighter-weight "drained" marker
                // in write_completion_event().
                if context.flow_start_time.is_some() {
                    // Best-effort reconciliation with tail system events to ensure we have
                    // the latest wide lifecycle snapshots before computing flow rollup.
                    if let Err(e) = self.reconcile_stage_metrics_from_tail(context).await {
                        tracing::warn!(
                            pipeline = %self.name,
                            error = %e,
                            "Failed to reconcile stage lifecycle metrics from tail before completion"
                        );
                    }

                    // Compute flow duration (best-effort)
                    let duration_ms = context
                        .flow_start_time
                        .map(|start| start.elapsed().as_millis() as u64)
                        .unwrap_or(0);

                    // Compute flow-level lifecycle metrics from per-stage snapshots
                    let metrics = crate::pipeline::fsm::compute_flow_lifecycle_metrics(context);

                    let system_event_factory =
                        obzenflow_core::event::system_event::SystemEventFactory::new(
                            self.system_id,
                        );
                    if context.stop_requested && context.stop_should_fail {
                        // Finite-only flows treat Stop as a cancellation/abort signal.
                        let failed = system_event_factory.pipeline_failed(
                            "user_stop".to_string(),
                            duration_ms,
                            Some(metrics.clone()),
                            Some(ViolationCause::Other("user_stop".into())),
                        );

                        if let Err(e) = self.system_journal.append(failed, None).await {
                            tracing::error!(
                                pipeline = %self.name,
                                journal_error = %e,
                                "Failed to write pipeline failed event for user_stop"
                            );
                        } else {
                            tracing::info!(
                                pipeline = %self.name,
                                "Pipeline failed event written (user_stop)"
                            );
                        }
                    } else {
                        let completed = system_event_factory.pipeline_completed(duration_ms, metrics);

                        if let Err(e) = self.system_journal.append(completed, None).await {
                            tracing::error!(
                                pipeline = %self.name,
                                journal_error = %e,
                                "Failed to write pipeline completed event"
                            );
                        } else {
                            tracing::info!(
                                pipeline = %self.name,
                                "Pipeline completed event written (success path)"
                            );
                        }
                    }
                }

                tracing::info!("Pipeline drained, terminating");
                Ok(EventLoopDirective::Terminate)
            }

            PipelineState::Failed {
                reason,
                failure_cause,
            } => {
                // Terminal failure: write flow_failed with duration + best-effort rollup metrics.
                // This snapshot is derived from per-stage lifecycle snapshots
                // (`stage_lifecycle_metrics`) via `compute_flow_lifecycle_metrics`, after a
                // best-effort reconciliation of wide lifecycle events from the system
                // journal tail to capture any completions written after we stopped polling.
                if let Err(e) = self.reconcile_stage_metrics_from_tail(context).await {
                    tracing::warn!(
                        pipeline = %self.name,
                        error = %e,
                        "Failed to reconcile stage lifecycle metrics from tail before failure"
                    );
                }

                let duration_ms = context
                    .flow_start_time
                    .map(|start| start.elapsed().as_millis() as u64)
                    .unwrap_or(0);

                let metrics = Some(crate::pipeline::fsm::compute_flow_lifecycle_metrics(
                    context,
                ));

                let system_event_factory =
                    obzenflow_core::event::system_event::SystemEventFactory::new(self.system_id);
                let failed = system_event_factory.pipeline_failed(
                    reason.clone(),
                    duration_ms,
                    metrics,
                    failure_cause.clone(),
                );

                if let Err(e) = self.system_journal.append(failed, None).await {
                    tracing::error!(
                        pipeline = %self.name,
                        journal_error = %e,
                        "Failed to write pipeline failed event"
                    );
                } else {
                    tracing::error!(
                        pipeline = %self.name,
                        error = %reason,
                        "Pipeline failed event written (failure path)"
                    );
                }

                // Terminal state
                tracing::error!("Pipeline failed: {}", reason);
                Ok(EventLoopDirective::Terminate)
            }

            PipelineState::AbortRequested { reason, upstream } => {
                let msg = format!(
                    "Pipeline abort requested: {:?} (upstream={:?})",
                    reason, upstream
                );
                Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                    message: msg,
                }))
            }
        }
    }
}

impl PipelineSupervisor {
    /// Best-effort reconciliation of per-stage lifecycle metrics using tail system events.
    ///
    /// Reads only system events that causally follow the last system event observed via
    /// the completion subscription and updates `stage_lifecycle_metrics` with any
    /// terminal wide lifecycle snapshots found there.
    async fn reconcile_stage_metrics_from_tail(
        &self,
        context: &mut PipelineContext,
    ) -> Result<(), String> {
        let last_id = match &context.last_system_event_id_seen {
            Some(id) => id.clone(),
            None => {
                // No prior system events recorded; nothing to reconcile.
                return Ok(());
            }
        };

        let tail_events = self
            .system_journal
            .read_causally_after(&last_id)
            .await
            .map_err(|e| format!("Failed to read tail system events: {}", e))?;

        if tail_events.is_empty() {
            return Ok(());
        }

        for envelope in tail_events.iter() {
            if let obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, event } =
                &envelope.event.event
            {
                match event {
                    obzenflow_core::event::StageLifecycleEvent::Completed { metrics: Some(m) }
                    | obzenflow_core::event::StageLifecycleEvent::Failed {
                        metrics: Some(m), ..
                    } => {
                        context.stage_lifecycle_metrics.insert(*stage_id, m.clone());
                    }
                    obzenflow_core::event::StageLifecycleEvent::Draining { metrics: Some(m) } => {
                        context
                            .stage_lifecycle_metrics
                            .entry(*stage_id)
                            .or_insert_with(|| m.clone());
                    }
                    _ => {}
                }
            }
        }

        if let Some(last_envelope) = tail_events.last() {
            context.last_system_event_id_seen = Some(last_envelope.event.id.clone());
        }

        Ok(())
    }

    /// If any contract edge has an explicit failure recorded, return an abort directive
    fn missing_contract_abort(
        &self,
        context: &PipelineContext,
    ) -> Option<EventLoopDirective<PipelineEvent>> {
        let seen = &context.contract_pairs;

        // Find any edge with an explicit failure (contract violated)
        if let Some(((upstream, reader), status)) =
            seen.iter().find(|((upstream, _reader), status)| {
                let is_source = context.expected_sources.contains(upstream);
                let mode = source_contract_mode();
                let is_gating = is_gating_edge_for_contract(is_source, mode);
                is_gating && !status.is_passed()
            })
        {
            let upstream_name = context
                .topology
                .stage_name(upstream.to_topology_id())
                .unwrap_or("unknown")
                .to_string();
            let reader_name = context
                .topology
                .stage_name(reader.to_topology_id())
                .unwrap_or("unknown")
                .to_string();

            // Prefer the recorded violation cause, fall back to a generic label
            let reason = status
                .reason
                .clone()
                .unwrap_or_else(|| ViolationCause::Other("contract_failed".into()));

            tracing::error!(
                ?upstream,
                ?reader,
                upstream_name,
                reader_name,
                "Contract edge recorded as failed; aborting pipeline based on explicit contract violation"
            );

            Some(EventLoopDirective::Transition(PipelineEvent::Abort {
                reason,
                upstream: Some(*upstream),
            }))
        } else {
            None
        }
    }

    /// Check if all stages have completed and all contract pairs are satisfied
    fn all_stages_and_contracts_complete(&self, context: &PipelineContext) -> bool {
        let completed = context.completed_stages.len();
        let total = context.topology.num_stages();

        if completed < total {
            return false;
        }

        let seen = &context.contract_pairs;

        // Success requires that no *gating* contract edge has an explicit failure recorded.
        // Missing contract evidence is tolerated here; it is surfaced via logs/metrics
        // but does not block drain at the transport-contract layer. Source edges configured
        // in warn-only mode are treated as non-gating for this check.
        !seen.iter().any(|((upstream, _reader), status)| {
            let is_source = context.expected_sources.contains(upstream);
            let mode = source_contract_mode();
            let is_gating = is_gating_edge_for_contract(is_source, mode);
            is_gating && !status.is_passed()
        })
    }

    /// Synthesize and write AllStagesCompleted when we know we’re done
    async fn write_all_stages_completed(&self, context: &PipelineContext) -> Result<(), String> {
        let event = SystemEvent::new(
            WriterId::from(self.system_id),
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted { metrics: None },
            ),
        );
        self.system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write AllStagesCompleted: {}", e))
    }

    /// Snapshot the current drain barrier state for logging/inspection
    fn barrier_snapshot(&self, context: &PipelineContext) -> BarrierSnapshot {
        let completed: Vec<StageId> = context.completed_stages.clone();
        let expected_stages: Vec<StageId> = context
            .topology
            .stages()
            .map(|s| StageId::from_topology_id(s.id))
            .collect();
        let pending_stages: Vec<StageId> = expected_stages
            .iter()
            .copied()
            .filter(|id| !completed.contains(id))
            .collect();

        let expected_contracts = context.expected_contract_pairs.clone();
        let seen = &context.contract_pairs;
        let missing_contracts: Vec<(StageId, StageId)> = expected_contracts
            .iter()
            .filter(|(upstream, reader)| {
                let is_source = context.expected_sources.contains(upstream);
                let mode = source_contract_mode();
                let is_gating = is_gating_edge_for_contract(is_source, mode);
                if !is_gating {
                    return false;
                }
                !matches!(seen.get(&(*upstream, *reader)), Some(status) if status.is_passed())
            })
            .copied()
            .collect();

        let total_contracts = expected_contracts.len();
        let satisfied_contracts = expected_contracts
            .iter()
            .filter(|(upstream, reader)| {
                let is_source = context.expected_sources.contains(upstream);
                let mode = source_contract_mode();
                let is_gating = is_gating_edge_for_contract(is_source, mode);
                if !is_gating {
                    return false;
                }
                matches!(seen.get(&(*upstream, *reader)), Some(status) if status.is_passed())
            })
            .count();

        BarrierSnapshot {
            pending_stages,
            missing_contracts,
            completed: completed.len(),
            total: expected_stages.len(),
            satisfied_contracts,
            total_contracts,
        }
    }

    /// Throttle barrier logging to avoid spamming the drain loop
    fn should_log_barrier(&mut self) -> bool {
        let now = Instant::now();
        match self.last_barrier_log {
            Some(last) if now.duration_since(last) < Duration::from_secs(1) => false,
            _ => {
                self.last_barrier_log = Some(now);
                true
            }
        }
    }
}

/// Lightweight snapshot of drain barrier progress for diagnostics
#[derive(Debug)]
struct BarrierSnapshot {
    pending_stages: Vec<StageId>,
    missing_contracts: Vec<(StageId, StageId)>,
    completed: usize,
    total: usize,
    satisfied_contracts: usize,
    total_contracts: usize,
}

/// Status for a contract edge (upstream -> reader)
#[derive(Clone, Debug, Default)]
pub struct ContractEdgeStatus {
    passed: bool,
    reason: Option<ViolationCause>,
    reader_seq: Option<SeqNo>,
    advertised_writer_seq: Option<SeqNo>,
}

impl ContractEdgeStatus {
    pub(crate) fn passed(reader_seq: Option<SeqNo>, advertised_writer_seq: Option<SeqNo>) -> Self {
        Self {
            passed: true,
            reason: None,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn failed(
        reason: Option<ViolationCause>,
        reader_seq: Option<SeqNo>,
        advertised_writer_seq: Option<SeqNo>,
    ) -> Self {
        Self {
            passed: false,
            reason,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn is_passed(&self) -> bool {
        self.passed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_gating_edge_for_contract_behaves_as_expected() {
        // Non-source edges are always gating, regardless of mode.
        assert!(is_gating_edge_for_contract(
            false,
            SourceContractStrictMode::Abort
        ));
        assert!(is_gating_edge_for_contract(
            false,
            SourceContractStrictMode::Warn
        ));

        // Source edges are gating only when strict mode is Abort.
        assert!(is_gating_edge_for_contract(
            true,
            SourceContractStrictMode::Abort
        ));
        assert!(
            !is_gating_edge_for_contract(true, SourceContractStrictMode::Warn),
            "source edges should be non-gating when strict mode is Warn"
        );
    }
}

#[inline]
async fn idle_backoff() {
    tokio::time::sleep(Duration::from_millis(IDLE_BACKOFF_MS)).await;
}
