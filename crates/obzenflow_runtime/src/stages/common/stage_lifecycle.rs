// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Retained results of child FSM transitions. Observation never drives a child.

use super::stage_handle::StageError;
use crate::supervised_base::publication;
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::CausalFrontier;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageMilestone {
    Initialized,
    Ready,
    Started,
}

#[derive(Debug, Clone, Default)]
pub struct StageSnapshot {
    pub accounting: ExecutionAccounting,
    pub causal_context: CausalFrontier,
}

#[derive(Debug, Clone)]
pub struct MilestoneAck {
    pub milestone: StageMilestone,
    pub snapshot: StageSnapshot,
}

#[derive(Debug, Clone)]
pub struct LifecycleFailure {
    pub cause: StageError,
    pub snapshot: StageSnapshot,
}

#[derive(Debug, Clone)]
pub enum LifecycleExit {
    Completed(StageSnapshot),
    Cancelled {
        reason: String,
        snapshot: StageSnapshot,
    },
    Failed(LifecycleFailure),
}

impl LifecycleExit {
    pub fn snapshot(&self) -> &StageSnapshot {
        match self {
            Self::Completed(snapshot) | Self::Cancelled { snapshot, .. } => snapshot,
            Self::Failed(failure) => &failure.snapshot,
        }
    }

    pub fn result(&self) -> Result<(), StageError> {
        match self {
            Self::Completed(_) => Ok(()),
            Self::Cancelled { .. } => Err(StageError::Aborted),
            Self::Failed(failure) => Err(failure.cause.clone()),
        }
    }
}

/// Observation of an assigned FSM state. This mapping selects no operations
/// and grants no authority to the runner or the handle.
#[derive(Debug, Clone, Default)]
pub enum LifecyclePhase {
    #[default]
    Other,
    Initializing,
    Initialized,
    Ready,
    Active,
    Finalising,
    Failing(String),
    Cancelling(String),
    Completed,
    Failed(String),
    Cancelled(String),
}

#[derive(Clone, Default)]
pub(crate) struct Results {
    pub(crate) initialized: Option<MilestoneAck>,
    pub(crate) ready: Option<MilestoneAck>,
    pub(crate) started: Option<MilestoneAck>,
    pub(crate) failure: Option<LifecycleFailure>,
    pub(crate) outcome: Option<LifecycleExit>,
    pub(crate) snapshot: StageSnapshot,
}

pub(crate) struct LifecycleResults {
    tx: watch::Sender<Results>,
}

tokio::task_local! {
    static CURRENT: Arc<LifecycleResults>;
}

impl LifecycleResults {
    pub(crate) fn new() -> Arc<Self> {
        let (tx, _) = watch::channel(Results::default());
        Arc::new(Self { tx })
    }

    pub(crate) async fn enter<T>(
        self: &Arc<Self>,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        CURRENT.scope(self.clone(), future).await
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Results> {
        self.tx.subscribe()
    }

    pub(crate) fn current(&self) -> Results {
        self.tx.borrow().clone()
    }

    pub(crate) fn fail(&self, cause: StageError) {
        self.tx.send_modify(|results| {
            if results.failure.is_none() {
                results.failure = Some(LifecycleFailure {
                    cause,
                    snapshot: results.snapshot.clone(),
                });
            }
        });
    }

    pub(crate) fn cancelled(&self, reason: &str) {
        self.tx.send_modify(|results| {
            if results.outcome.is_none() {
                results.outcome = Some(LifecycleExit::Cancelled {
                    reason: reason.to_owned(),
                    snapshot: results.snapshot.clone(),
                });
            }
        });
    }

    /// Retain the original operation error only when the child FSM selects a
    /// failure phase. Diagnostics never select or override a lifecycle outcome.
    pub(crate) fn observe(
        phase: &LifecyclePhase,
        accounting: ExecutionAccounting,
        failure: Option<StageError>,
    ) {
        let snapshot = StageSnapshot {
            accounting,
            causal_context: publication::capture(),
        };
        let _ = CURRENT.try_with(|owner| {
            owner.tx.send_modify(|results| {
                results.snapshot = snapshot.clone();
                match phase {
                    LifecyclePhase::Initialized => {
                        results.initialized.get_or_insert(MilestoneAck {
                            milestone: StageMilestone::Initialized,
                            snapshot,
                        });
                    }
                    LifecyclePhase::Ready => {
                        results.ready.get_or_insert(MilestoneAck {
                            milestone: StageMilestone::Ready,
                            snapshot,
                        });
                    }
                    LifecyclePhase::Active => {
                        results.ready.get_or_insert(MilestoneAck {
                            milestone: StageMilestone::Ready,
                            snapshot: snapshot.clone(),
                        });
                        results.started.get_or_insert(MilestoneAck {
                            milestone: StageMilestone::Started,
                            snapshot,
                        });
                    }
                    LifecyclePhase::Failing(cause) | LifecyclePhase::Failed(cause) => {
                        results.failure.get_or_insert_with(|| LifecycleFailure {
                            cause: failure.unwrap_or_else(|| StageError::Other(cause.clone())),
                            snapshot: snapshot.clone(),
                        });
                        if matches!(phase, LifecyclePhase::Failed(_)) {
                            results.outcome =
                                Some(LifecycleExit::Failed(results.failure.clone().unwrap()));
                        }
                    }
                    LifecyclePhase::Completed => {
                        results.outcome = Some(LifecycleExit::Completed(snapshot))
                    }
                    LifecyclePhase::Cancelled(reason) => {
                        results.outcome = Some(match &results.failure {
                            Some(failure) => LifecycleExit::Failed(failure.clone()),
                            None => LifecycleExit::Cancelled {
                                reason: reason.clone(),
                                snapshot,
                            },
                        });
                    }
                    _ => {}
                }
            })
        });
    }

    pub(crate) fn settled(&self, causal_context: CausalFrontier) -> LifecycleExit {
        let results = self.current();
        let snapshot = StageSnapshot {
            causal_context,
            ..results.snapshot
        };
        if let Some(failure) = results.failure {
            return LifecycleExit::Failed(LifecycleFailure {
                snapshot,
                ..failure
            });
        }
        match results.outcome {
            Some(LifecycleExit::Completed(_)) => LifecycleExit::Completed(snapshot),
            Some(LifecycleExit::Cancelled { reason, .. }) => {
                LifecycleExit::Cancelled { reason, snapshot }
            }
            _ => LifecycleExit::Failed(LifecycleFailure {
                cause: StageError::InvalidState(
                    "child task ended without a settled FSM outcome".into(),
                ),
                snapshot,
            }),
        }
    }
}
