// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned source policy boundary (FLOWIP-115a).
//!
//! The runtime sees only `SourceBoundary`. This module owns the
//! middleware policy onion hidden behind that seam.

use super::MiddlewareContext;
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope, WriterId};
use obzenflow_runtime::prelude::SourceError;
use obzenflow_runtime::stages::source::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollReport,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// RAII guard returned by source-policy admission for reserved resources.
pub trait SourceAdmissionGuard: Send + Sync {}

impl<T: Send + Sync> SourceAdmissionGuard for T {}

/// Admission decision from one source policy.
pub enum SourceAdmission {
    /// Admit the poll, optionally holding a guard across the protected attempt.
    Admit(Option<Box<dyn SourceAdmissionGuard>>),
    /// Reject before polling. 115a policies do not return this, but the seam is
    /// reserved for future fail-fast policies.
    Reject { reason: String },
}

/// Post-poll source-policy decision. 115a supports proceeding only, while the
/// enum leaves the trait extensible for future post-poll policy decisions.
pub enum SourceAfterPoll {
    Proceed,
}

/// Raw source-poll outcome shown independently to each policy.
pub enum SourcePollOutcome<'a> {
    Delivered {
        events: &'a [ChainEvent],
        poll_duration: Duration,
    },
    Empty {
        poll_duration: Duration,
    },
    Eof {
        poll_duration: Duration,
    },
    Failed {
        error: &'a SourceError,
        poll_duration: Duration,
    },
    RejectedBy {
        policy: &'static str,
        reason: &'a str,
    },
}

/// Source-shaped policy context. It owns the observability outbox returned by
/// the boundary report and never crosses into the runtime supervisor.
pub struct SourcePolicyCtx {
    writer_id: WriterId,
    synthetic_event: Option<ChainEvent>,
    middleware_ctx: MiddlewareContext,
}

impl SourcePolicyCtx {
    pub fn new(writer_id: WriterId) -> Self {
        Self {
            writer_id,
            synthetic_event: None,
            middleware_ctx: MiddlewareContext::with_scope(
                MiddlewareExecutionScope::LiveSourceBoundary,
            ),
        }
    }

    pub fn writer_id(&self) -> WriterId {
        self.writer_id
    }

    pub fn synthetic_event_clone(&mut self) -> ChainEvent {
        self.synthetic_event().clone()
    }

    pub fn write_control_event(&mut self, event: ChainEvent) {
        self.middleware_ctx.write_control_event(event);
    }

    pub fn take_control_events(&mut self) -> Vec<ChainEvent> {
        self.middleware_ctx.take_control_events()
    }

    pub(crate) fn middleware_context_mut(&mut self) -> &mut MiddlewareContext {
        &mut self.middleware_ctx
    }

    fn synthetic_event(&mut self) -> &ChainEvent {
        if self.synthetic_event.is_none() {
            self.synthetic_event = Some(ChainEventFactory::data_event(
                self.writer_id,
                "system.source.next",
                serde_json::json!({
                    "source_type": "boundary",
                    "timestamp_ms": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                }),
            ));
        }
        self.synthetic_event
            .as_ref()
            .expect("synthetic event must be initialized")
    }
}

/// A source resilience policy behind the adapter-owned boundary.
#[async_trait]
pub trait SourcePolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission;

    async fn after_poll(
        &self,
        _batch: &[ChainEvent],
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        SourceAfterPoll::Proceed
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, ctx: &mut SourcePolicyCtx);
}

/// Source boundary backed by a declared-order policy chain.
pub struct PerSourcePolicyBoundary {
    policies: Arc<Vec<Arc<dyn SourcePolicy>>>,
    writer_id: WriterId,
}

impl PerSourcePolicyBoundary {
    pub fn new(policies: Vec<Arc<dyn SourcePolicy>>, writer_id: WriterId) -> Self {
        Self {
            policies: Arc::new(policies),
            writer_id,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }
}

type SourceAdmitGuard = Option<Box<dyn SourceAdmissionGuard>>;

impl SourceBoundary for PerSourcePolicyBoundary {
    fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a> {
        Box::pin(async move {
            if self.policies.is_empty() {
                return SourceBoundaryReport {
                    outcome: SourceBoundaryOutcome::Polled(execute.await),
                    control_events: Vec::new(),
                };
            }

            let mut ctx = SourcePolicyCtx::new(self.writer_id);
            let mut admitted: Vec<(&Arc<dyn SourcePolicy>, SourceAdmitGuard)> = Vec::new();

            for policy in self.policies.iter() {
                match policy.admit(&mut ctx).await {
                    SourceAdmission::Admit(guard) => admitted.push((policy, guard)),
                    SourceAdmission::Reject { reason } => {
                        let outcome = SourcePollOutcome::RejectedBy {
                            policy: policy.label(),
                            reason: &reason,
                        };
                        for (prior, _) in admitted.iter().rev() {
                            prior.observe(&outcome, &mut ctx);
                        }
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Rejected { reason },
                            control_events: ctx.take_control_events(),
                        };
                    }
                }
            }

            let poll = execute.await;
            if let Ok(SourcePollCompletion::Batch(batch)) = &poll.result {
                if !batch.is_empty() {
                    for (policy, _) in &admitted {
                        match policy.after_poll(batch, &mut ctx).await {
                            SourceAfterPoll::Proceed => {}
                        }
                    }
                }
            }

            let outcome = source_poll_outcome(&poll);
            for (policy, _) in admitted.iter().rev() {
                policy.observe(&outcome, &mut ctx);
            }

            SourceBoundaryReport {
                outcome: SourceBoundaryOutcome::Polled(poll),
                control_events: ctx.take_control_events(),
            }
        })
    }
}

fn source_poll_outcome(report: &SourcePollReport) -> SourcePollOutcome<'_> {
    match &report.result {
        Ok(SourcePollCompletion::Batch(batch)) if batch.is_empty() => SourcePollOutcome::Empty {
            poll_duration: report.poll_duration,
        },
        Ok(SourcePollCompletion::Batch(batch)) => SourcePollOutcome::Delivered {
            events: batch,
            poll_duration: report.poll_duration,
        },
        Ok(SourcePollCompletion::Eof) => SourcePollOutcome::Eof {
            poll_duration: report.poll_duration,
        },
        Err(err) => SourcePollOutcome::Failed {
            error: err,
            poll_duration: report.poll_duration,
        },
    }
}

/// Raw fact helper for policies that care whether a delivered batch contains an
/// error-marked event. This deliberately does not classify the whole attempt.
pub fn batch_has_error_marked(events: &[ChainEvent]) -> bool {
    events
        .iter()
        .any(|event| matches!(event.processing_info.status, ProcessingStatus::Error { .. }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::StageId;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    struct RecordingPolicy {
        label: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for RecordingPolicy {
        fn label(&self) -> &'static str {
            self.label
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.log
                .lock()
                .unwrap()
                .push(format!("admit:{}", self.label));
            SourceAdmission::Admit(None)
        }

        async fn after_poll(
            &self,
            _batch: &[ChainEvent],
            _ctx: &mut SourcePolicyCtx,
        ) -> SourceAfterPoll {
            self.log
                .lock()
                .unwrap()
                .push(format!("after:{}", self.label));
            SourceAfterPoll::Proceed
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.log
                .lock()
                .unwrap()
                .push(format!("observe:{}", self.label));
        }
    }

    fn test_event() -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.source",
            json!({ "value": 1 }),
        )
    }

    #[tokio::test]
    async fn source_boundary_composes_forward_poll_forward_reverse() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                Arc::new(RecordingPolicy {
                    label: "a",
                    log: log.clone(),
                }),
                Arc::new(RecordingPolicy {
                    label: "b",
                    log: log.clone(),
                }),
            ],
            WriterId::from(StageId::new()),
        );

        let report = boundary
            .around_poll(Box::pin(async {
                SourcePollReport {
                    result: Ok(SourcePollCompletion::Batch(vec![test_event()])),
                    poll_duration: Duration::from_millis(1),
                }
            }))
            .await;

        assert!(matches!(report.outcome, SourceBoundaryOutcome::Polled(_)));
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:a",
                "admit:b",
                "after:a",
                "after:b",
                "observe:b",
                "observe:a",
            ]
        );
    }

    struct RejectingPolicy {
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for RejectingPolicy {
        fn label(&self) -> &'static str {
            "rejecting"
        }

        async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.log.lock().unwrap().push("admit:rejecting".to_string());
            ctx.write_control_event(test_event());
            SourceAdmission::Reject {
                reason: "not now".to_string(),
            }
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            panic!("rejecting policy should not observe its own rejection")
        }
    }

    struct RejectObservingPolicy {
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for RejectObservingPolicy {
        fn label(&self) -> &'static str {
            "observer"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.log.lock().unwrap().push("admit:observer".to_string());
            SourceAdmission::Admit(None)
        }

        fn observe(&self, outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            match outcome {
                SourcePollOutcome::RejectedBy { policy, reason } => {
                    self.log
                        .lock()
                        .unwrap()
                        .push(format!("observe:rejected_by:{policy}:{reason}"));
                }
                SourcePollOutcome::Empty { .. } => {
                    self.log.lock().unwrap().push("observe:empty".to_string());
                }
                _ => self.log.lock().unwrap().push("observe:other".to_string()),
            }
        }
    }

    #[tokio::test]
    async fn source_boundary_reject_observes_not_polled_and_returns_outbox() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let executed = Arc::new(AtomicUsize::new(0));
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                Arc::new(RejectObservingPolicy { log: log.clone() }),
                Arc::new(RejectingPolicy { log: log.clone() }),
            ],
            WriterId::from(StageId::new()),
        );

        let executed_in_future = executed.clone();
        let report = boundary
            .around_poll(Box::pin(async move {
                executed_in_future.fetch_add(1, Ordering::SeqCst);
                SourcePollReport {
                    result: Ok(SourcePollCompletion::Batch(vec![test_event()])),
                    poll_duration: Duration::from_millis(1),
                }
            }))
            .await;

        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Rejected { ref reason } if reason == "not now"
        ));
        assert_eq!(report.control_events.len(), 1);
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:observer",
                "admit:rejecting",
                "observe:rejected_by:rejecting:not now",
            ]
        );
    }

    struct DropCountingGuard {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for DropCountingGuard {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct GuardPolicy {
        admitted: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
        observed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourcePolicy for GuardPolicy {
        fn label(&self) -> &'static str {
            "guard_policy"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.admitted.fetch_add(1, Ordering::SeqCst);
            SourceAdmission::Admit(Some(Box::new(DropCountingGuard {
                dropped: self.dropped.clone(),
            })))
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.observed.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn source_boundary_drop_releases_guard_without_observe() {
        let admitted = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(AtomicUsize::new(0));
        let boundary = Arc::new(PerSourcePolicyBoundary::new(
            vec![Arc::new(GuardPolicy {
                admitted: admitted.clone(),
                dropped: dropped.clone(),
                observed: observed.clone(),
            })],
            WriterId::from(StageId::new()),
        ));

        let boundary_task = boundary.clone();
        let handle = tokio::spawn(async move {
            boundary_task
                .around_poll(Box::pin(std::future::pending::<SourcePollReport>()))
                .await
        });

        for _ in 0..10 {
            if admitted.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(admitted.load(Ordering::SeqCst), 1);

        handle.abort();
        match handle.await {
            Ok(_) => panic!("boundary task should be cancelled"),
            Err(join) => assert!(join.is_cancelled()),
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(observed.load(Ordering::SeqCst), 0);
    }
}
