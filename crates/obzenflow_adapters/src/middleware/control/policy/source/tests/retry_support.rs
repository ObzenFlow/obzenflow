// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct RetryContextMarker;

impl MiddlewareContextKey for RetryContextMarker {
    type Value = ();
    const LABEL: &'static str = "source_retry_test_marker";
}

struct RetryOwnerPolicy {
    owner: BoundaryRetryOwner,
    log: Arc<Mutex<Vec<String>>>,
    admissions: Arc<AtomicUsize>,
    guard_drops: Arc<AtomicUsize>,
    recovery_allowed: Arc<AtomicBool>,
    observed: Option<Arc<Notify>>,
}

#[async_trait]
impl SourcePolicy for RetryOwnerPolicy {
    fn label(&self) -> &'static str {
        "retry_owner"
    }

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        let middleware = ctx.middleware_context_mut();
        assert!(
            !middleware.contains::<RetryContextMarker>(),
            "each retry attempt must receive a fresh source policy context"
        );
        middleware.insert::<RetryContextMarker>(());
        let attempt = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
        self.log
            .lock()
            .unwrap()
            .push(format!("admit:owner:{attempt}"));
        SourceAdmission::Admit(Some(Box::new(DropCountingGuard {
            dropped: self.guard_drops.clone(),
        })))
    }

    fn commit_execution(&self, _ctx: &mut SourcePolicyCtx) {
        self.log.lock().unwrap().push("commit:owner".to_string());
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        let suffix = match outcome {
            SourcePollOutcome::Failed { .. } => "failed",
            SourcePollOutcome::Delivered { .. } => "delivered",
            SourcePollOutcome::Empty { .. } => "empty",
            SourcePollOutcome::Eof { .. } => "eof",
            SourcePollOutcome::RejectedBy { .. } => "rejected",
            SourcePollOutcome::NotExecuted { .. } => "not_executed",
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("observe:owner:{suffix}"));
        if let Some(observed) = &self.observed {
            observed.notify_one();
        }
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        Some(self.owner.clone())
    }

    fn recovery_allowed_after_settlement(&self, _ctx: &SourcePolicyCtx) -> bool {
        self.recovery_allowed.load(Ordering::SeqCst)
    }
}

struct InnerOnionPolicy {
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SourcePolicy for InnerOnionPolicy {
    fn label(&self) -> &'static str {
        "inner"
    }

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        assert!(ctx
            .middleware_context_mut()
            .contains::<RetryContextMarker>());
        self.log.lock().unwrap().push("admit:inner".to_string());
        SourceAdmission::Admit(None)
    }

    fn commit_execution(&self, _ctx: &mut SourcePolicyCtx) {
        self.log.lock().unwrap().push("commit:inner".to_string());
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        let suffix = if matches!(outcome, SourcePollOutcome::Failed { .. }) {
            "failed"
        } else {
            "delivered"
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("observe:inner:{suffix}"));
    }
}

struct ObserveOnlyAfterDeadlinePolicy {
    after_poll_calls: Arc<AtomicUsize>,
    observations: Arc<AtomicUsize>,
}

#[async_trait]
impl SourcePolicy for ObserveOnlyAfterDeadlinePolicy {
    fn label(&self) -> &'static str {
        "observe_only_after_deadline"
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        SourceAdmission::Admit(None)
    }

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        self.after_poll_calls.fetch_add(1, Ordering::SeqCst);
        SourceAfterPoll::Proceed
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        self.observations.fetch_add(1, Ordering::SeqCst);
    }
}

struct BlockingAfterPollRetryOwnerPolicy {
    owner: BoundaryRetryOwner,
    after_poll_started: Arc<Notify>,
    observations: Arc<AtomicUsize>,
}

struct ControlledAfterPollRetryOwnerPolicy {
    owner: BoundaryRetryOwner,
    after_poll_started: Arc<Notify>,
    after_poll_release: Arc<Notify>,
    after_poll_calls: Arc<AtomicUsize>,
    observations: Arc<AtomicUsize>,
}

#[async_trait]
impl SourcePolicy for BlockingAfterPollRetryOwnerPolicy {
    fn label(&self) -> &'static str {
        "blocking_after_poll_retry_owner"
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        SourceAdmission::Admit(None)
    }

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        self.after_poll_started.notify_one();
        std::future::pending::<SourceAfterPoll>().await
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        self.observations.fetch_add(1, Ordering::SeqCst);
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        Some(self.owner.clone())
    }
}

#[async_trait]
impl SourcePolicy for ControlledAfterPollRetryOwnerPolicy {
    fn label(&self) -> &'static str {
        "controlled_after_poll_retry_owner"
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        SourceAdmission::Admit(None)
    }

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        self.after_poll_calls.fetch_add(1, Ordering::SeqCst);
        self.after_poll_started.notify_one();
        self.after_poll_release.notified().await;
        SourceAfterPoll::Proceed
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
        self.observations.fetch_add(1, Ordering::SeqCst);
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        Some(self.owner.clone())
    }
}

struct SequenceSourceExecutor {
    outcomes: VecDeque<Result<SourcePollCompletion, SourceError>>,
    ordinals: Arc<Mutex<Vec<u32>>>,
}

struct ControlledSourceExecutor {
    outcome: Option<Result<SourcePollCompletion, SourceError>>,
    ordinals: Arc<Mutex<Vec<u32>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct PendingSourceExecutor {
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

struct SourceFutureDropCounter(Arc<AtomicUsize>);

impl Drop for SourceFutureDropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl SourcePollExecutor for PendingSourceExecutor {
    fn attempt<'a>(&'a mut self, _attempt: NonZeroU32) -> SourcePollExecution<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let started = self.started.clone();
        let dropped = self.dropped.clone();
        Box::pin(async move {
            let _drop_counter = SourceFutureDropCounter(dropped);
            started.notify_one();
            std::future::pending::<SourcePollReport>().await
        })
    }
}

impl SourcePollExecutor for SequenceSourceExecutor {
    fn attempt<'a>(&'a mut self, attempt: NonZeroU32) -> SourcePollExecution<'a> {
        self.ordinals.lock().unwrap().push(attempt.get());
        let result = self
            .outcomes
            .pop_front()
            .expect("source test executor has an outcome for every attempt");
        Box::pin(async move {
            SourcePollReport {
                result,
                poll_duration: Duration::from_millis(2),
            }
        })
    }
}

impl SourcePollExecutor for ControlledSourceExecutor {
    fn attempt<'a>(&'a mut self, attempt: NonZeroU32) -> SourcePollExecution<'a> {
        self.ordinals.lock().unwrap().push(attempt.get());
        let outcome = self
            .outcome
            .take()
            .expect("controlled source executor runs exactly once");
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            SourcePollReport {
                result: outcome,
                poll_duration: Duration::from_millis(2),
            }
        })
    }
}

fn retry_boundary(
    owner: BoundaryRetryOwner,
    log: Arc<Mutex<Vec<String>>>,
    admissions: Arc<AtomicUsize>,
    guard_drops: Arc<AtomicUsize>,
    recovery_allowed: Arc<AtomicBool>,
    include_inner: bool,
) -> PerSourcePolicyBoundary {
    retry_boundary_with_observer(
        owner,
        log,
        admissions,
        guard_drops,
        recovery_allowed,
        include_inner,
        None,
    )
}

fn retry_boundary_with_observer(
    owner: BoundaryRetryOwner,
    log: Arc<Mutex<Vec<String>>>,
    admissions: Arc<AtomicUsize>,
    guard_drops: Arc<AtomicUsize>,
    recovery_allowed: Arc<AtomicBool>,
    include_inner: bool,
    observed: Option<Arc<Notify>>,
) -> PerSourcePolicyBoundary {
    let mut policies: Vec<Arc<dyn SourcePolicy>> = vec![Arc::new(RetryOwnerPolicy {
        owner,
        log: log.clone(),
        admissions,
        guard_drops,
        recovery_allowed,
        observed,
    })];
    if include_inner {
        policies.push(Arc::new(InnerOnionPolicy { log }));
    }
    PerSourcePolicyBoundary::new(
        policies,
        WriterId::from(StageId::from_ulid(Ulid::from(0x115_0001_u128))),
    )
}
