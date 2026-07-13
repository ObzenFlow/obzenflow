// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[tokio::test]
async fn source_boundary_composes_forward_admission_and_single_reverse_settlement() {
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
            "after:b",
            "observe:b",
            "after:a",
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
