// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::supervised_base::handle::StandardHandle;
use crate::supervised_base::handler_supervised::HandlerSupervisedCleanup;
use futures::FutureExt;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

#[derive(Default)]
struct CleanupProbe {
    dispatches: AtomicUsize,
    completion_hooks: AtomicUsize,
    started: AtomicUsize,
    finished: AtomicUsize,
    entered: Notify,
    release: Notify,
}

#[derive(Default)]
struct CleanupSupervisor {
    stage_id: StageId,
    runner_fails: bool,
    cleanup_fails: bool,
    block_cleanup: bool,
    probe: Arc<CleanupProbe>,
}

impl Supervisor for CleanupSupervisor {
    type State = TestState;
    type Event = TestEvent;
    type Context = TestContext;
    type Action = TestAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        build_test_machine(initial_state)
    }

    fn name(&self) -> &str {
        "cleanup-supervisor"
    }

    fn supervisor_kind(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind {
        obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind::Transform
    }

    fn system_journal(
        &self,
        context: &Self::Context,
    ) -> Arc<dyn obzenflow_core::journal::Journal<obzenflow_core::event::SystemEvent>> {
        context.system_journal.clone()
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for CleanupSupervisor {
    type Handler = ();

    async fn dispatch_state(
        &mut self,
        _state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        context.assert_publication_owner();
        self.probe.dispatches.fetch_add(1, Ordering::SeqCst);
        Ok(EventLoopDirective::Terminate)
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        TestEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.probe.completion_hooks.fetch_add(1, Ordering::SeqCst);
        if self.runner_fails {
            return Err("primary runner failure".into());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl HandlerSupervisedCleanup for CleanupSupervisor {
    async fn cleanup_after_run(
        &mut self,
        context: &Self::Context,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let current = PublicationScope::current().expect("cleanup retains publication ownership");
        assert!(Arc::ptr_eq(&context.publications, &current));
        self.probe.started.fetch_add(1, Ordering::SeqCst);
        self.probe.entered.notify_one();
        if self.block_cleanup {
            self.probe.release.notified().await;
        }
        self.probe.finished.fetch_add(1, Ordering::SeqCst);
        if self.cleanup_fails {
            return Err("secondary cleanup failure".into());
        }
        Ok(())
    }
}

fn spawn(
    supervisor: CleanupSupervisor,
    journal: terminal_commands::TestJournal,
) -> StandardHandle<TestEvent, TestState> {
    let publications = PublicationScope::new();
    let context = TestContext {
        system_journal: Arc::new(journal),
        failure_actions_executed: Arc::new(AtomicUsize::new(0)),
        publications: publications.clone(),
    };
    let (sender, _receiver, watcher) = ChannelBuilder::new().build(TestState::Running);
    let task = SupervisorTaskBuilder::new("cleanup-supervisor")
        .with_publications(publications)
        .spawn_handler_supervised_with_cleanup(supervisor, TestState::Running, context);
    HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap()
}

#[tokio::test]
async fn cleanup_runs_once_and_preserves_runner_error_precedence() {
    for runner_fails in [false, true] {
        for cleanup_fails in [false, true] {
            let probe = Arc::new(CleanupProbe::default());
            let handle = spawn(
                CleanupSupervisor {
                    runner_fails,
                    cleanup_fails,
                    probe: probe.clone(),
                    ..Default::default()
                },
                terminal_commands::TestJournal::default(),
            );
            let result = timeout(Duration::from_secs(3), handle.wait_for_completion())
                .await
                .expect("cleanup settles");
            match (runner_fails, cleanup_fails) {
                (true, _) => assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("primary runner failure")),
                (false, true) => assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("secondary cleanup failure")),
                (false, false) => result.unwrap(),
            }
            assert_eq!(probe.dispatches.load(Ordering::SeqCst), 1);
            assert_eq!(probe.completion_hooks.load(Ordering::SeqCst), 1);
            assert_eq!(probe.started.load(Ordering::SeqCst), 1);
            assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn registration_failure_still_reaches_cleanup_with_its_publication_owner() {
    let probe = Arc::new(CleanupProbe::default());
    let handle = spawn(
        CleanupSupervisor {
            cleanup_fails: true,
            probe: probe.clone(),
            ..Default::default()
        },
        terminal_commands::TestJournal::failing_registration(),
    );
    let failure = timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("failed registration settles")
        .unwrap_err();
    assert!(failure.to_string().contains("Journal is full"));
    assert_eq!(probe.dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(probe.completion_hooks.load(Ordering::SeqCst), 0);
    assert_eq!(probe.started.load(Ordering::SeqCst), 1);
    assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn handle_completion_waits_for_cleanup_after_success_or_failure() {
    for runner_fails in [false, true] {
        let probe = Arc::new(CleanupProbe::default());
        let handle = spawn(
            CleanupSupervisor {
                runner_fails,
                block_cleanup: true,
                probe: probe.clone(),
                ..Default::default()
            },
            terminal_commands::TestJournal::default(),
        );
        timeout(Duration::from_secs(3), probe.entered.notified())
            .await
            .expect("cleanup starts");
        assert_eq!(probe.completion_hooks.load(Ordering::SeqCst), 1);
        assert_eq!(probe.finished.load(Ordering::SeqCst), 0);
        assert!(handle.wait_for_completion().now_or_never().is_none());

        probe.release.notify_one();
        let result = timeout(Duration::from_secs(3), handle.wait_for_completion())
            .await
            .expect("handle joins after cleanup");
        assert_eq!(result.is_err(), runner_fails);
        assert_eq!(probe.started.load(Ordering::SeqCst), 1);
        assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
    }
}
