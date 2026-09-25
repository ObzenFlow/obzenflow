// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::stages::common::stage_handle::discarded_control_details;
use crate::supervised_base::cleanup::HandlerSupervisedCleanup;
use crate::supervised_base::handle::StandardHandle;
use crate::supervised_base::with_external_events::ExternalControlEvent;
use futures::FutureExt;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::{CommandDiscardDisposition, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_fsm::FsmError;
use std::error::Error;
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
    failure_path: Option<FailurePath>,
    cleanup_fails: bool,
    block_cleanup: bool,
    probe: Arc<CleanupProbe>,
}

#[derive(Clone, Copy, Debug)]
enum FailurePath {
    Transition,
    DispatchFailureTransition,
    ActionFailureTransition,
    DispatchFailureAction,
    ActionFailureAction,
}

#[derive(Clone, Debug)]
enum CleanupTestAction {
    Normal,
    Failure,
}

#[async_trait::async_trait]
impl FsmAction for CleanupTestAction {
    type Context = TestContext;

    async fn execute(&self, context: &mut Self::Context) -> Result<(), FsmError> {
        context.assert_publication_owner();
        context
            .failure_actions_executed
            .fetch_add(1, Ordering::SeqCst);
        let error = match self {
            Self::Normal => "normal action failure",
            Self::Failure => "primary failure action failure",
        };
        Err(FsmError::HandlerError(error.into()))
    }
}

impl Supervisor for CleanupSupervisor {
    type State = TestState;
    type Event = TestEvent;
    type Context = TestContext;
    type Action = CleanupTestAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        let failure_path = self.failure_path;
        fsm! {
            state: TestState;
            event: TestEvent;
            context: TestContext;
            action: CleanupTestAction;
            initial: initial_state;

            state TestState::Running {
                on TestEvent::Error => move |_state: &TestState, _event: &TestEvent, _ctx: &mut TestContext| {
                    Box::pin(async move {
                        let action = match failure_path {
                            Some(FailurePath::Transition | FailurePath::DispatchFailureTransition) => {
                                return Err(FsmError::HandlerError("primary transition failure".into()));
                            }
                            Some(FailurePath::DispatchFailureAction) => CleanupTestAction::Failure,
                            Some(FailurePath::ActionFailureTransition | FailurePath::ActionFailureAction) => CleanupTestAction::Normal,
                            None => panic!("completion-only fixture must not transition"),
                        };
                        Ok(Transition {
                            next_state: TestState::Failed("failure fixture".into()),
                            actions: vec![action],
                        })
                    })
                };
            }

            state TestState::Failed {
                on TestEvent::Error => move |state: &TestState, _event: &TestEvent, _ctx: &mut TestContext| {
                    let state = state.clone();
                    Box::pin(async move {
                        if matches!(failure_path, Some(FailurePath::ActionFailureTransition)) {
                            return Err(FsmError::HandlerError("primary transition failure".into()));
                        }
                        Ok(Transition {
                            next_state: state,
                            actions: vec![CleanupTestAction::Failure],
                        })
                    })
                };
            }
        }
    }

    fn name(&self) -> &str {
        "cleanup-supervisor"
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Transform
    }

    fn system_journal(&self, context: &Self::Context) -> Arc<dyn Journal<SystemEvent>> {
        context.system_journal.clone()
    }
}

impl ExternalEventPolicy for CleanupSupervisor {
    fn external_event_mode(_state: &Self::State) -> ExternalEventMode {
        ExternalEventMode::Poll
    }

    fn on_external_event_channel_closed(_state: &Self::State) -> Option<Self::Event> {
        None
    }
}

impl ExternalControlEvent for TestEvent {
    fn discard_details(&self) -> (CommandDiscardDisposition, Option<String>) {
        let Self::Error(error) = self;
        discarded_control_details(Some(error))
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for CleanupSupervisor {
    type Handler = ();

    async fn dispatch_state(
        &mut self,
        _state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        context.assert_publication_owner();
        self.probe.dispatches.fetch_add(1, Ordering::SeqCst);
        match self.failure_path {
            Some(FailurePath::DispatchFailureTransition | FailurePath::DispatchFailureAction) => {
                Err("dispatch failure".into())
            }
            Some(_) => Ok(EventLoopDirective::Transition(TestEvent::Error(
                "transition fixture".into(),
            ))),
            None => Ok(EventLoopDirective::Terminate),
        }
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

    async fn write_completion_event(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
    wrapped: bool,
) -> StandardHandle<TestEvent, TestState> {
    let publications = PublicationScope::new();
    let context = TestContext {
        system_journal: Arc::new(journal),
        failure_actions_executed: Arc::new(AtomicUsize::new(0)),
        publications: publications.clone(),
    };
    let (sender, receiver, watcher) = ChannelBuilder::new().build(TestState::Running);
    let task = if wrapped {
        let supervisor = HandlerSupervisedWithExternalEvents::new(
            supervisor,
            receiver,
            watcher.clone(),
            context.system_journal.clone(),
        );
        SupervisorTaskBuilder::new("cleanup-supervisor")
            .with_publications(publications)
            .spawn_handler_supervised(supervisor, TestState::Running, context)
    } else {
        SupervisorTaskBuilder::new("cleanup-supervisor")
            .with_publications(publications)
            .spawn_handler_supervised(supervisor, TestState::Running, context)
    };
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
                false,
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
        false,
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
            false,
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

#[tokio::test]
async fn fsm_and_failure_action_errors_still_reach_cleanup_through_the_canonical_runner() {
    for wrapped in [false, true] {
        for (failure_path, expected) in [
            (FailurePath::Transition, "FSM error:"),
            (
                FailurePath::DispatchFailureTransition,
                "FSM error after dispatch_state failure",
            ),
            (
                FailurePath::ActionFailureTransition,
                "FSM error after action failure:",
            ),
            (
                FailurePath::DispatchFailureAction,
                "Action error during dispatch_state failure handling:",
            ),
            (
                FailurePath::ActionFailureAction,
                "Action error during failure handling:",
            ),
        ] {
            let probe = Arc::new(CleanupProbe::default());
            let handle = spawn(
                CleanupSupervisor {
                    failure_path: Some(failure_path),
                    cleanup_fails: true,
                    probe: probe.clone(),
                    ..Default::default()
                },
                terminal_commands::TestJournal::default(),
                wrapped,
            );
            let error = timeout(Duration::from_secs(3), handle.wait_for_completion())
                .await
                .expect("early runner failure settles")
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(expected),
                "{failure_path:?}, wrapped={wrapped}: {error}"
            );
            assert!(!error.contains("secondary cleanup failure"));
            assert_eq!(probe.dispatches.load(Ordering::SeqCst), 1);
            assert_eq!(probe.completion_hooks.load(Ordering::SeqCst), 0);
            assert_eq!(probe.started.load(Ordering::SeqCst), 1);
            assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn external_event_wrapper_retains_cleanup_until_handle_completion() {
    let probe = Arc::new(CleanupProbe::default());
    let handle = spawn(
        CleanupSupervisor {
            block_cleanup: true,
            probe: probe.clone(),
            ..Default::default()
        },
        terminal_commands::TestJournal::default(),
        true,
    );
    timeout(Duration::from_secs(3), probe.entered.notified())
        .await
        .expect("inner supervisor cleanup starts");
    assert!(handle.wait_for_completion().now_or_never().is_none());
    probe.release.notify_one();
    timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("wrapped supervisor cleanup settles")
        .unwrap();
    assert_eq!(probe.completion_hooks.load(Ordering::SeqCst), 1);
    assert_eq!(probe.started.load(Ordering::SeqCst), 1);
    assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
}
