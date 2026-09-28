// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::supervised_base::handle::StandardHandle;
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, SupervisorAction,
};
use crate::supervised_base::with_external_events::ExternalControlEvent;
use futures::FutureExt;
use obzenflow_core::event::CommandDiscardDisposition;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

#[derive(Default)]
struct CleanupProbe {
    work: AtomicUsize,
    started: AtomicUsize,
    finished: AtomicUsize,
    dropped: AtomicUsize,
    entered: Notify,
    release: Notify,
}

#[derive(Clone, Debug, PartialEq, StateVariant)]
enum State {
    Created,
    Initializing,
    Running,
    Finalising,
    Failing(String),
    Completed,
    Failed(String),
}
#[derive(Clone, Debug, EventVariant)]
enum Event {
    Initialize,
    Initialized,
    Finish,
    Settled,
    Error(String),
}
#[derive(Clone, Debug)]
enum Action {
    Host(SupervisorAction<Event>),
    Work,
}

struct Context {
    journal: Arc<terminal_commands::TestJournal<obzenflow_core::ChainEvent>>,
    publications: Arc<PublicationScope>,
    probe: Arc<CleanupProbe>,
    work_fails: bool,
}
impl FsmContext for Context {}
impl Drop for Context {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl FsmAction for Action {
    type Context = Context;
    async fn execute(&self, context: &mut Context) -> Result<(), FsmError> {
        let Self::Work = self else {
            unreachable!("host operation")
        };
        context.journal.assert_registered();
        let current = PublicationScope::current().expect("publication owner");
        assert!(Arc::ptr_eq(&context.publications, &current));
        context.probe.work.fetch_add(1, Ordering::SeqCst);
        if context.work_fails {
            return Err(FsmError::HandlerError("primary work failure".into()));
        }
        Ok(())
    }
}

#[derive(Default)]
struct CleanupSupervisor {
    stage_id: StageId,
    work_fails: bool,
    cleanup_fails: bool,
    block_cleanup: bool,
    probe: Arc<CleanupProbe>,
}

fn failing<'a>(
    state: &'a State,
    event: &'a Event,
    _: &'a mut Context,
) -> futures::future::BoxFuture<'a, Result<Transition<State, Action>, FsmError>> {
    let Event::Error(cause) = event else {
        unreachable!()
    };
    let cause = cause.clone();
    // Finalising has already selected cleanup. Its failed result must not repeat it.
    let actions = if matches!(state, State::Finalising) {
        vec![
            Action::Host(SupervisorAction::SettlePublications),
            Action::Host(SupervisorAction::Emit(Event::Settled)),
        ]
    } else {
        vec![
            Action::Host(SupervisorAction::Cleanup),
            Action::Host(SupervisorAction::SettlePublications),
            Action::Host(SupervisorAction::Emit(Event::Settled)),
        ]
    };
    Box::pin(async move {
        Ok(Transition {
            next_state: State::Failing(cause),
            actions,
        })
    })
}

impl Supervisor for CleanupSupervisor {
    type State = State;
    type Event = Event;
    type Context = Context;
    type Action = Action;
    fn build_state_machine(
        &self,
        initial_state: State,
    ) -> StateMachine<State, Event, Context, Action> {
        fsm! {
            state: State; event: Event; context: Context; action: Action; initial: initial_state;
            state State::Created {
                on Event::Initialize => |_: &State, _: &Event, _: &mut Context| {
                    Box::pin(async { Ok(Transition { next_state: State::Initializing, actions: vec![Action::Host(SupervisorAction::Register), Action::Host(SupervisorAction::Emit(Event::Initialized))] }) })
                };
            }
            state State::Initializing {
                on Event::Initialized => |_: &State, _: &Event, _: &mut Context| {
                    Box::pin(async { Ok(Transition { next_state: State::Running, actions: vec![Action::Work, Action::Host(SupervisorAction::Emit(Event::Finish))] }) })
                };
                on Event::Error => failing;
            }
            state State::Running {
                on Event::Finish => |_: &State, _: &Event, _: &mut Context| {
                    Box::pin(async { Ok(Transition { next_state: State::Finalising, actions: vec![Action::Host(SupervisorAction::Cleanup), Action::Host(SupervisorAction::SettlePublications), Action::Host(SupervisorAction::Emit(Event::Settled))] }) })
                };
                on Event::Error => failing;
            }
            state State::Finalising {
                on Event::Settled => |_: &State, _: &Event, _: &mut Context| {
                    Box::pin(async { Ok(Transition { next_state: State::Completed, actions: vec![] }) })
                };
                on Event::Error => failing;
            }
            state State::Failing {
                on Event::Error => |state: &State, _: &Event, _: &mut Context| {
                    let state = state.clone();
                    Box::pin(async move { Ok(Transition { next_state: state, actions: vec![] }) })
                };
                on Event::Settled => |state: &State, _: &Event, _: &mut Context| {
                    let State::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: State::Failed(cause), actions: vec![] }) })
                };
            }
            state State::Completed {}
            state State::Failed {}
        }
    }
    fn name(&self) -> &str {
        "cleanup-supervisor"
    }
    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Transform
    }
    fn registration(
        &self,
        context: &Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        let journal: Arc<dyn obzenflow_core::Journal<obzenflow_core::ChainEvent>> =
            context.journal.clone();
        crate::supervised_base::base::register_stage(
            journal,
            obzenflow_core::event::provenance::FlowContext::new("cleanup", self.stage_id),
            descriptor,
        )
    }
}

impl ExternalEventPolicy for CleanupSupervisor {
    fn external_event_mode(_: &State) -> ExternalEventMode {
        ExternalEventMode::Poll
    }
    fn on_external_event_channel_closed(_: &State) -> Option<Event> {
        None
    }
}
impl ExternalControlEvent for Event {
    fn discard_details(&self) -> (CommandDiscardDisposition, Option<String>) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(cause) => Some(cause),
            _ => None,
        })
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for CleanupSupervisor {
    type Handler = ();
    fn lifecycle_phase(&self, state: &State) -> LifecyclePhase {
        match state {
            State::Initializing => LifecyclePhase::Initializing,
            State::Running => LifecyclePhase::Active,
            State::Finalising => LifecyclePhase::Finalising,
            State::Failing(cause) => LifecyclePhase::Failing(cause.clone()),
            State::Failed(cause) => LifecyclePhase::Failed(cause.clone()),
            State::Completed => LifecyclePhase::Completed,
            State::Created => LifecyclePhase::Other,
        }
    }
    fn supervisor_action(&self, action: &Action) -> Option<SupervisorAction<Event>> {
        match action {
            Action::Host(action) => Some(action.clone()),
            _ => None,
        }
    }
    async fn dispatch_state(
        &mut self,
        state: &State,
        _: &mut Context,
    ) -> Result<EventLoopDirective<Event>, Box<dyn Error + Send + Sync>> {
        Ok(match state {
            State::Created => EventLoopDirective::Transition(Event::Initialize),
            State::Completed | State::Failed(_) => EventLoopDirective::Terminate,
            _ => panic!("pending states must have an operation"),
        })
    }
    async fn execute_cleanup(
        &mut self,
        context: &Context,
    ) -> Result<ActionExecution<Context, Event>, FsmError> {
        let current = PublicationScope::current().expect("cleanup retains publication ownership");
        assert!(Arc::ptr_eq(&context.publications, &current));
        let probe = self.probe.clone();
        let block = self.block_cleanup;
        let fail = self.cleanup_fails;
        Ok(ActionExecution::Pending(Box::pin(async move {
            probe.started.fetch_add(1, Ordering::SeqCst);
            probe.entered.notify_one();
            if block {
                probe.release.notified().await;
            }
            probe.finished.fetch_add(1, Ordering::SeqCst);
            Box::new(move |_: &mut Context| {
                if fail {
                    Err(FsmError::HandlerError("secondary cleanup failure".into()))
                } else {
                    Ok(None)
                }
            }) as ActionCompletion<Context, Event>
        })))
    }
    fn writer_id(&self) -> WriterId {
        self.stage_id.into()
    }

    fn event_for_action_error(&self, message: String) -> Event {
        Event::Error(message)
    }
}

fn spawn(
    supervisor: CleanupSupervisor,
    mut journal: terminal_commands::TestJournal<obzenflow_core::ChainEvent>,
    wrapped: bool,
) -> StandardHandle<Event, State> {
    journal.owner = Some(obzenflow_core::JournalOwner::stage(supervisor.stage_id));
    let stage_id = supervisor.stage_id;
    let publications = PublicationScope::new();
    let context = Context {
        journal: Arc::new(journal),
        publications: publications.clone(),
        probe: supervisor.probe.clone(),
        work_fails: supervisor.work_fails,
    };
    let (sender, receiver, watcher) = ChannelBuilder::new().build(State::Created);
    let task = if wrapped {
        let supervisor = HandlerSupervisedWithExternalEvents::new(
            supervisor,
            receiver,
            watcher.clone(),
            crate::supervised_base::with_external_events::stage_commands(
                context.journal.clone(),
                obzenflow_core::event::provenance::FlowContext::new("cleanup", stage_id),
            ),
        );
        SupervisorTaskBuilder::new("cleanup-supervisor")
            .with_publications(publications)
            .spawn_handler_supervised(supervisor, State::Created, context)
    } else {
        SupervisorTaskBuilder::new("cleanup-supervisor")
            .with_publications(publications)
            .spawn_handler_supervised(supervisor, State::Created, context)
    };
    HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap()
}

#[tokio::test]
async fn fsm_selected_cleanup_runs_once_and_preserves_the_original_failure() {
    for wrapped in [false, true] {
        for work_fails in [false, true] {
            for cleanup_fails in [false, true] {
                let probe = Arc::new(CleanupProbe::default());
                let handle = spawn(
                    CleanupSupervisor {
                        work_fails,
                        cleanup_fails,
                        probe: probe.clone(),
                        ..Default::default()
                    },
                    terminal_commands::TestJournal::default(),
                    wrapped,
                );
                let result = timeout(Duration::from_secs(3), handle.wait_for_completion())
                    .await
                    .expect("cleanup settles");
                match (work_fails, cleanup_fails) {
                    (true, _) => assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("primary work failure")),
                    (false, true) => assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("secondary cleanup failure")),
                    (false, false) => result.unwrap(),
                }
                assert_eq!(probe.work.load(Ordering::SeqCst), 1);
                assert_eq!(probe.started.load(Ordering::SeqCst), 1);
                assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
                assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
            }
        }
    }
}

#[tokio::test]
async fn registration_failure_selects_cleanup_with_its_publication_owner() {
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
    assert!(failure.to_string().contains("Journal is full"), "{failure}");
    assert_eq!(probe.work.load(Ordering::SeqCst), 0);
    assert_eq!(probe.started.load(Ordering::SeqCst), 1);
    assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failure_is_observable_while_owned_cleanup_and_physical_completion_are_pending() {
    for wrapped in [false, true] {
        for work_fails in [false, true] {
            let probe = Arc::new(CleanupProbe::default());
            let handle = spawn(
                CleanupSupervisor {
                    work_fails,
                    block_cleanup: true,
                    probe: probe.clone(),
                    ..Default::default()
                },
                terminal_commands::TestJournal::default(),
                wrapped,
            );
            timeout(Duration::from_secs(3), probe.entered.notified())
                .await
                .expect("cleanup starts");
            assert_eq!(probe.finished.load(Ordering::SeqCst), 0);
            assert!(handle.wait_for_completion().now_or_never().is_none());
            if work_fails {
                assert!(handle
                    .wait_for_failure()
                    .now_or_never()
                    .flatten()
                    .unwrap()
                    .cause
                    .to_string()
                    .contains("primary work failure"));
            }
            probe.release.notify_one();
            let result = timeout(Duration::from_secs(3), handle.wait_for_completion())
                .await
                .expect("handle joins after cleanup");
            assert_eq!(result.is_err(), work_fails);
            assert_eq!(probe.started.load(Ordering::SeqCst), 1);
            assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
            assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
        }
    }
}
