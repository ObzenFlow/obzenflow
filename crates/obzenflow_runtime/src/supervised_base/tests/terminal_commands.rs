// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::stages::common::stage_handle::STOP_REASON_USER_STOP;
use crate::supervised_base::with_external_events::record_terminal_commands;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::{
    ChainEvent, ChainPayload, CommandDiscardDisposition, JournalEvent, JournalRecord, SystemEvent,
    SystemPayload,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::{AppendOptions, Journal, JournalError, JournalReader};
use obzenflow_core::{EventId, JournalId};
use obzenflow_fsm::FsmError;
use std::error::Error;
use std::sync::Mutex;
use tokio::sync::Notify;

pub(super) struct TestJournal<T: JournalEvent = SystemEvent> {
    id: JournalId,
    pub(super) owner: Option<JournalOwner>,
    records: Mutex<Vec<JournalRecord<T::Payload>>>,
    attempts: AtomicUsize,
    first_append_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    fail: bool,
    allow_registration: bool,
}

impl<T: JournalEvent> Default for TestJournal<T> {
    fn default() -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(JournalOwner::stage(StageId::new_const(1))),
            records: Mutex::new(Vec::new()),
            attempts: AtomicUsize::new(0),
            first_append_gate: None,
            fail: false,
            allow_registration: false,
        }
    }
}

impl<T: JournalEvent> TestJournal<T> {
    pub(super) fn with_owner(mut self, owner: JournalOwner) -> Self {
        self.owner = Some(owner);
        self
    }
    pub(super) fn failing_registration() -> Self {
        Self {
            fail: true,
            ..Default::default()
        }
    }

    pub(super) fn assert_registered(&self) {
        assert!(
            self.records.lock().unwrap().first().is_some_and(|record| {
                matches!(
                    record.event_type_name(),
                    "execution.supervisor.registered" | "system.supervisor.registered"
                )
            }),
            "registration must precede FSM dispatch and actions"
        );
    }
}

#[tokio::test]
async fn self_supervised_runner_does_not_invent_registration_actions() {
    let journal = Arc::new(
        TestJournal::failing_registration()
            .with_owner(JournalOwner::system(SystemId::new_const(1))),
    );
    let publications = PublicationScope::new();
    let actions = Arc::new(AtomicUsize::new(0));
    let completions = Arc::new(AtomicUsize::new(0));
    let context = TestContext {
        system_journal: journal.clone(),
        failure_actions_executed: actions.clone(),
        publications: publications.clone(),
    };
    let (sender, _receiver, watcher) =
        ChannelBuilder::<TestEvent, TestState>::new().build(TestState::Running);
    let task = SupervisorTaskBuilder::new("runtime")
        .with_publications(publications)
        .spawn_self_supervised(
            TestSelfSupervisor {
                name: "runtime".into(),
            },
            TestState::Running,
            context,
        );
    let handle = HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
    handle.wait_for_completion().await.unwrap();
    assert_eq!(journal.attempts.load(Ordering::SeqCst), 0);
    assert_eq!(actions.load(Ordering::SeqCst), 1);
    assert_eq!(completions.load(Ordering::SeqCst), 0);
    assert!(journal.records.lock().unwrap().is_empty());
}

#[async_trait::async_trait]
impl<T: JournalEvent + 'static> obzenflow_core::journal::JournalStorage<T> for TestJournal<T> {
    fn storage_id(&self) -> &JournalId {
        &self.id
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn storage_append(
        &self,
        event: T,
        mut options: AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        let event = options.capture.prepare(0, event);
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            if let Some((entered, release)) = &self.first_append_gate {
                entered.notify_one();
                release.notified().await;
            }
        }
        if self.fail
            && !(self.allow_registration
                && matches!(
                    event.event_type_name(),
                    "execution.supervisor.registered" | "system.supervisor.registered"
                ))
        {
            return Err(JournalError::Full);
        }
        let mut records = self.records.lock().unwrap();
        let record = crate::testing::causal_fixture::commit(self.id, event, &options, &records)?;
        records.push(record.clone());
        Ok(record)
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        Ok(self.records.lock().unwrap().clone())
    }

    async fn storage_read_event(
        &self,
        id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.id() == id)
            .cloned())
    }

    async fn storage_reader_from(
        &self,
        _position: u64,
    ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        unreachable!("terminal mailbox tests read committed records directly")
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(count)
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn terminal_mailbox_records_each_command_once_and_rejects_later_sends() {
    for state in [
        ExternalEventTestState::Drained,
        ExternalEventTestState::Failed("first failure".into()),
    ] {
        let journal = Arc::new(TestJournal::default());
        let (sender, receiver, watcher) = ChannelBuilder::new()
            .with_event_buffer(3)
            .build(state.clone());
        sender
            .send(ExternalEventTestEvent::Initialize)
            .await
            .unwrap();
        sender
            .send(ExternalEventTestEvent::Error("late failure".into()))
            .await
            .unwrap();
        sender
            .send(ExternalEventTestEvent::Error(STOP_REASON_USER_STOP.into()))
            .await
            .unwrap();

        let mut pending_send =
            tokio_test::task::spawn(sender.send(ExternalEventTestEvent::Initialize));
        tokio_test::assert_pending!(pending_send.poll());
        let stage_id = StageId::new_const(1);
        let inner = ExternalEventTestHandlerSupervisor {
            name: "terminal-worker".into(),
            dispatch_calls: Arc::new(AtomicUsize::new(0)),
            stage_id,
        };
        let mut supervisor = HandlerSupervisedWithExternalEvents::new(
            inner,
            receiver,
            watcher,
            crate::supervised_base::with_external_events::stage_commands(
                journal.clone(),
                obzenflow_core::event::provenance::FlowContext::new(
                    "terminal-worker",
                    StageId::new_const(1),
                ),
            ),
        );
        let mut context = ExternalEventTestContext;
        supervisor
            .dispatch_state(&state, &mut context)
            .await
            .unwrap();
        assert!(tokio_test::assert_ready!(pending_send.poll()).is_err());
        assert!(sender
            .send(ExternalEventTestEvent::Initialize)
            .await
            .is_err());
        supervisor
            .dispatch_state(&state, &mut context)
            .await
            .unwrap();

        let records = journal.read_all_unordered().await.unwrap();
        assert_eq!(records.len(), 3);
        let expected = [
            (
                "Initialize",
                CommandDiscardDisposition::ObsoleteControl,
                None,
            ),
            (
                "Error",
                CommandDiscardDisposition::UnexpectedError,
                Some("late failure"),
            ),
            (
                "Error",
                CommandDiscardDisposition::ObsoleteControl,
                Some("user_stop"),
            ),
        ];
        for (record, (name, expected_disposition, expected_error)) in records.iter().zip(expected) {
            assert_eq!(*record.writer_id(), WriterId::from(stage_id));
            assert_eq!(
                record.envelope.provenance.event.event_type,
                "execution.supervisor.command_discarded"
            );
            let ChainPayload::Execution(ExecutionPayload::SupervisorCommandDiscarded {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            }) = &record.payload
            else {
                panic!("expected a command disposition fact");
            };
            assert_eq!(supervisor, "terminal-worker");
            assert_eq!(terminal_state, state.variant_name());
            assert_eq!(command, name);
            assert_eq!(*disposition, expected_disposition);
            assert_eq!(error.as_deref(), expected_error);
        }
    }
}

#[tokio::test]
async fn terminal_mailbox_retains_the_whole_queue_when_recording_waiter_is_cancelled() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let journal = Arc::new(TestJournal {
        first_append_gate: Some((entered.clone(), release.clone())),
        ..Default::default()
    });
    let (sender, mut receiver, _) = ChannelBuilder::new().build(ExternalEventTestState::Drained);
    for event in [
        ExternalEventTestEvent::Initialize,
        ExternalEventTestEvent::Error("late failure".into()),
    ] {
        sender.send(event).await.unwrap();
    }
    let scope = PublicationScope::new();
    let owner = scope.clone();
    let target = journal.clone();
    let waiter = tokio::spawn(async move {
        owner
            .enter(record_terminal_commands(
                &mut receiver,
                crate::supervised_base::with_external_events::stage_commands(
                    target,
                    obzenflow_core::event::provenance::FlowContext::new(
                        "terminal-worker",
                        StageId::new_const(1),
                    ),
                ),
                "worker",
                "Drained",
            ))
            .await
    });
    entered.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(sender
        .send(ExternalEventTestEvent::Initialize)
        .await
        .is_err());
    assert!(journal.read_all_unordered().await.unwrap().is_empty());
    release.notify_one();
    scope.join().await.unwrap();
    assert_eq!(journal.read_all_unordered().await.unwrap().len(), 2);
}

struct CompletionContext {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    fail_action: bool,
    journal: Arc<TestJournal<ChainEvent>>,
}
impl FsmContext for CompletionContext {}

#[derive(Clone, Debug, PartialEq, StateVariant)]
enum CompletionState {
    Created,
    Initializing,
    Finalising,
    Failing(String),
    Drained,
    Failed(String),
}
#[derive(Clone, Debug, EventVariant)]
enum CompletionEvent {
    Initialize,
    Initialized,
    Settled,
    Error(String),
}
#[derive(Clone, Debug)]
enum CompletionAction {
    Host(crate::supervised_base::handler_supervised::SupervisorAction<CompletionEvent>),
    Work,
}
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, SupervisorAction,
};

#[async_trait::async_trait]
impl FsmAction for CompletionAction {
    type Context = CompletionContext;
    async fn execute(&self, _: &mut CompletionContext) -> Result<(), FsmError> {
        unreachable!("completion work is an owned operation")
    }
}

struct CompletionSupervisor;
fn completion_failure<'a>(
    _: &'a CompletionState,
    event: &'a CompletionEvent,
    _: &'a mut CompletionContext,
) -> futures::future::BoxFuture<'a, Result<Transition<CompletionState, CompletionAction>, FsmError>>
{
    let CompletionEvent::Error(error) = event else {
        unreachable!()
    };
    let cause = error.clone();
    Box::pin(async move {
        Ok(Transition {
            next_state: CompletionState::Failing(cause),
            actions: vec![
                CompletionAction::Host(SupervisorAction::CloseMailbox),
                CompletionAction::Host(SupervisorAction::SettlePublications),
                CompletionAction::Host(SupervisorAction::Emit(CompletionEvent::Settled)),
            ],
        })
    })
}
impl Supervisor for CompletionSupervisor {
    type State = CompletionState;
    type Event = CompletionEvent;
    type Context = CompletionContext;
    type Action = CompletionAction;
    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state: CompletionState; event: CompletionEvent; context: CompletionContext; action: CompletionAction; initial: initial_state;
            state CompletionState::Created {
                on CompletionEvent::Initialize => |_: &CompletionState, _: &CompletionEvent, _: &mut CompletionContext| {
                    Box::pin(async { Ok(Transition { next_state: CompletionState::Initializing, actions: vec![CompletionAction::Host(SupervisorAction::Register), CompletionAction::Host(SupervisorAction::Emit(CompletionEvent::Initialized))] }) })
                };
            }
            state CompletionState::Initializing {
                on CompletionEvent::Initialized => |_: &CompletionState, _: &CompletionEvent, _: &mut CompletionContext| {
                    Box::pin(async { Ok(Transition { next_state: CompletionState::Finalising, actions: vec![CompletionAction::Work, CompletionAction::Host(SupervisorAction::CloseMailbox), CompletionAction::Host(SupervisorAction::SettlePublications), CompletionAction::Host(SupervisorAction::Emit(CompletionEvent::Settled))] }) })
                };
                on CompletionEvent::Error => completion_failure;
            }
            state CompletionState::Finalising {
                on CompletionEvent::Settled => |_: &CompletionState, _: &CompletionEvent, _: &mut CompletionContext| {
                    Box::pin(async { Ok(Transition { next_state: CompletionState::Drained, actions: vec![] }) })
                };
                on CompletionEvent::Error => completion_failure;
            }
            state CompletionState::Failing {
                on CompletionEvent::Error => |state: &CompletionState, _: &CompletionEvent, _: &mut CompletionContext| {
                    let state = state.clone();
                    Box::pin(async { Ok(Transition { next_state: state, actions: vec![] }) })
                };
                on CompletionEvent::Settled => |state: &CompletionState, _: &CompletionEvent, _: &mut CompletionContext| {
                    let CompletionState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: CompletionState::Failed(cause), actions: vec![] }) })
                };
            }
            state CompletionState::Drained {}
            state CompletionState::Failed {}
        }
    }
    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Transform
    }
    fn registration(
        &self,
        context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        let journal: Arc<dyn Journal<ChainEvent>> = context.journal.clone();
        crate::supervised_base::base::register_stage(
            journal,
            obzenflow_core::event::provenance::FlowContext::new(
                "terminal-worker",
                StageId::new_const(1),
            ),
            descriptor,
        )
    }
    fn name(&self) -> &str {
        "completion-worker"
    }
}
impl crate::supervised_base::with_external_events::ExternalControlEvent for CompletionEvent {
    fn discard_details(&self) -> (CommandDiscardDisposition, Option<String>) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(cause) => Some(cause),
            _ => None,
        })
    }
}
impl ExternalEventPolicy for CompletionSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, CompletionState::Created) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }
    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        !matches!(state, CompletionState::Created) && matches!(event, CompletionEvent::Initialize)
    }
    fn on_external_event_channel_closed(_: &Self::State) -> Option<Self::Event> {
        None
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for CompletionSupervisor {
    type Handler = ();
    fn lifecycle_phase(&self, state: &CompletionState) -> LifecyclePhase {
        match state {
            CompletionState::Created => LifecyclePhase::Other,
            CompletionState::Initializing => LifecyclePhase::Initializing,
            CompletionState::Finalising => LifecyclePhase::Finalising,
            CompletionState::Failing(cause) => LifecyclePhase::Failing(cause.clone()),
            CompletionState::Drained => LifecyclePhase::Completed,
            CompletionState::Failed(cause) => LifecyclePhase::Failed(cause.clone()),
        }
    }
    fn supervisor_action(
        &self,
        action: &CompletionAction,
    ) -> Option<SupervisorAction<CompletionEvent>> {
        match action {
            CompletionAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }
    async fn execute_action(
        &mut self,
        _: CompletionAction,
        context: &mut CompletionContext,
    ) -> Result<ActionExecution<CompletionContext, CompletionEvent>, FsmError> {
        let entered = context.entered.clone();
        let release = context.release.clone();
        let fail = context.fail_action;
        Ok(ActionExecution::Pending(Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            Box::new(move |_: &mut CompletionContext| {
                if fail {
                    Err(FsmError::HandlerError("terminal action failed".into()).into())
                } else {
                    Ok(None)
                }
            }) as ActionCompletion<CompletionContext, CompletionEvent>
        })))
    }
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        _: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        assert!(matches!(
            state,
            CompletionState::Drained | CompletionState::Failed(_)
        ));
        Ok(EventLoopDirective::Terminate)
    }
    fn writer_id(&self) -> WriterId {
        StageId::new_const(1).into()
    }

    fn event_for_action_error(&self, error: String) -> CompletionEvent {
        CompletionEvent::Error(error)
    }
}

fn spawn_completion(
    context: CompletionContext,
) -> crate::supervised_base::handle::StandardHandle<CompletionEvent, CompletionState> {
    let (sender, receiver, watcher) = ChannelBuilder::new().build(CompletionState::Created);
    let supervisor = HandlerSupervisedWithExternalEvents::new(
        CompletionSupervisor,
        receiver,
        watcher.clone(),
        crate::supervised_base::with_external_events::stage_commands(
            context.journal.clone(),
            obzenflow_core::event::provenance::FlowContext::new(
                "terminal-worker",
                StageId::new_const(1),
            ),
        ),
    );
    let task = SupervisorTaskBuilder::new("completion-worker").spawn_handler_supervised(
        supervisor,
        CompletionState::Created,
        context,
    );
    HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap()
}

#[tokio::test]
async fn terminal_mailbox_preserves_commands_and_failure_during_owned_completion_work() {
    for fail_action in [false, true] {
        let journal = Arc::new(TestJournal::default());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handle = spawn_completion(CompletionContext {
            entered: entered.clone(),
            release: release.clone(),
            fail_action,
            journal: journal.clone(),
        });
        handle
            .send_event(CompletionEvent::Initialize)
            .await
            .unwrap();
        entered.notified().await;
        assert_eq!(handle.current_state(), CompletionState::Finalising);
        handle
            .send_event(CompletionEvent::Initialize)
            .await
            .unwrap();
        handle
            .send_event(CompletionEvent::Error("late external failure".into()))
            .await
            .unwrap();
        let failure =
            tokio::time::timeout(std::time::Duration::from_secs(3), handle.wait_for_failure())
                .await
                .unwrap()
                .unwrap();
        assert!(failure.cause.to_string().contains("late external failure"));
        assert!(matches!(
            handle.current_state(),
            CompletionState::Failing(_)
        ));
        release.notify_one();
        let error = handle.wait_for_completion().await.unwrap_err();
        assert!(
            error.to_string().contains("late external failure"),
            "{error}"
        );
        let records = journal.read_all_unordered().await.unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(
            &records[0].payload,
            ChainPayload::Execution(ExecutionPayload::SupervisorRegistered { .. })
        ));
        assert!(
            matches!(&records[1].payload, ChainPayload::Execution(ExecutionPayload::SupervisorCommandDiscarded { command, disposition: CommandDiscardDisposition::ObsoleteControl, .. }) if command == "Initialize")
        );
    }
}

#[tokio::test]
async fn terminal_mailbox_journal_failure_is_retained_by_supervisor_join() {
    let journal = Arc::new(TestJournal {
        fail: true,
        allow_registration: true,
        ..Default::default()
    });
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handle = spawn_completion(CompletionContext {
        entered: entered.clone(),
        release: release.clone(),
        fail_action: false,
        journal: journal.clone(),
    });
    handle
        .send_event(CompletionEvent::Initialize)
        .await
        .unwrap();
    entered.notified().await;
    handle
        .send_event(CompletionEvent::Initialize)
        .await
        .unwrap();
    release.notify_one();
    let error = handle.wait_for_completion().await.unwrap_err();
    assert!(error.to_string().contains("Journal is full"), "{error}");
    assert_eq!(
        journal.attempts.load(Ordering::SeqCst),
        2,
        "failed appends must not be retried"
    );
    let records = journal.read_all_unordered().await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(
        &records[0].payload,
        ChainPayload::Execution(ExecutionPayload::SupervisorRegistered { .. })
    ));
}

#[tokio::test]
async fn deferred_commands_keep_distinct_payloads_capacity_and_admission_causality() {
    use crate::supervised_base::publication::{capture, with_snapshot};
    use crate::supervised_base::with_external_events::CommandMailbox;
    use obzenflow_core::event::CausalFrontier;

    #[derive(Debug, PartialEq)]
    enum Command {
        Value(u64),
    }

    let journal = TestJournal::default();
    let event = || {
        SystemEvent::new(
            SystemId::new().into(),
            SystemPayload::PipelineLifecycle(
                obzenflow_core::event::payloads::system_payload::PipelineLifecycleEvent::Starting,
            ),
        )
    };
    let first = journal.append(event(), Default::default()).await.unwrap();
    let second = journal.append(event(), Default::default()).await.unwrap();
    let first_frontier = CausalFrontier::from_record(&first).unwrap();
    let second_frontier = CausalFrontier::from_record(&second).unwrap();
    let (sender, receiver, _) = ChannelBuilder::<Command, ()>::new()
        .with_event_buffer(2)
        .build(());
    with_snapshot(first_frontier.clone(), async {
        sender.send(Command::Value(1)).await
    })
    .await
    .unwrap();
    with_snapshot(second_frontier.clone(), async {
        sender.send(Command::Value(2)).await
    })
    .await
    .unwrap();
    let mut mailbox: CommandMailbox<_> = receiver.into();
    with_snapshot(CausalFrontier::default(), async {
        let mut deferred = tokio_test::task::spawn(mailbox.recv(|_| false));
        tokio_test::assert_pending!(deferred.poll());
        drop(deferred);
        assert_eq!(
            capture(),
            CausalFrontier::default(),
            "deferral is not causal admission"
        );
        let mut third = tokio_test::task::spawn(sender.send(Command::Value(3)));
        tokio_test::assert_pending!(third.poll());
        assert_eq!(mailbox.recv(|_| true).await, Some(Command::Value(1)));
        assert_eq!(capture(), first_frontier);
        tokio_test::assert_ready!(third.poll()).unwrap();
        assert_eq!(mailbox.recv(|_| true).await, Some(Command::Value(2)));
        assert_eq!(capture(), second_frontier);
        assert_eq!(mailbox.recv(|_| true).await, Some(Command::Value(3)));
    })
    .await;
}
