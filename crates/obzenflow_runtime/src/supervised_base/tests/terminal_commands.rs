// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::stages::common::stage_handle::STOP_REASON_USER_STOP;
use crate::supervised_base::cleanup::HandlerSupervisedCleanup;
use crate::supervised_base::with_external_events::record_terminal_commands;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::{CommandDiscardDisposition, JournalRecord, SystemEvent, SystemPayload};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::{AppendOptions, Journal, JournalError, JournalReader};
use obzenflow_core::{EventId, JournalId};
use obzenflow_fsm::FsmError;
use std::error::Error;
use std::sync::Mutex;
use tokio::sync::Notify;

pub(super) struct TestJournal {
    id: JournalId,
    pub(super) owner: Option<JournalOwner>,
    records: Mutex<Vec<JournalRecord<SystemPayload>>>,
    attempts: AtomicUsize,
    first_append_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    fail: bool,
    allow_registration: bool,
}

impl Default for TestJournal {
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

impl TestJournal {
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
            matches!(
                self.records
                    .lock()
                    .unwrap()
                    .first()
                    .map(|record| &record.payload),
                Some(SystemPayload::SupervisorRegistered { .. })
            ),
            "registration must precede FSM dispatch and actions"
        );
    }
}

#[tokio::test]
async fn registration_failure_prevents_both_supervisor_runners_from_executing() {
    for handler_supervised in [false, true] {
        let journal = Arc::new(TestJournal {
            fail: true,
            owner: Some(if handler_supervised {
                JournalOwner::stage(StageId::new_const(1))
            } else {
                JournalOwner::system(SystemId::new_const(1))
            }),
            ..Default::default()
        });
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
        let task = if handler_supervised {
            SupervisorTaskBuilder::new("handler")
                .with_publications(publications)
                .spawn_handler_supervised(
                    TestHandlerSupervisor {
                        name: "handler".into(),
                        completion_writes: completions.clone(),
                        stage_id: StageId::new_const(1),
                    },
                    TestState::Running,
                    context,
                )
        } else {
            SupervisorTaskBuilder::new("runtime")
                .with_publications(publications)
                .spawn_self_supervised(
                    TestSelfSupervisor {
                        name: "runtime".into(),
                        completion_writes: completions.clone(),
                    },
                    TestState::Running,
                    context,
                )
        };
        let handle = HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap();
        assert!(handle
            .wait_for_completion()
            .await
            .unwrap_err()
            .to_string()
            .contains("Journal is full"));
        assert_eq!(journal.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(actions.load(Ordering::SeqCst), 0);
        assert_eq!(completions.load(Ordering::SeqCst), 0);
        assert!(journal.records.lock().unwrap().is_empty());
    }
}

#[async_trait::async_trait]
impl obzenflow_core::journal::JournalStorage<SystemEvent> for TestJournal {
    fn storage_id(&self) -> &JournalId {
        &self.id
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn storage_append(
        &self,
        event: SystemEvent,
        mut options: AppendOptions<SystemEvent>,
    ) -> Result<JournalRecord<SystemPayload>, JournalError> {
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
                && matches!(&event.payload, SystemPayload::SupervisorRegistered { .. }))
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
    ) -> Result<Vec<JournalRecord<SystemPayload>>, JournalError> {
        Ok(self.records.lock().unwrap().clone())
    }

    async fn storage_read_event(
        &self,
        id: &EventId,
    ) -> Result<Option<JournalRecord<SystemPayload>>, JournalError> {
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
    ) -> Result<Box<dyn JournalReader<SystemEvent>>, JournalError> {
        unreachable!("terminal mailbox tests read committed records directly")
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<SystemPayload>>, JournalError> {
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
            (journal.clone()).into(),
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
                "system.supervisor.command_discarded"
            );
            let SystemPayload::SupervisorCommandDiscarded {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            } = &record.payload
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
                (target).into(),
                WriterId::from(StageId::new_const(1)),
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
}
impl FsmContext for CompletionContext {}

#[derive(Clone, Debug)]
struct CompletionAction;

#[async_trait::async_trait]
impl FsmAction for CompletionAction {
    type Context = CompletionContext;

    async fn execute(&self, context: &mut CompletionContext) -> Result<(), FsmError> {
        context.entered.notify_one();
        context.release.notified().await;
        if context.fail_action {
            return Err(FsmError::HandlerError("terminal action failed".into()));
        }
        Ok(())
    }
}

struct CompletionSupervisor;

impl Supervisor for CompletionSupervisor {
    type State = ExternalEventTestState;
    type Event = ExternalEventTestEvent;
    type Context = CompletionContext;
    type Action = CompletionAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state: ExternalEventTestState;
            event: ExternalEventTestEvent;
            context: CompletionContext;
            action: CompletionAction;
            initial: initial_state;

            state ExternalEventTestState::Running {
                on ExternalEventTestEvent::Initialize => |_state: &ExternalEventTestState, _event: &ExternalEventTestEvent, _ctx: &mut CompletionContext| {
                    Box::pin(async { Ok(Transition { next_state: ExternalEventTestState::Drained, actions: vec![CompletionAction] }) })
                };
            }
            state ExternalEventTestState::Drained {
                on ExternalEventTestEvent::Error => |_state: &ExternalEventTestState, event: &ExternalEventTestEvent, _ctx: &mut CompletionContext| {
                    let ExternalEventTestEvent::Error(error) = event else { unreachable!() };
                    let error = error.clone();
                    Box::pin(async { Ok(Transition { next_state: ExternalEventTestState::Failed(error), actions: vec![] }) })
                };
            }
            state ExternalEventTestState::Failed {
                on ExternalEventTestEvent::Error => |state: &ExternalEventTestState, _event: &ExternalEventTestEvent, _ctx: &mut CompletionContext| {
                    let state = state.clone();
                    Box::pin(async { Ok(Transition { next_state: state, actions: vec![] }) })
                };
            }
        }
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Transform
    }

    fn report_journal(
        &self,
        _context: &Self::Context,
    ) -> crate::supervised_base::SupervisorJournal {
        Arc::new(TestJournal::default()).into()
    }

    fn name(&self) -> &str {
        "completion-worker"
    }
}

impl ExternalEventPolicy for CompletionSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        ExternalEventTestHandlerSupervisor::external_event_mode(state)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        ExternalEventTestHandlerSupervisor::on_external_event_channel_closed(state)
    }
}

impl HandlerSupervisedCleanup for CompletionSupervisor {}

#[async_trait::async_trait]
impl HandlerSupervised for CompletionSupervisor {
    type Handler = ();

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        assert!(matches!(
            state,
            ExternalEventTestState::Drained | ExternalEventTestState::Failed(_)
        ));
        Ok(EventLoopDirective::Terminate)
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id())
    }
    fn stage_id(&self) -> StageId {
        StageId::new_const(1)
    }
    fn event_for_action_error(&self, error: String) -> Self::Event {
        ExternalEventTestEvent::Error(error)
    }
}

#[tokio::test]
async fn terminal_mailbox_records_errors_arriving_during_completion_actions() {
    for fail_action in [false, true] {
        let journal = Arc::new(TestJournal::default());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (sender, receiver, watcher) =
            ChannelBuilder::new().build(ExternalEventTestState::Running);
        sender
            .send(ExternalEventTestEvent::Initialize)
            .await
            .unwrap();
        let supervisor = HandlerSupervisedWithExternalEvents::new(
            CompletionSupervisor,
            receiver,
            watcher.clone(),
            (journal.clone()).into(),
        );
        let task = SupervisorTaskBuilder::new("completion-worker").spawn_handler_supervised(
            supervisor,
            ExternalEventTestState::Running,
            CompletionContext {
                entered: entered.clone(),
                release: release.clone(),
                fail_action,
            },
        );
        let handle = HandleBuilder::new()
            .with_event_sender(sender.clone())
            .with_state_watcher(watcher.clone())
            .with_supervisor_task(task)
            .build_standard()
            .unwrap();
        entered.notified().await;
        sender
            .send(ExternalEventTestEvent::Initialize)
            .await
            .unwrap();
        sender
            .send(ExternalEventTestEvent::Error(
                "late external failure".into(),
            ))
            .await
            .unwrap();
        release.notify_one();
        handle.wait_for_completion().await.unwrap();
        let state = watcher.current();
        if fail_action {
            assert!(
                matches!(&state, ExternalEventTestState::Failed(error) if error.contains("terminal action failed"))
            );
        } else {
            assert_eq!(state, ExternalEventTestState::Drained);
        }
        let records = journal.read_all_unordered().await.unwrap();
        assert_eq!(records.len(), 3);
        assert!(matches!(
            &records[0].payload,
            SystemPayload::SupervisorRegistered { .. }
        ));
        assert!(
            matches!(&records[2].payload, SystemPayload::SupervisorCommandDiscarded { terminal_state, disposition: CommandDiscardDisposition::UnexpectedError, error: Some(error), .. } if terminal_state == state.variant_name() && error == "late external failure")
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
    let (sender, receiver, watcher) = ChannelBuilder::new().build(ExternalEventTestState::Drained);
    sender
        .send(ExternalEventTestEvent::Initialize)
        .await
        .unwrap();
    let supervisor = HandlerSupervisedWithExternalEvents::new(
        CompletionSupervisor,
        receiver,
        watcher.clone(),
        (journal.clone()).into(),
    );
    let context = CompletionContext {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        fail_action: false,
    };
    let task = SupervisorTaskBuilder::new("completion-worker").spawn_handler_supervised(
        supervisor,
        ExternalEventTestState::Drained,
        context,
    );
    let handle = HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
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
        SystemPayload::SupervisorRegistered { .. }
    ));
}
