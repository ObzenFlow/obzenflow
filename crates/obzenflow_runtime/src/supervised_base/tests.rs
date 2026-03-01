// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::base::Supervisor;
use super::{
    ChannelBuilder, EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
    HandlerSupervisedExt, HandlerSupervisedWithExternalEvents, SelfSupervised, SelfSupervisedExt,
    SelfSupervisedWithExternalEvents,
};
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, StateMachine, StateVariant, Transition,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, StateVariant)]
enum TestState {
    Running,
    Failed(String),
}

#[derive(Clone, Debug, EventVariant)]
enum TestEvent {
    Error(String),
}

#[derive(Clone, Debug)]
enum TestAction {
    MarkFailed,
}

struct TestContext {
    failure_actions_executed: Arc<AtomicUsize>,
}

impl FsmContext for TestContext {}

#[async_trait::async_trait]
impl FsmAction for TestAction {
    type Context = TestContext;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            TestAction::MarkFailed => {
                ctx.failure_actions_executed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }
}

fn build_test_machine(
    initial_state: TestState,
) -> StateMachine<TestState, TestEvent, TestContext, TestAction> {
    fsm! {
        state: TestState;
        event: TestEvent;
        context: TestContext;
        action: TestAction;
        initial: initial_state;

        state TestState::Running {
            on TestEvent::Error => |_state: &TestState, event: &TestEvent, _ctx: &mut TestContext| {
                let msg = match event {
                    TestEvent::Error(msg) => msg.clone(),
                };
                Box::pin(async move {
                    Ok(Transition {
                        next_state: TestState::Failed(msg),
                        actions: vec![TestAction::MarkFailed],
                    })
                })
            };
        }

        state TestState::Failed {
            on TestEvent::Error => |state: &TestState, _event: &TestEvent, _ctx: &mut TestContext| {
                let state = state.clone();
                Box::pin(async move {
                    Ok(Transition {
                        next_state: state,
                        actions: vec![],
                    })
                })
            };
        }
    }
}

struct TestSelfSupervisor {
    name: String,
    completion_writes: Arc<AtomicUsize>,
}

impl Supervisor for TestSelfSupervisor {
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
        &self.name
    }
}

#[async_trait::async_trait]
impl SelfSupervised for TestSelfSupervisor {
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            TestState::Running => Err("dispatch_state boom".into()),
            TestState::Failed(_) => Ok(EventLoopDirective::Terminate),
        }
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(StageId::new_const(1))
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.completion_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        TestEvent::Error(msg)
    }
}

struct TestHandlerSupervisor {
    name: String,
    completion_writes: Arc<AtomicUsize>,
    stage_id: StageId,
}

impl Supervisor for TestHandlerSupervisor {
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
        &self.name
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for TestHandlerSupervisor {
    type Handler = ();

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            TestState::Running => Err("dispatch_state boom".into()),
            TestState::Failed(_) => Ok(EventLoopDirective::Terminate),
        }
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.completion_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        TestEvent::Error(msg)
    }
}

#[tokio::test]
async fn dispatch_state_error_drives_fsm_failure_path_self_supervised() {
    let completion_writes = Arc::new(AtomicUsize::new(0));
    let failure_actions_executed = Arc::new(AtomicUsize::new(0));

    let supervisor = TestSelfSupervisor {
        name: "test-self-supervisor".to_string(),
        completion_writes: completion_writes.clone(),
    };
    let ctx = TestContext {
        failure_actions_executed: failure_actions_executed.clone(),
    };

    let result = SelfSupervisedExt::run(supervisor, TestState::Running, ctx).await;
    assert!(result.is_ok());
    assert_eq!(failure_actions_executed.load(Ordering::Relaxed), 1);
    assert_eq!(completion_writes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dispatch_state_error_drives_fsm_failure_path_handler_supervised() {
    let completion_writes = Arc::new(AtomicUsize::new(0));
    let failure_actions_executed = Arc::new(AtomicUsize::new(0));

    let supervisor = TestHandlerSupervisor {
        name: "test-handler-supervisor".to_string(),
        completion_writes: completion_writes.clone(),
        stage_id: StageId::new_const(1),
    };
    let ctx = TestContext {
        failure_actions_executed: failure_actions_executed.clone(),
    };

    let result = HandlerSupervisedExt::run(supervisor, TestState::Running, ctx).await;
    assert!(result.is_ok());
    assert_eq!(failure_actions_executed.load(Ordering::Relaxed), 1);
    assert_eq!(completion_writes.load(Ordering::Relaxed), 1);
}

#[derive(Clone, Debug, PartialEq, StateVariant)]
enum ExternalEventTestState {
    Created,
    Materializing,
    Running,
    Drained,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, EventVariant)]
enum ExternalEventTestEvent {
    Initialize,
    Error(String),
}

#[derive(Clone, Debug)]
enum ExternalEventTestAction {
    Noop,
}

#[async_trait::async_trait]
impl FsmAction for ExternalEventTestAction {
    type Context = ExternalEventTestContext;

    async fn execute(&self, _ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            ExternalEventTestAction::Noop => Ok(()),
        }
    }
}

#[derive(Default)]
struct ExternalEventTestContext;

impl FsmContext for ExternalEventTestContext {}

struct ExternalEventTestSelfSupervisor {
    name: String,
    dispatch_calls: Arc<AtomicUsize>,
}

impl Supervisor for ExternalEventTestSelfSupervisor {
    type State = ExternalEventTestState;
    type Event = ExternalEventTestEvent;
    type Context = ExternalEventTestContext;
    type Action = ExternalEventTestAction;

    fn build_state_machine(
        &self,
        _initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        panic!("not required for wrapper dispatch_state tests");
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl ExternalEventPolicy for ExternalEventTestSelfSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        match state {
            ExternalEventTestState::Created => ExternalEventMode::Block,
            ExternalEventTestState::Materializing => ExternalEventMode::Ignore,
            ExternalEventTestState::Drained | ExternalEventTestState::Failed(_) => {
                ExternalEventMode::Ignore
            }
            ExternalEventTestState::Running => ExternalEventMode::Poll,
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            ExternalEventTestState::Drained | ExternalEventTestState::Failed(_)
        ) {
            None
        } else {
            Some(ExternalEventTestEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl SelfSupervised for ExternalEventTestSelfSupervisor {
    async fn dispatch_state(
        &mut self,
        _state: &Self::State,
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        self.dispatch_calls.fetch_add(1, Ordering::Relaxed);
        Ok(EventLoopDirective::Continue)
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(StageId::new_const(1))
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        ExternalEventTestEvent::Error(msg)
    }
}

struct ExternalEventTestHandlerSupervisor {
    name: String,
    dispatch_calls: Arc<AtomicUsize>,
    stage_id: StageId,
}

impl Supervisor for ExternalEventTestHandlerSupervisor {
    type State = ExternalEventTestState;
    type Event = ExternalEventTestEvent;
    type Context = ExternalEventTestContext;
    type Action = ExternalEventTestAction;

    fn build_state_machine(
        &self,
        _initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        panic!("not required for wrapper dispatch_state tests");
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl ExternalEventPolicy for ExternalEventTestHandlerSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        <ExternalEventTestSelfSupervisor as ExternalEventPolicy>::external_event_mode(state)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        <ExternalEventTestSelfSupervisor as ExternalEventPolicy>::on_external_event_channel_closed(
            state,
        )
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for ExternalEventTestHandlerSupervisor {
    type Handler = ();

    async fn dispatch_state(
        &mut self,
        _state: &Self::State,
        _context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        self.dispatch_calls.fetch_add(1, Ordering::Relaxed);
        Ok(EventLoopDirective::Continue)
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        ExternalEventTestEvent::Error(msg)
    }
}

#[tokio::test]
async fn with_external_events_disconnected_maps_to_error_event() {
    // Keep all variants "live" so refactors do not accidentally narrow the policy surface.
    let _ = ExternalEventTestState::Drained;
    let _ = ExternalEventTestState::Failed("x".to_string());
    let _ = ExternalEventTestAction::Noop;

    // SelfSupervised wrapper
    let (_sender, receiver, watcher) =
        ChannelBuilder::<ExternalEventTestEvent, ExternalEventTestState>::new()
            .build(ExternalEventTestState::Created);
    drop(_sender);

    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let inner = ExternalEventTestSelfSupervisor {
        name: "test-self-with-external-events".to_string(),
        dispatch_calls: dispatch_calls.clone(),
    };
    let mut sup = SelfSupervisedWithExternalEvents::new(inner, receiver, watcher);
    let mut ctx = ExternalEventTestContext;

    let created = sup
        .dispatch_state(&ExternalEventTestState::Created, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        created,
        EventLoopDirective::Transition(ExternalEventTestEvent::Error(_))
    ));

    let running = sup
        .dispatch_state(&ExternalEventTestState::Running, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        running,
        EventLoopDirective::Transition(ExternalEventTestEvent::Error(_))
    ));
    assert_eq!(dispatch_calls.load(Ordering::Relaxed), 0);

    // HandlerSupervised wrapper
    let (_sender, receiver, watcher) =
        ChannelBuilder::<ExternalEventTestEvent, ExternalEventTestState>::new()
            .build(ExternalEventTestState::Created);
    drop(_sender);

    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let inner = ExternalEventTestHandlerSupervisor {
        name: "test-handler-with-external-events".to_string(),
        dispatch_calls: dispatch_calls.clone(),
        stage_id: StageId::new_const(1),
    };
    let mut sup = HandlerSupervisedWithExternalEvents::new(inner, receiver, watcher);
    let mut ctx = ExternalEventTestContext;

    let created = sup
        .dispatch_state(&ExternalEventTestState::Created, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        created,
        EventLoopDirective::Transition(ExternalEventTestEvent::Error(_))
    ));

    let running = sup
        .dispatch_state(&ExternalEventTestState::Running, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        running,
        EventLoopDirective::Transition(ExternalEventTestEvent::Error(_))
    ));
    assert_eq!(dispatch_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn with_external_events_ignore_mode_does_not_drain_channel() {
    let (sender, receiver, watcher) =
        ChannelBuilder::<ExternalEventTestEvent, ExternalEventTestState>::new()
            .build(ExternalEventTestState::Materializing);

    sender
        .send(ExternalEventTestEvent::Initialize)
        .await
        .unwrap();

    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let inner = ExternalEventTestSelfSupervisor {
        name: "test-self-ignore-mode".to_string(),
        dispatch_calls: dispatch_calls.clone(),
    };
    let mut sup = SelfSupervisedWithExternalEvents::new(inner, receiver, watcher);
    let mut ctx = ExternalEventTestContext;

    let ignored = sup
        .dispatch_state(&ExternalEventTestState::Materializing, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(ignored, EventLoopDirective::Continue));
    assert_eq!(dispatch_calls.load(Ordering::Relaxed), 1);

    let queued = sup
        .dispatch_state(&ExternalEventTestState::Running, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        queued,
        EventLoopDirective::Transition(ExternalEventTestEvent::Initialize)
    ));
    assert_eq!(
        dispatch_calls.load(Ordering::Relaxed),
        1,
        "poll mode must not delegate when an external event is ready"
    );
}
