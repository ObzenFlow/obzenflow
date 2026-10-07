// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::supervised_base::base::{EventLoopDirective, Registration, Supervisor};
use crate::supervised_base::handler_supervised::{
    ActionCompletion, ActionExecution, DispatchCompletion, HandlerSupervised, HandlerSupervisedExt,
    OwnedDispatch,
};
use crate::supervised_base::{SelfSupervised, SelfSupervisedExt};
use futures::{future::BoxFuture, poll};
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::{StageId, SystemId};
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, FsmError, StateMachine, StateVariant, Transition,
};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Instrument, Metadata, Subscriber};

#[test]
fn phases_partition_elapsed_time_including_residual_and_state_revisits() {
    let start = Instant::now();
    let mut accumulator = Accumulator::new("Running", start);
    accumulator.phase(Phase::PendingAction, start + Duration::from_nanos(7));
    accumulator.phase(Phase::Residual, start + Duration::from_nanos(18));
    accumulator.state("Pausing", start + Duration::from_nanos(21));
    accumulator.phase(Phase::PendingAction, start + Duration::from_nanos(26));
    accumulator.phase(Phase::Residual, start + Duration::from_nanos(39));
    accumulator.state("Running", start + Duration::from_nanos(41));
    accumulator.phase(Phase::Transition, start + Duration::from_nanos(42));
    accumulator.phase(Phase::Residual, start + Duration::from_nanos(45));
    accumulator.charge(start + Duration::from_nanos(47));
    assert_eq!(accumulator.states.len(), 2);
    let running = &accumulator.states[0].totals;
    let pausing = &accumulator.states[1].totals;
    assert_eq!(running.elapsed_ns, 27);
    assert_eq!(pausing.elapsed_ns, 20);
    assert_eq!(running.nanoseconds[Phase::PendingAction as usize], 11);
    assert_eq!(pausing.nanoseconds[Phase::PendingAction as usize], 13);
    for state in &accumulator.states {
        assert_eq!(
            state.totals.nanoseconds.iter().sum::<u64>(),
            state.totals.elapsed_ns
        );
    }
}

#[test]
fn zero_duration_and_repeated_selection_do_not_recharge_operation_lifetime() {
    let start = Instant::now();
    let mut accumulator = Accumulator::new("Running", start);
    accumulator.phase(Phase::PendingDispatch, start);
    accumulator.phase(Phase::Residual, start);
    accumulator.phase(Phase::Transition, start);
    accumulator.phase(Phase::Residual, start + Duration::from_nanos(3));
    accumulator.phase(Phase::PendingDispatch, start + Duration::from_nanos(3));
    accumulator.phase(Phase::Residual, start + Duration::from_nanos(8));
    let totals = &accumulator.states[0].totals;
    assert_eq!(totals.elapsed_ns, 8);
    assert_eq!(totals.nanoseconds[Phase::PendingDispatch as usize], 5);
    assert_eq!(totals.entries[Phase::PendingDispatch as usize], 2);
    assert_eq!(totals.nanoseconds.iter().sum::<u64>(), 8);
}

#[derive(Clone, Debug, Default)]
struct Fields(BTreeMap<String, Value>);

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
}

struct RecordedSpan {
    name: &'static str,
    parent: Option<u64>,
    fields: Fields,
}

#[derive(Default)]
struct Recording {
    spans: Vec<RecordedSpan>,
    stacks: HashMap<std::thread::ThreadId, Vec<u64>>,
    summaries: Vec<Fields>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Recording>>, bool);

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        (!self.1 || metadata.is_span())
            && matches!(
                metadata.target(),
                "obzenflow::performance" | "obzenflow::supervisor_timing"
            )
    }
    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        let mut recording = self.0.lock().unwrap();
        let parent = attributes.parent().map(Id::into_u64).or_else(|| {
            attributes
                .is_contextual()
                .then(|| {
                    recording
                        .stacks
                        .get(&std::thread::current().id())
                        .and_then(|stack| stack.last().copied())
                })
                .flatten()
        });
        recording.spans.push(RecordedSpan {
            name: attributes.metadata().name(),
            parent,
            fields,
        });
        Id::from_u64(recording.spans.len() as u64)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if event.metadata().target() == "obzenflow::supervisor_timing" {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().summaries.push(fields);
        }
    }
    fn enter(&self, span: &Id) {
        self.0
            .lock()
            .unwrap()
            .stacks
            .entry(std::thread::current().id())
            .or_default()
            .push(span.into_u64());
    }
    fn exit(&self, span: &Id) {
        assert_eq!(
            self.0
                .lock()
                .unwrap()
                .stacks
                .entry(std::thread::current().id())
                .or_default()
                .pop(),
            Some(span.into_u64())
        );
    }
}

impl Capture {
    fn assert_not_entered(&self) {
        assert!(
            self.0.lock().unwrap().stacks.values().all(Vec::is_empty),
            "no entered span may survive suspension"
        );
    }

    fn assert_summaries(&self, interrupted: bool, mode: &str, phase: &str, entries: u64) {
        let recording = self.0.lock().unwrap();
        assert!(!recording.summaries.is_empty());
        let mut actual_entries = 0;
        for summary in &recording.summaries {
            let fields = &summary.0;
            assert_eq!(fields["supervisor"], "timing-fixture");
            assert_eq!(fields["supervision_mode"], mode);
            assert_eq!(
                fields["supervisor_kind"],
                if mode == "self_supervised" {
                    "Pipeline"
                } else {
                    "Transform"
                }
            );
            assert_eq!(fields["interrupted"], interrupted);
            let total: u64 = [
                "inline_action_ns",
                "pending_action_ns",
                "pending_dispatch_ns",
                "direct_dispatch_ns",
                "transition_ns",
                "yield_ns",
                "residual_ns",
            ]
            .iter()
            .map(|field| fields[*field].as_u64().unwrap())
            .sum();
            assert_eq!(total, fields["elapsed_ns"].as_u64().unwrap(), "{fields:?}");
            actual_entries += fields[phase].as_u64().unwrap();
        }
        assert_eq!(actual_entries, entries);
    }

    fn assert_selections(&self, completed_phase: Option<&str>) {
        let recording = self.0.lock().unwrap();
        let mut control_wins = 0;
        let mut complete_wins = 0;
        for summary in &recording.summaries {
            let fields = &summary.0;
            for phase in ["pending_action", "pending_dispatch"] {
                for branch in ["publication_failure", "control", "complete"] {
                    let key = format!("{phase}_{branch}_wins");
                    let actual = fields[&key].as_u64().unwrap();
                    let expected = match (
                        completed_phase == Some(phase),
                        fields["state"].as_str(),
                        branch,
                    ) {
                        (true, Some("Running" | "Pausing"), "control") => 1,
                        (true, Some("Pausing"), "complete") => 1,
                        _ => 0,
                    };
                    assert_eq!(actual, expected, "{key}: {fields:?}");
                    if branch == "control" {
                        control_wins += actual;
                    } else if branch == "complete" {
                        complete_wins += actual;
                    }
                }
            }
        }
        assert_eq!(control_wins, if completed_phase.is_some() { 2 } else { 0 });
        assert_eq!(complete_wins, u64::from(completed_phase.is_some()));
    }

    fn assert_nested(&self, phase: &str) {
        let recording = self.0.lock().unwrap();
        let child = recording
            .spans
            .iter()
            .find(|span| span.name == "retained_child")
            .expect("work emits a nested operation span");
        let parent = &recording.spans[child.parent.unwrap() as usize - 1];
        assert_eq!(parent.name, phase);
        let state = &recording.spans[parent.parent.unwrap() as usize - 1];
        assert_eq!(state.name, "supervisor_state");
        assert_eq!(state.fields.0["state"], "Running");
        let root = &recording.spans[state.parent.unwrap() as usize - 1];
        assert_eq!(root.name, "supervisor");
        assert_eq!(root.fields.0["supervisor"], "timing-fixture");
        assert_eq!(
            root.fields.0["supervisor_kind"],
            if root.fields.0["supervision_mode"] == "self_supervised" {
                "Pipeline"
            } else {
                "Transform"
            }
        );
        assert!(root.fields.0.contains_key("writer_id"));
        for span in &recording.spans {
            if matches!(
                span.name,
                "inline_action"
                    | "pending_action"
                    | "pending_dispatch"
                    | "direct_dispatch"
                    | "transition"
                    | "yield"
            ) {
                assert_eq!(
                    recording.spans[span.parent.unwrap() as usize - 1].name,
                    "supervisor_state"
                );
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, StateVariant)]
enum State {
    Created,
    Running,
    Pausing,
    Done,
}
#[derive(Clone, Debug, EventVariant)]
enum Input {
    Start,
    Control,
    Done,
}
#[derive(Clone, Debug)]
struct Begin;
#[derive(Clone, Copy)]
enum Work {
    Action,
    Dispatch,
    Direct,
}
struct Context {
    work: Work,
    gate: Option<oneshot::Receiver<()>>,
    completed: Arc<AtomicUsize>,
}
impl FsmContext for Context {}
#[async_trait::async_trait]
impl FsmAction for Begin {
    type Context = Context;
    async fn execute(&self, _: &mut Context) -> Result<(), FsmError> {
        unreachable!()
    }
}

struct Fixture {
    controls: mpsc::UnboundedReceiver<Input>,
    writer: WriterId,
}

impl Supervisor for Fixture {
    type State = State;
    type Event = Input;
    type Context = Context;
    type Action = Begin;
    fn name(&self) -> &str {
        "timing-fixture"
    }
    fn supervisor_kind(&self) -> SupervisorKind {
        if self.writer.is_system() {
            SupervisorKind::Pipeline
        } else {
            SupervisorKind::Transform
        }
    }
    fn registration(&self, _: &Context, _: SupervisorDescriptor) -> Registration {
        Box::pin(async { Ok(()) })
    }
    fn build_state_machine(
        &self,
        initial_state: State,
    ) -> StateMachine<State, Input, Context, Begin> {
        fsm! {
            state: State; event: Input; context: Context; action: Begin; initial: initial_state;
            state State::Created {
                on Input::Start => |_: &State, _: &Input, context: &mut Context| {
                    let actions = if matches!(context.work, Work::Action) { vec![Begin] } else { vec![] };
                    Box::pin(async move { Ok(Transition { next_state: State::Running, actions }) })
                };
            }
            state State::Running {
                on Input::Control => |_: &State, _: &Input, _: &mut Context| { Box::pin(async { Ok(Transition { next_state: State::Pausing, actions: vec![] }) }) };
                on Input::Done => |_: &State, _: &Input, _: &mut Context| { Box::pin(async { Ok(Transition { next_state: State::Done, actions: vec![] }) }) };
            }
            state State::Pausing {
                on Input::Control => |_: &State, _: &Input, _: &mut Context| { Box::pin(async { Ok(Transition { next_state: State::Pausing, actions: vec![] }) }) };
                on Input::Done => |_: &State, _: &Input, _: &mut Context| { Box::pin(async { Ok(Transition { next_state: State::Done, actions: vec![] }) }) };
            }
            state State::Done {}
        }
    }
}

async fn work(gate: oneshot::Receiver<()>, completed: Arc<AtomicUsize>) {
    async move {
        let _ = gate.await;
        completed.fetch_add(1, Ordering::SeqCst);
    }
    .instrument(tracing::debug_span!(target: "obzenflow::performance", "retained_child"))
    .await;
}

#[async_trait::async_trait]
impl HandlerSupervised for Fixture {
    type Handler = ();
    fn writer_id(&self) -> WriterId {
        self.writer
    }
    fn event_for_action_error(&self, _: String) -> Input {
        panic!("fixture execution failed")
    }
    async fn next_control(&mut self, _: &State, _: &mut Context) -> Option<Input> {
        self.controls.recv().await
    }
    async fn dispatch_state(
        &mut self,
        state: &State,
        context: &mut Context,
    ) -> Result<EventLoopDirective<Input>, super::super::publication::BoxError> {
        Ok(match state {
            State::Created => EventLoopDirective::Transition(Input::Start),
            State::Done => EventLoopDirective::Terminate,
            State::Running if matches!(context.work, Work::Direct) => {
                work(context.gate.take().unwrap(), context.completed.clone()).await;
                EventLoopDirective::Transition(Input::Done)
            }
            _ => panic!("retained operation must own processing"),
        })
    }
    async fn execute_action(
        &mut self,
        _: Begin,
        context: &mut Context,
    ) -> Result<ActionExecution<Context, Input>, FsmError> {
        let gate = context.gate.take().unwrap();
        let completed = context.completed.clone();
        Ok(ActionExecution::Pending(Box::pin(async move {
            work(gate, completed).await;
            Box::new(|_: &mut Context| Ok(Some(Input::Done))) as ActionCompletion<Context, Input>
        })))
    }
    fn owned_dispatch(
        &mut self,
        state: &State,
        context: &mut Context,
    ) -> Option<OwnedDispatch<Self>> {
        if !matches!(state, State::Running) || !matches!(context.work, Work::Dispatch) {
            return None;
        }
        let gate = context.gate.take().unwrap();
        let completed = context.completed.clone();
        Some(Box::pin(async move {
            work(gate, completed).await;
            Box::new(|_: &mut Self, _: &mut Context| {
                Ok(EventLoopDirective::Transition(Input::Done))
            }) as DispatchCompletion<Self>
        }))
    }
}

#[async_trait::async_trait]
impl SelfSupervised for Fixture {
    fn writer_id(&self) -> WriterId {
        self.writer
    }
    fn event_for_action_error(&self, _: String) -> Input {
        panic!("fixture execution failed")
    }
    async fn next_control(&mut self, state: &State, context: &mut Context) -> Option<Input> {
        HandlerSupervised::next_control(self, state, context).await
    }
    async fn dispatch_state(
        &mut self,
        state: &State,
        context: &mut Context,
    ) -> Result<EventLoopDirective<Input>, super::super::publication::BoxError> {
        HandlerSupervised::dispatch_state(self, state, context).await
    }
    async fn execute_action(
        &mut self,
        action: Begin,
        context: &mut Context,
    ) -> Result<ActionExecution<Context, Input>, FsmError> {
        HandlerSupervised::execute_action(self, action, context).await
    }
}

fn fixture(
    work: Work,
    self_supervised: bool,
) -> (
    BoxFuture<'static, Result<(), super::super::publication::BoxError>>,
    mpsc::UnboundedSender<Input>,
    oneshot::Sender<()>,
    Arc<AtomicUsize>,
) {
    let (send, controls) = mpsc::unbounded_channel();
    let (release, gate) = oneshot::channel();
    let completed = Arc::new(AtomicUsize::new(0));
    let context = Context {
        work,
        gate: Some(gate),
        completed: completed.clone(),
    };
    let supervisor = Fixture {
        controls,
        writer: if self_supervised {
            SystemId::new_const(42).into()
        } else {
            StageId::new().into()
        },
    };
    let runner = if self_supervised {
        SelfSupervisedExt::run(supervisor, State::Created, context)
    } else {
        HandlerSupervisedExt::run(supervisor, State::Created, context)
    };
    (runner, send, release, completed)
}

#[tokio::test]
async fn both_runner_families_conserve_retained_turns_and_keep_nested_spans() {
    for (work, self_supervised, phase) in [
        (Work::Action, false, "pending_action"),
        (Work::Dispatch, false, "pending_dispatch"),
        (Work::Action, true, "pending_action"),
    ] {
        let capture = Capture::default();
        let (runner, controls, release, completed) = fixture(work, self_supervised);
        let mut runner = Box::pin(runner.with_subscriber(capture.clone()));
        assert!(poll!(runner.as_mut()).is_pending());
        capture.assert_not_entered();
        for _ in 0..2 {
            controls.send(Input::Control).unwrap();
            assert!(poll!(runner.as_mut()).is_pending());
            capture.assert_not_entered();
            assert_eq!(
                completed.load(Ordering::SeqCst),
                0,
                "a control must retain the operation"
            );
        }
        release.send(()).unwrap();
        runner.await.unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        capture.assert_not_entered();
        capture.assert_summaries(
            false,
            if self_supervised {
                "self_supervised"
            } else {
                "handler_supervised"
            },
            &format!("{phase}_entries"),
            3,
        );
        capture.assert_selections(Some(phase));
        capture.assert_nested(phase);
    }
}

#[tokio::test]
async fn dropping_pending_runner_finalises_open_phase_without_completing_work() {
    for (work, self_supervised, phase) in [
        (Work::Action, false, "pending_action"),
        (Work::Dispatch, false, "pending_dispatch"),
        (Work::Action, true, "pending_action"),
        (Work::Direct, false, "direct_dispatch"),
    ] {
        let capture = Capture::default();
        let (runner, _controls, _release, completed) = fixture(work, self_supervised);
        let mut runner = Box::pin(runner.with_subscriber(capture.clone()));
        assert!(poll!(runner.as_mut()).is_pending());
        capture.assert_not_entered();
        drop(runner);
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        // Direct dispatch also handles Created before entering Running.
        let entries = if matches!(work, Work::Direct) { 2 } else { 1 };
        capture.assert_summaries(
            true,
            if self_supervised {
                "self_supervised"
            } else {
                "handler_supervised"
            },
            &format!("{phase}_entries"),
            entries,
        );
        capture.assert_selections(None);
        capture.assert_nested(phase);
        capture.assert_not_entered();
    }
}

#[test]
fn disabled_diagnostics_have_no_accumulator_or_spans() {
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        let mut timing = RunnerTiming::new(
            "disabled",
            SupervisorKind::Pipeline,
            SystemId::new_const(43).into(),
            SupervisionMode::SelfSupervised,
            "Ready",
        );
        assert!(timing.enabled.is_none());
        assert!(timing.root().is_disabled());
        timing.turn();
        timing.state("Running");
        assert!(timing.phase(Phase::DirectDispatch).span().is_disabled());
    });
}

#[tokio::test]
async fn accepted_publication_keeps_supervisor_ancestry_and_scoped_subscriber() {
    use crate::supervised_base::publication::PublicationScope;

    // A future flame-graph consumer may accept spans while rejecting all events.
    let capture = Capture(Arc::default(), true);
    let dispatch = tracing::Dispatch::new(capture.clone());
    let scope = PublicationScope::new();
    let receipt = tracing::dispatcher::with_default(&dispatch, || {
        let mut timing = RunnerTiming::new(
            "publication-fixture",
            SupervisorKind::Transform,
            StageId::new().into(),
            SupervisionMode::HandlerSupervised,
            "Running",
        );
        assert!(timing.enabled.as_ref().unwrap().accumulator.is_none());
        let phase = timing.phase(Phase::PendingDispatch);
        phase
            .span()
            .in_scope(|| {
                scope.enqueue(async {
                    // The task runs after the caller's scoped dispatcher is gone.
                    tracing::debug_span!(target: "obzenflow::performance", "publication_child")
                        .in_scope(|| {});
                    Ok(())
                })
            })
            .unwrap()
    });
    receipt.await.unwrap();
    scope.close();
    scope.join().await.unwrap();
    capture.assert_not_entered();
    let recording = capture.0.lock().unwrap();
    assert!(recording.summaries.is_empty());
    let child = recording
        .spans
        .iter()
        .find(|span| span.name == "publication_child")
        .expect("accepted work retains the originating dispatcher");
    let publication = &recording.spans[child.parent.unwrap() as usize - 1];
    assert_eq!(publication.name, "publication");
    let phase = &recording.spans[publication.parent.unwrap() as usize - 1];
    assert_eq!(phase.name, "pending_dispatch");
    let state = &recording.spans[phase.parent.unwrap() as usize - 1];
    assert_eq!(state.name, "supervisor_state");
    let supervisor = &recording.spans[state.parent.unwrap() as usize - 1];
    assert_eq!(supervisor.name, "supervisor");
    assert_eq!(supervisor.fields.0["supervisor"], "publication-fixture");
}
