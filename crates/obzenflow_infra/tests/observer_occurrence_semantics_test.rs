// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Behavioural occurrence proof for ordinary observer surfaces.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{handler_observer, join_observer, stage_lifecycle_observer};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{StageOutputs, TypedPayload};
use obzenflow_dsl::{effectful_transform, flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome,
    SinkWriteContext, SinkWriteReport, TypedFiniteSourceHandler, TypedJoinHandler,
    TypedTransformHandler,
};
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, JoinObserver, JoinObserverContext,
    JoinObserverOccurrence, JoinSide, JoinSignalKind, StageInputPosition, StageLifecycleObserver,
    StageLifecycleObserverContext, StageLifecyclePhase,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Barrier;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reference {
    key: u64,
}

impl TypedPayload for Reference {
    const EVENT_TYPE: &'static str = "observer.join.reference";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamInput {
    key: u64,
}

impl TypedPayload for StreamInput {
    const EVENT_TYPE: &'static str = "observer.join.stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Joined {
    key: u64,
}

impl TypedPayload for Joined {
    const EVENT_TYPE: &'static str = "observer.join.output";
}

#[derive(Clone, Debug)]
struct OneEventSource<T> {
    event: Option<T>,
}

impl<T> OneEventSource<T> {
    fn new(event: T) -> Self {
        Self { event: Some(event) }
    }
}

impl TypedFiniteSourceHandler for OneEventSource<Reference> {
    type Output = Reference;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(self.event.take().map(|event| vec![event]))
    }
}

impl TypedFiniteSourceHandler for OneEventSource<StreamInput> {
    type Output = StreamInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(self.event.take().map(|event| vec![event]))
    }
}

#[derive(Clone, Debug)]
struct LookupJoin;

impl TypedJoinHandler for LookupJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = Reference;
    type Stream = StreamInput;
    type Output = Joined;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.key)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, u64, Reference>,
        stream: StreamInput,
    ) -> Result<Vec<Joined>, HandlerError> {
        assert!(references.select(&stream.key).is_some());
        Ok(vec![Joined { key: stream.key }])
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = Joined;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: Joined,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Noop,
            None,
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Before,
    After,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Occurrence {
    Delivery {
        phase: Phase,
        side: JoinSide,
        event_type: String,
        output_count: Option<usize>,
    },
    Signal {
        phase: Phase,
        side: Option<JoinSide>,
        signal: JoinSignalKind,
        output_count: Option<usize>,
    },
}

#[derive(Clone)]
struct RecordsJoinOccurrences {
    observations: Arc<Mutex<Vec<Occurrence>>>,
}

impl RecordsJoinOccurrences {
    fn record(&self, phase: Phase, ctx: &JoinObserverContext<'_>, output_count: Option<usize>) {
        let occurrence = match ctx.occurrence() {
            JoinObserverOccurrence::Delivery(delivery) => {
                assert!(ctx.input().is_some());
                assert!(ctx.stage_input_position().is_some());
                Occurrence::Delivery {
                    phase,
                    side: delivery.side(),
                    event_type: delivery.input().event_type().to_string(),
                    output_count,
                }
            }
            JoinObserverOccurrence::Signal(signal) => {
                assert!(ctx.input().is_none());
                assert!(ctx.stage_input_position().is_none());
                Occurrence::Signal {
                    phase,
                    side: signal.side(),
                    signal: signal.signal(),
                    output_count,
                }
            }
        };
        self.observations
            .lock()
            .expect("join observation lock poisoned")
            .push(occurrence);
    }
}

impl JoinObserver for RecordsJoinOccurrences {
    fn before_join_input(&self, ctx: &JoinObserverContext<'_>) {
        self.record(Phase::Before, ctx, None);
    }

    fn after_join_output(
        &self,
        ctx: &JoinObserverContext<'_>,
        outputs: &[obzenflow_core::ChainEvent],
    ) {
        self.record(Phase::After, ctx, Some(outputs.len()));
    }
}

#[tokio::test]
async fn join_observer_distinguishes_deliveries_from_signals_without_synthetic_positions() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observations_for_flow = observations.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let reference_source = OneEventSource::new(Reference { key: 7 });
            let stream_source = OneEventSource::new(StreamInput { key: 7 });
            let join_handler = LookupJoin;
            let sink_handler = NoopSink;

            Ok(flow! {
                name: "observer_join_occurrences",
                journals: memory_journals(),

                stages: {
                    reference = source!(Reference => reference_source);
                    stream = source!(StreamInput => stream_source);
                    joined = join!(
                        catalog reference: Reference,
                        StreamInput -> Joined => join_handler,
                        observers: [
                            join_observer(
                                "join-occurrences",
                                RecordsJoinOccurrences {
                                    observations: observations_for_flow,
                                }
                            )
                        ]
                    );
                    output = sink!(Joined => sink_handler);
                },

                topology: {
                    stream |> joined;
                    joined |> output;
                }
            })
        }))
        .await
        .expect("join observer flow completes");

    let observations = observations
        .lock()
        .expect("join observation assertion lock");
    assert_eq!(
        observations.len(),
        6,
        "two deliveries and the stream's contract and EOF signals must produce exactly six callbacks: {observations:?}"
    );
    assert_eq!(
        observations
            .iter()
            .filter(|occurrence| matches!(occurrence, Occurrence::Delivery { .. }))
            .count(),
        4,
        "each of the two deliveries has one before and one after callback"
    );
    assert_eq!(
        observations
            .iter()
            .filter(|occurrence| matches!(occurrence, Occurrence::Signal { .. }))
            .count(),
        2,
        "the stream contract and EOF each have one before-input signal callback"
    );
    for (side, event_type) in [
        (JoinSide::Reference, Reference::versioned_event_type()),
        (JoinSide::Stream, StreamInput::versioned_event_type()),
    ] {
        assert!(observations.contains(&Occurrence::Delivery {
            phase: Phase::Before,
            side,
            event_type: event_type.clone(),
            output_count: None,
        }));
        assert!(observations.iter().any(|occurrence| matches!(
            occurrence,
            Occurrence::Delivery {
                phase: Phase::After,
                side: observed_side,
                event_type: observed_type,
                ..
            } if *observed_side == side && observed_type == &event_type
        )));
    }
    assert!(observations.contains(&Occurrence::Delivery {
        phase: Phase::After,
        side: JoinSide::Stream,
        event_type: StreamInput::versioned_event_type(),
        output_count: Some(1),
    }));
    assert!(observations.contains(&Occurrence::Signal {
        phase: Phase::Before,
        side: Some(JoinSide::Stream),
        signal: JoinSignalKind::OtherControl,
        output_count: None,
    }));
    assert!(observations.contains(&Occurrence::Signal {
        phase: Phase::Before,
        side: Some(JoinSide::Stream),
        signal: JoinSignalKind::Eof,
        output_count: None,
    }));
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HandlerInput {
    value: u64,
}

impl TypedPayload for HandlerInput {
    const EVENT_TYPE: &'static str = "observer.handler.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HandlerOutput {
    value: u64,
    copy: u64,
}

impl TypedPayload for HandlerOutput {
    const EVENT_TYPE: &'static str = "observer.handler.output";
}

#[derive(Clone, Debug)]
struct TwoHandlerInputs {
    emitted: bool,
}

impl TypedFiniteSourceHandler for TwoHandlerInputs {
    type Output = HandlerInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![
            HandlerInput { value: 0 },
            HandlerInput { value: 1 },
        ]))
    }
}

#[derive(Clone, Debug)]
struct OneHandlerInput {
    emitted: bool,
}

impl TypedFiniteSourceHandler for OneHandlerInput {
    type Output = HandlerInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![HandlerInput { value: 7 }]))
    }
}

#[derive(Clone, Debug)]
struct ZeroThenFanOut;

impl TypedTransformHandler for ZeroThenFanOut {
    type Input = HandlerInput;
    type Output = StageOutputs<HandlerOutput>;

    fn process(&self, input: HandlerInput) -> Result<Self::Output, HandlerError> {
        if input.value == 0 {
            return Ok(StageOutputs::none());
        }
        Ok(StageOutputs::many((0..3).map(|copy| HandlerOutput {
            value: input.value,
            copy,
        })))
    }
}

#[derive(Clone, Debug)]
struct FatalTransform;

impl TypedTransformHandler for FatalTransform {
    type Input = HandlerInput;
    type Output = HandlerOutput;

    fn process(&self, _input: HandlerInput) -> Result<Self::Output, HandlerError> {
        Err(HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            "intentional fatal return for observer occurrence proof",
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandlerOccurrence {
    Before {
        stage: String,
        input: u64,
        position: StageInputPosition,
    },
    After {
        stage: String,
        input: u64,
        position: StageInputPosition,
        outputs: usize,
    },
}

#[derive(Clone)]
struct RecordsHandlerOccurrences {
    observations: Arc<Mutex<Vec<HandlerOccurrence>>>,
}

impl RecordsHandlerOccurrences {
    fn input(ctx: &HandlerObserverContext<'_>) -> u64 {
        ctx.input()
            .payload()
            .get("value")
            .and_then(serde_json::Value::as_u64)
            .expect("handler occurrence input carries a numeric value")
    }
}

impl HandlerObserver for RecordsHandlerOccurrences {
    fn before_handle(&self, ctx: &HandlerObserverContext<'_>) {
        self.observations
            .lock()
            .expect("handler observation lock")
            .push(HandlerOccurrence::Before {
                stage: ctx.stage_name().to_string(),
                input: Self::input(ctx),
                position: ctx.stage_input_position(),
            });
    }

    fn after_handle(
        &self,
        ctx: &HandlerObserverContext<'_>,
        outputs: &[obzenflow_core::ChainEvent],
    ) {
        self.observations
            .lock()
            .expect("handler observation lock")
            .push(HandlerOccurrence::After {
                stage: ctx.stage_name().to_string(),
                input: Self::input(ctx),
                position: ctx.stage_input_position(),
                outputs: outputs.len(),
            });
    }
}

#[derive(Clone, Debug)]
struct CountsHandlerOutputs {
    deliveries: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for CountsHandlerOutputs {
    type Input = HandlerOutput;

    async fn write(
        &mut self,
        _input: HandlerOutput,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Noop,
            None,
        )))
    }
}

#[tokio::test]
async fn handler_observer_runs_once_for_zero_output_and_reports_the_whole_fan_out_slice() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observations_for_flow = observations.clone();
    let deliveries = Arc::new(AtomicUsize::new(0));
    let deliveries_for_flow = deliveries.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let inputs = TwoHandlerInputs { emitted: false };
            let handler = ZeroThenFanOut;
            let output = CountsHandlerOutputs {
                deliveries: deliveries_for_flow,
            };

            Ok(flow! {
                name: "observer_zero_and_fan_out_occurrences",
                journals: memory_journals(),

                stages: {
                    input = source!(HandlerInput => inputs);
                    expanded = transform!(
                        HandlerInput -> HandlerOutput => handler,
                        observers: [handler_observer(
                            "handler-occurrences",
                            RecordsHandlerOccurrences {
                                observations: observations_for_flow,
                            }
                        )]
                    );
                    output = sink!(HandlerOutput => output);
                },

                topology: {
                    input |> expanded;
                    expanded |> output;
                }
            })
        }))
        .await
        .expect("zero-output and fan-out observer flow completes");

    assert_eq!(deliveries.load(Ordering::SeqCst), 3);
    let observations = observations.lock().expect("handler assertion lock");
    let [before_zero, after_zero, before_fan_out, after_fan_out] = observations.as_slice() else {
        panic!("expected two balanced handler occurrences: {observations:?}");
    };
    let (
        HandlerOccurrence::Before {
            stage: before_zero_stage,
            input: 0,
            position: zero_before_position,
        },
        HandlerOccurrence::After {
            stage: after_zero_stage,
            input: 0,
            position: zero_after_position,
            outputs: 0,
        },
        HandlerOccurrence::Before {
            stage: before_fan_out_stage,
            input: 1,
            position: fan_out_before_position,
        },
        HandlerOccurrence::After {
            stage: after_fan_out_stage,
            input: 1,
            position: fan_out_after_position,
            outputs: 3,
        },
    ) = (before_zero, after_zero, before_fan_out, after_fan_out)
    else {
        panic!("unexpected handler occurrence sequence: {observations:?}");
    };
    assert_eq!(before_zero_stage, "expanded");
    assert_eq!(after_zero_stage, before_zero_stage);
    assert_eq!(before_fan_out_stage, before_zero_stage);
    assert_eq!(after_fan_out_stage, before_zero_stage);
    assert_eq!(zero_before_position, zero_after_position);
    assert_eq!(fan_out_before_position, fan_out_after_position);
    assert!(fan_out_before_position > zero_before_position);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConcurrentInput {
    value: u64,
}

impl TypedPayload for ConcurrentInput {
    const EVENT_TYPE: &'static str = "observer.concurrent.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ConcurrentOutput {
    value: u64,
    branch: String,
}

impl TypedPayload for ConcurrentOutput {
    const EVENT_TYPE: &'static str = "observer.concurrent.output";
}

#[derive(Clone, Debug)]
struct OneConcurrentInput {
    emitted: bool,
}

impl TypedFiniteSourceHandler for OneConcurrentInput {
    type Output = ConcurrentInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ConcurrentInput { value: 11 }]))
    }
}

#[derive(Clone, Debug)]
struct ConcurrentTransform {
    branch: &'static str,
    barrier: Arc<Barrier>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for ConcurrentTransform {
    type Input = ConcurrentInput;
    type Output = ConcurrentOutput;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        input: ConcurrentInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.barrier.wait().await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        fx.emit(ConcurrentOutput {
            value: input.value,
            branch: self.branch.to_string(),
        })
        .await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
struct CountsConcurrentOutputs {
    deliveries: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for CountsConcurrentOutputs {
    type Input = ConcurrentOutput;

    async fn write(
        &mut self,
        _input: ConcurrentOutput,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Noop,
            None,
        )))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_fan_out_dispatches_once_per_concurrent_stage_occurrence() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observations_for_left = observations.clone();
    let observations_for_right = observations.clone();
    let deliveries = Arc::new(AtomicUsize::new(0));
    let deliveries_for_flow = deliveries.clone();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let active_for_flow = active.clone();
    let max_active_for_flow = max_active.clone();
    let barrier = Arc::new(Barrier::new(2));
    let barrier_for_flow = barrier.clone();

    let run = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let input = OneConcurrentInput { emitted: false };
            let left = ConcurrentTransform {
                branch: "left",
                barrier: barrier_for_flow.clone(),
                active: active_for_flow.clone(),
                max_active: max_active_for_flow.clone(),
            };
            let right = ConcurrentTransform {
                branch: "right",
                barrier: barrier_for_flow,
                active: active_for_flow,
                max_active: max_active_for_flow,
            };
            let output = CountsConcurrentOutputs {
                deliveries: deliveries_for_flow,
            };

            Ok(flow! {
                name: "observer_concurrent_graph_fan_out",
                journals: memory_journals(),

                stages: {
                    input = source!(ConcurrentInput => input);
                    left = effectful_transform!(
                        ConcurrentInput -> ConcurrentOutput => left,
                        observers: [handler_observer(
                            "left-occurrences",
                            RecordsHandlerOccurrences {
                                observations: observations_for_left,
                            }
                        )]
                    );
                    right = effectful_transform!(
                        ConcurrentInput -> ConcurrentOutput => right,
                        observers: [handler_observer(
                            "right-occurrences",
                            RecordsHandlerOccurrences {
                                observations: observations_for_right,
                            }
                        )]
                    );
                    output = sink!(ConcurrentOutput => output);
                },

                topology: {
                    input |> left;
                    input |> right;
                    left |> output;
                    right |> output;
                }
            })
        }));

    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("concurrent fan-out flow does not stall")
        .expect("concurrent fan-out flow completes");

    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    let observations = observations.lock().expect("concurrent assertion lock");
    assert_eq!(observations.len(), 4, "{observations:?}");
    for stage in ["left", "right"] {
        assert_eq!(
            observations
                .iter()
                .filter(|occurrence| matches!(
                    occurrence,
                    HandlerOccurrence::Before { stage: observed, input: 11, .. }
                        if observed == stage
                ))
                .count(),
            1,
            "{stage} receives one before callback: {observations:?}"
        );
        assert_eq!(
            observations
                .iter()
                .filter(|occurrence| matches!(
                    occurrence,
                    HandlerOccurrence::After {
                        stage: observed,
                        input: 11,
                        outputs: 0,
                        ..
                    } if observed == stage
                ))
                .count(),
            1,
            "{stage} receives one after callback: {observations:?}"
        );
    }
}

#[derive(Clone)]
struct RecordsLifecycle {
    phases: Arc<Mutex<Vec<StageLifecyclePhase>>>,
}

impl StageLifecycleObserver for RecordsLifecycle {
    fn on_stage_lifecycle(&self, ctx: &StageLifecycleObserverContext<'_>) {
        self.phases
            .lock()
            .expect("lifecycle observation lock")
            .push(ctx.phase());
    }
}

#[tokio::test]
async fn fatal_handler_return_has_before_without_after_and_one_failed_lifecycle() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observations_for_flow = observations.clone();
    let phases = Arc::new(Mutex::new(Vec::new()));
    let phases_for_flow = phases.clone();
    let deliveries = Arc::new(AtomicUsize::new(0));
    let deliveries_for_flow = deliveries.clone();

    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let input = OneHandlerInput { emitted: false };
            let fatal = FatalTransform;
            let output = CountsHandlerOutputs {
                deliveries: deliveries_for_flow,
            };

            Ok(flow! {
                name: "observer_fatal_handler_occurrence",
                journals: memory_journals(),

                stages: {
                    input = source!(HandlerInput => input);
                    fatal = transform!(
                        HandlerInput -> HandlerOutput => fatal,
                        observers: [
                            handler_observer(
                                "fatal-handler-occurrences",
                                RecordsHandlerOccurrences {
                                    observations: observations_for_flow,
                                }
                            ),
                            stage_lifecycle_observer(
                                "fatal-lifecycle-occurrences",
                                RecordsLifecycle {
                                    phases: phases_for_flow,
                                }
                            )
                        ]
                    );
                    output = sink!(HandlerOutput => output);
                },

                topology: {
                    input |> fatal;
                    fatal |> output;
                }
            })
        }))
        .await;

    assert!(
        result.is_err(),
        "the intentional fatal return must fail the flow"
    );
    assert_eq!(deliveries.load(Ordering::SeqCst), 0);
    let observations = observations.lock().expect("fatal assertion lock");
    assert_eq!(observations.len(), 1, "{observations:?}");
    assert!(matches!(
        &observations[0],
        HandlerOccurrence::Before {
            stage,
            input: 7,
            ..
        } if stage == "fatal"
    ));
    assert_eq!(
        *phases.lock().expect("fatal lifecycle assertion lock"),
        [StageLifecyclePhase::Running, StageLifecyclePhase::Failed]
    );
}
