// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Behavioural occurrence proof for the join observer surface.

use async_trait::async_trait;
use obzenflow_adapters::middleware::join_observer;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, join, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedFiniteSourceHandler, TypedJoinHandler,
};
use obzenflow_runtime::stages::observer::{
    JoinObserver, JoinObserverContext, JoinObserverOccurrence, JoinSide, JoinSignalKind,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

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
