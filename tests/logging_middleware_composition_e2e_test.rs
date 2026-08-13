// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m acceptance proof for logging middleware composition.
//!
//! One flow exercises source, handler, stateful, effectful-handler, join, and
//! sink observer surfaces. A logger-free control run and a logged treatment
//! run must have identical authoritative projections and external behaviour.
//! Strict replay then runs with panic-on-I/O ports and proves that logging is
//! live-only: no source/effect/physical-sink call and no fresh logging row.

#[path = "replay_testkit/mod.rs"]
mod replay_testkit;

use async_trait::async_trait;
use obzenflow_adapters::middleware::{log_event, LoggingLevel};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::observability_payload::{
    LoggingEvidence, LoggingJoinSide, LoggingOccurrence, LoggingSinkAttemptResult,
    LoggingSinkOutcome, LoggingSourceOutcome, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, EventEnvelope, JournalCausalLane, JournalEvent,
};
use obzenflow_core::{StageOutputs, TypedPayload};
use obzenflow_dsl::{
    effectful_transform, flow, join, sink, source, stateful, transform, FlowDefinition,
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::{
    DomainFacts, Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
    SinkRedeliverySafety,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome,
    SinkWriteContext, SinkWriteReport, StatefulEmission, TypedFiniteSourceHandler,
    TypedJoinHandler, TypedStatefulHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const PRIVATE_CANARY: &str = "card_4242_do_not_log";

const LOGGING_SURFACES: [(&str, &str, usize); 7] = [
    ("references", "proof.reference.poll", 2),
    ("stream", "proof.stream.poll", 3),
    ("fan_out", "proof.fanout.handle", 4),
    ("batcher", "proof.batcher.state", 7),
    ("lookup", "proof.lookup.handle", 4),
    ("joined", "proof.join.delivery", 10),
    ("output", "proof.sink.delivery", 4),
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReferenceRow {
    key: String,
    multiplier: u64,
    private_note: String,
}

impl TypedPayload for ReferenceRow {
    const EVENT_TYPE: &'static str = "flowip_115m.reference";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StreamInput {
    value: u64,
    private_note: String,
}

impl TypedPayload for StreamInput {
    const EVENT_TYPE: &'static str = "flowip_115m.stream_input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FanOutRow {
    value: u64,
}

impl TypedPayload for FanOutRow {
    const EVENT_TYPE: &'static str = "flowip_115m.fan_out";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Batch {
    ordinal: u64,
    sum: u64,
}

impl TypedPayload for Batch {
    const EVENT_TYPE: &'static str = "flowip_115m.batch";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LookupReply {
    ordinal: u64,
    accepted: bool,
}

impl TypedPayload for LookupReply {
    const EVENT_TYPE: &'static str = "flowip_115m.lookup_reply";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EnrichedBatch {
    ordinal: u64,
    sum: u64,
}

impl TypedPayload for EnrichedBatch {
    const EVENT_TYPE: &'static str = "flowip_115m.enriched_batch";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct JoinedRow {
    phase: String,
    value: u64,
}

impl TypedPayload for JoinedRow {
    const EVENT_TYPE: &'static str = "flowip_115m.joined";
}

#[derive(Clone, Default)]
struct ProofPorts {
    reference_source_calls: Arc<AtomicUsize>,
    stream_source_calls: Arc<AtomicUsize>,
    effect_calls: Arc<AtomicUsize>,
    join_calls: Arc<AtomicUsize>,
    sink_handler_calls: Arc<AtomicUsize>,
    physical_sink_calls: Arc<AtomicUsize>,
    delivered: Arc<Mutex<Vec<JoinedRow>>>,
}

#[derive(Clone, Debug)]
struct ReferenceSource {
    emitted: bool,
    calls: Arc<AtomicUsize>,
    panic_on_call: bool,
}

impl TypedFiniteSourceHandler for ReferenceSource {
    type Output = ReferenceRow;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        assert!(
            !self.panic_on_call,
            "strict replay polled the reference source"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ReferenceRow {
            key: "catalog".to_string(),
            multiplier: 10,
            private_note: PRIVATE_CANARY.to_string(),
        }]))
    }
}

#[derive(Clone, Debug)]
struct StreamSource {
    next: u64,
    calls: Arc<AtomicUsize>,
    panic_on_call: bool,
}

impl TypedFiniteSourceHandler for StreamSource {
    type Output = StreamInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        assert!(
            !self.panic_on_call,
            "strict replay polled the stream source"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.next > 2 {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![StreamInput {
            value,
            private_note: PRIVATE_CANARY.to_string(),
        }]))
    }
}

#[derive(Clone, Debug)]
struct FanOut;

impl TypedTransformHandler for FanOut {
    type Input = StreamInput;
    type Output = StageOutputs<FanOutRow>;

    fn process(&self, input: StreamInput) -> Result<Self::Output, HandlerError> {
        Ok(StageOutputs::many([
            FanOutRow {
                value: input.value * 10 + 1,
            },
            FanOutRow {
                value: input.value * 10 + 2,
            },
        ]))
    }
}

#[derive(Clone, Debug, Default)]
struct BatchState {
    ordinal: u64,
    count: u64,
    sum: u64,
}

#[derive(Clone, Debug)]
struct Batcher;

impl TypedStatefulHandler for Batcher {
    type State = BatchState;
    type Input = FanOutRow;
    type Output = Batch;

    fn initial_state(&self) -> Self::State {
        BatchState::default()
    }

    fn accumulate(&self, state: &mut Self::State, input: Self::Input) {
        state.count += 1;
        state.sum += input.value;
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.count == 2
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::ResetEpoch {
            next_state: BatchState {
                ordinal: state.ordinal + 1,
                ..BatchState::default()
            },
            outputs: vec![Batch {
                ordinal: state.ordinal,
                sum: state.sum,
            }],
        })
    }

    fn drain(&self, state: &Self::State) -> Result<Vec<Self::Output>, HandlerError> {
        Ok((state.count > 0)
            .then_some(Batch {
                ordinal: state.ordinal,
                sum: state.sum,
            })
            .into_iter()
            .collect())
    }
}

#[derive(Clone, Debug)]
struct ExternalLookup {
    ordinal: u64,
    sum: u64,
    calls: Arc<AtomicUsize>,
    panic_on_call: bool,
}

#[async_trait]
impl Effect for ExternalLookup {
    const EFFECT_TYPE: &'static str = "flowip_115m.external_lookup";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = LookupReply;
    type OutcomeSemantics = DomainFacts;

    fn label(&self) -> &str {
        "external_lookup"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "ordinal": self.ordinal, "sum": self.sum })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        assert!(
            !self.panic_on_call,
            "strict replay executed the external effect"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(LookupReply {
            ordinal: self.ordinal,
            accepted: true,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("lookup:{}", self.ordinal)))
    }
}

#[derive(Clone, Debug)]
struct LookupTransform {
    calls: Arc<AtomicUsize>,
    panic_on_call: bool,
}

#[async_trait]
impl EffectfulTransformHandler for LookupTransform {
    type Input = Batch;
    type Output = obzenflow_core::stage_fact_set![LookupReply, EnrichedBatch];
    type AllowedEffects = obzenflow_runtime::effect_set![ExternalLookup];

    async fn process(
        &self,
        input: Batch,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let reply = fx
            .perform(ExternalLookup {
                ordinal: input.ordinal,
                sum: input.sum,
                calls: self.calls.clone(),
                panic_on_call: self.panic_on_call,
            })
            .await
            .map_err(|error| HandlerError::Other(error.to_string()))?;
        if !reply.accepted {
            return Err(HandlerError::Domain("lookup rejected batch".to_string()));
        }
        fx.emit(EnrichedBatch {
            ordinal: input.ordinal,
            sum: input.sum,
        })
        .await
        .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "flowip-115m-lookup-v1"
    }
}

#[derive(Clone, Debug)]
struct ProofJoin {
    calls: Arc<AtomicUsize>,
}

impl TypedJoinHandler for ProofJoin {
    type State = ();
    type ReferenceKey = String;
    type Reference = ReferenceRow;
    type Stream = EnrichedBatch;
    type Output = JoinedRow;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<String, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(reference.key.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, ReferenceRow>,
        stream: EnrichedBatch,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let reference = references
            .select(&"catalog".to_string())
            .ok_or_else(|| HandlerError::Domain("catalog reference missing".to_string()))?;
        if stream.ordinal == 0 {
            let base = stream.sum * reference.multiplier;
            Ok(vec![
                JoinedRow {
                    phase: "stream".to_string(),
                    value: base,
                },
                JoinedRow {
                    phase: "stream".to_string(),
                    value: base + 1,
                },
            ])
        } else {
            Ok(Vec::new())
        }
    }

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, String, ReferenceRow>,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![JoinedRow {
            phase: "eof".to_string(),
            value: 900,
        }])
    }

    fn drain(
        &self,
        _state: &Self::State,
        _references: &mut JoinReferenceView<'_, String, ReferenceRow>,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![JoinedRow {
            phase: "drain".to_string(),
            value: 901,
        }])
    }
}

#[derive(Clone, Debug)]
struct ExternalSink {
    handler_calls: Arc<AtomicUsize>,
    physical_calls: Arc<AtomicUsize>,
    delivered: Arc<Mutex<Vec<JoinedRow>>>,
    panic_on_live: bool,
}

#[async_trait]
impl InlineSink for ExternalSink {
    type Input = JoinedRow;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified().with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(
        &mut self,
        output: Self::Input,
        context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.handler_calls.fetch_add(1, Ordering::SeqCst);
        if context.delivery().is_replayed() {
            return Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
                DeliveryMethod::Custom("proof_external_port".to_string()),
                None,
            )));
        }
        assert!(
            !self.panic_on_live,
            "strict replay reached the physical sink port"
        );
        self.physical_calls.fetch_add(1, Ordering::SeqCst);
        self.delivered
            .lock()
            .expect("delivered rows lock poisoned")
            .push(output);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("proof_external_port".to_string()),
            None,
        )))
    }
}

fn handlers(
    ports: &ProofPorts,
    panic_on_io: bool,
) -> (
    ReferenceSource,
    StreamSource,
    FanOut,
    Batcher,
    LookupTransform,
    ProofJoin,
    ExternalSink,
) {
    (
        ReferenceSource {
            emitted: false,
            calls: ports.reference_source_calls.clone(),
            panic_on_call: panic_on_io,
        },
        StreamSource {
            next: 1,
            calls: ports.stream_source_calls.clone(),
            panic_on_call: panic_on_io,
        },
        FanOut,
        Batcher,
        LookupTransform {
            calls: ports.effect_calls.clone(),
            panic_on_call: panic_on_io,
        },
        ProofJoin {
            calls: ports.join_calls.clone(),
        },
        ExternalSink {
            handler_calls: ports.sink_handler_calls.clone(),
            physical_calls: ports.physical_sink_calls.clone(),
            delivered: ports.delivered.clone(),
            panic_on_live: panic_on_io,
        },
    )
}

fn build_flow(
    journal_base: PathBuf,
    ports: ProofPorts,
    with_logging: bool,
    panic_on_io: bool,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let (references, stream, fan_out, batcher, lookup, joined, output) =
            handlers(&ports, panic_on_io);

        if with_logging {
            Ok(flow! {
                name: "logging_middleware_composition",
                journals: disk_journals(journal_base),

                stages: {
                    references = source!(ReferenceRow => references, observers: [
                        log_event("proof.reference.poll")
                            .level(LoggingLevel::Debug)
                            .tag("proof.surface", "source")
                    ]);
                    stream = source!(StreamInput => stream, observers: [
                        log_event("proof.stream.poll")
                            .tag("proof.surface", "source")
                    ]);
                    fan_out = transform!(StreamInput -> FanOutRow => fan_out, observers: [
                        log_event("proof.fanout.handle")
                            .tag("proof.surface", "handler")
                    ]);
                    batcher = stateful!(FanOutRow -> Batch => batcher, observers: [
                        log_event("proof.batcher.state")
                            .tag("proof.surface", "stateful")
                    ]);
                    lookup = effectful_transform!(
                        Batch -> { LookupReply, EnrichedBatch } => lookup,
                        effects: [ExternalLookup],
                        observers: [
                            log_event("proof.lookup.handle")
                                .tag("proof.surface", "effectful_handler")
                        ]
                    );
                    joined = join!(catalog references: ReferenceRow, EnrichedBatch -> JoinedRow => joined, observers: [
                        log_event("proof.join.delivery")
                            .tag("proof.surface", "join")
                    ]);
                    output = sink!(JoinedRow => output, observers: [
                        log_event("proof.sink.delivery")
                            .level(LoggingLevel::Info)
                            .tag("proof.surface", "sink")
                            .trace_mirror()
                    ]);
                },

                topology: {
                    stream |> fan_out;
                    fan_out |> batcher;
                    batcher |> lookup;
                    lookup |> joined;
                    joined |> output;
                }
            })
        } else {
            Ok(flow! {
                name: "logging_middleware_composition",
                journals: disk_journals(journal_base),

                stages: {
                    references = source!(ReferenceRow => references);
                    stream = source!(StreamInput => stream);
                    fan_out = transform!(StreamInput -> FanOutRow => fan_out);
                    batcher = stateful!(FanOutRow -> Batch => batcher);
                    lookup = effectful_transform!(
                        Batch -> { LookupReply, EnrichedBatch } => lookup,
                        effects: [ExternalLookup],
                        observers: []
                    );
                    joined = join!(catalog references: ReferenceRow, EnrichedBatch -> JoinedRow => joined);
                    output = sink!(JoinedRow => output);
                },

                topology: {
                    stream |> fan_out;
                    fan_out |> batcher;
                    batcher |> lookup;
                    lookup |> joined;
                    joined |> output;
                }
            })
        }
    })
}

async fn run(
    journal_base: &Path,
    replay_from: Option<&Path>,
    ports: ProofPorts,
    with_logging: bool,
    panic_on_io: bool,
) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
        args.push(OsString::from("--verify"));
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(
            journal_base.to_path_buf(),
            ports,
            with_logging,
            panic_on_io,
        ))
        .await
        .expect("logging composition flow completes");
}

fn expected_deliveries() -> Vec<JoinedRow> {
    vec![
        JoinedRow {
            phase: "stream".to_string(),
            value: 230,
        },
        JoinedRow {
            phase: "stream".to_string(),
            value: 231,
        },
        JoinedRow {
            phase: "eof".to_string(),
            value: 900,
        },
        JoinedRow {
            phase: "drain".to_string(),
            value: 901,
        },
    ]
}

fn assert_live_ports(ports: &ProofPorts) {
    assert_eq!(ports.reference_source_calls.load(Ordering::SeqCst), 2);
    assert_eq!(ports.stream_source_calls.load(Ordering::SeqCst), 3);
    assert_eq!(ports.effect_calls.load(Ordering::SeqCst), 2);
    assert_eq!(ports.join_calls.load(Ordering::SeqCst), 5);
    assert_eq!(ports.sink_handler_calls.load(Ordering::SeqCst), 4);
    assert_eq!(ports.physical_sink_calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        *ports
            .delivered
            .lock()
            .expect("delivered rows lock poisoned"),
        expected_deliveries()
    );
}

fn logging_evidence(event: &ChainEvent) -> Option<&LoggingEvidence> {
    let ChainEventContent::Observability(ObservabilityPayload::Middleware(
        MiddlewareLifecycle::Logging(evidence),
    )) = &event.content
    else {
        return None;
    };
    Some(evidence)
}

async fn stage_logging_rows(run_dir: &Path, stage: &str) -> Vec<EventEnvelope<ChainEvent>> {
    replay_testkit::read_stage_envelopes_appended(run_dir, stage)
        .await
        .into_iter()
        .filter(|envelope| logging_evidence(&envelope.event).is_some())
        .collect()
}

async fn logging_count(run_dir: &Path) -> usize {
    let mut total = 0;
    for (stage, _, _) in LOGGING_SURFACES {
        total += stage_logging_rows(run_dir, stage).await.len();
    }
    total
}

async fn assert_logging_contract(run_dir: &Path) {
    let mut taxonomy = BTreeMap::<&str, usize>::new();
    let mut total = 0;

    for (stage, expected_name, expected_count) in LOGGING_SURFACES {
        let rows = stage_logging_rows(run_dir, stage).await;
        let occurrence_debug = rows
            .iter()
            .filter_map(|row| logging_evidence(&row.event))
            .map(|evidence| format!("{:?}", evidence.occurrence()))
            .collect::<Vec<_>>();
        assert_eq!(
            rows.len(),
            expected_count,
            "unexpected logging cardinality on stage '{stage}': {occurrence_debug:#?}"
        );
        total += rows.len();

        for envelope in rows {
            let event = &envelope.event;
            let evidence = logging_evidence(event).expect("row was filtered as logging evidence");
            assert_eq!(
                evidence.event().as_str(),
                expected_name,
                "observer evidence must stay on its attached stage"
            );
            assert!(
                !serde_json::to_string(evidence)
                    .expect("logging evidence serialises")
                    .contains(PRIVATE_CANARY),
                "logging evidence copied an application payload field"
            );
            assert!(
                event.admission_seq.is_none(),
                "observer rows are not admitted"
            );

            let lane = event.causal_lane();
            assert!(matches!(lane, JournalCausalLane::ObserverEvidence(_)));
            assert!(
                envelope.vector_clock.get(&lane.clock_key()) > 0,
                "observer evidence must advance only its observer clock lane"
            );

            if let Some(input) = evidence.occurrence().input_reference() {
                assert!(input.stage_input_position > 0);
                assert!(
                    event.causality.parent_ids.contains(&input.event_id),
                    "the opaque input reference must name the causal parent"
                );
            } else {
                assert!(matches!(
                    evidence.occurrence(),
                    LoggingOccurrence::SourcePollObserved { .. }
                ));
                assert!(event.causality.is_root(), "source-poll evidence is a root");
            }

            let kind = match evidence.occurrence() {
                LoggingOccurrence::HandlerInputObserved { .. } => "handler_input",
                LoggingOccurrence::HandlerOutputObserved { .. } => "handler_output",
                LoggingOccurrence::StatefulInputObserved { .. } => "stateful_input",
                LoggingOccurrence::StatefulOutputObserved { .. } => "stateful_output",
                LoggingOccurrence::JoinInputObserved { .. } => "join_input",
                LoggingOccurrence::JoinOutputObserved { .. } => "join_output",
                LoggingOccurrence::SourcePollObserved { .. } => "source_poll",
                LoggingOccurrence::SinkDeliveryBoundaryObserved { .. } => "sink_delivery",
            };
            *taxonomy.entry(kind).or_default() += 1;
        }
    }

    assert_eq!(total, 34);
    assert_eq!(taxonomy["source_poll"], 5);
    assert_eq!(taxonomy["handler_input"], 4);
    assert_eq!(taxonomy["handler_output"], 4);
    assert_eq!(taxonomy["stateful_input"], 4);
    assert_eq!(taxonomy["stateful_output"], 3);
    assert_eq!(taxonomy["join_input"], 5);
    assert_eq!(taxonomy["join_output"], 5);
    assert_eq!(taxonomy["sink_delivery"], 4);

    let reference_rows = stage_logging_rows(run_dir, "references").await;
    assert_eq!(
        reference_rows
            .iter()
            .filter(|row| matches!(
                logging_evidence(&row.event).map(LoggingEvidence::occurrence),
                Some(LoggingOccurrence::SourcePollObserved {
                    outcome: LoggingSourceOutcome::Batch { events: 1 },
                    ..
                })
            ))
            .count(),
        1
    );
    assert_eq!(
        reference_rows
            .iter()
            .filter(|row| matches!(
                logging_evidence(&row.event).map(LoggingEvidence::occurrence),
                Some(LoggingOccurrence::SourcePollObserved {
                    outcome: LoggingSourceOutcome::Eof,
                    ..
                })
            ))
            .count(),
        1
    );

    let batch_rows = stage_logging_rows(run_dir, "batcher").await;
    assert_eq!(
        batch_rows
            .iter()
            .filter_map(|row| logging_evidence(&row.event))
            .filter(|evidence| matches!(
                evidence.occurrence(),
                LoggingOccurrence::StatefulOutputObserved {
                    output_count: 0,
                    ..
                }
            ))
            .count(),
        1,
        "the terminal zero-output stateful drain remains observable"
    );

    let join_rows = stage_logging_rows(run_dir, "joined").await;
    assert_eq!(
        join_rows
            .iter()
            .filter_map(|row| logging_evidence(&row.event))
            .filter(|evidence| matches!(
                evidence.occurrence(),
                LoggingOccurrence::JoinOutputObserved {
                    output_count: 0,
                    ..
                }
            ))
            .count(),
        2,
        "zero-output reference admission and stream calls remain observable"
    );
    let reference_side = join_rows
        .iter()
        .filter_map(|row| logging_evidence(&row.event))
        .filter(|evidence| {
            matches!(
                evidence.occurrence(),
                LoggingOccurrence::JoinInputObserved {
                    delivery,
                    ..
                } | LoggingOccurrence::JoinOutputObserved {
                    delivery,
                    ..
                } if delivery.side == LoggingJoinSide::Reference
            )
        })
        .count();
    assert_eq!(reference_side, 2);

    let sink_rows = stage_logging_rows(run_dir, "output").await;
    assert!(sink_rows
        .iter()
        .filter_map(|row| logging_evidence(&row.event))
        .all(|evidence| matches!(
            evidence.occurrence(),
            LoggingOccurrence::SinkDeliveryBoundaryObserved {
                outcome: LoggingSinkOutcome::Attempted {
                    result: LoggingSinkAttemptResult::ReportedSuccess
                },
                ..
            }
        )));

    let manifest = replay_testkit::archive_manifest(run_dir);
    let system_file = manifest["system_journal_file"]
        .as_str()
        .expect("manifest names the system journal");
    let system_log = std::fs::read_to_string(run_dir.join(system_file)).unwrap_or_default();
    for (_, event_name, _) in LOGGING_SURFACES {
        assert!(
            !system_log.contains(event_name),
            "logging evidence must not be duplicated into the system journal"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logging_is_observational_across_composition_and_strict_replay() {
    let temp = tempfile::tempdir().expect("logging composition tempdir");
    let control_base = temp.path().join("control");
    let treatment_base = temp.path().join("treatment");

    let control_ports = ProofPorts::default();
    run(&control_base, None, control_ports.clone(), false, false).await;
    assert_live_ports(&control_ports);
    let control_run = replay_testkit::latest_run_dir(&control_base);
    assert_eq!(logging_count(&control_run).await, 0);

    let treatment_ports = ProofPorts::default();
    run(&treatment_base, None, treatment_ports.clone(), true, false).await;
    assert_live_ports(&treatment_ports);
    let treatment_run = replay_testkit::latest_run_dir(&treatment_base);
    assert_logging_contract(&treatment_run).await;

    let control_vs_treatment =
        verify_run_dirs(&control_run, &treatment_run, &VerifyOptions::default())
            .expect("control/treatment verification executes");
    assert_eq!(
        control_vs_treatment.exit_code(),
        0,
        "logging must not alter any authoritative data/control projection"
    );
    assert_eq!(
        *control_ports
            .delivered
            .lock()
            .expect("control deliveries lock poisoned"),
        *treatment_ports
            .delivered
            .lock()
            .expect("treatment deliveries lock poisoned")
    );

    let replay_ports = ProofPorts::default();
    run(
        &treatment_base,
        Some(&treatment_run),
        replay_ports.clone(),
        true,
        true,
    )
    .await;
    let replay_run = replay_testkit::latest_run_dir(&treatment_base);
    assert_ne!(replay_run, treatment_run);

    assert_eq!(
        replay_ports.reference_source_calls.load(Ordering::SeqCst),
        0
    );
    assert_eq!(replay_ports.stream_source_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_ports.effect_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_ports.join_calls.load(Ordering::SeqCst), 5);
    assert_eq!(replay_ports.sink_handler_calls.load(Ordering::SeqCst), 4);
    assert_eq!(replay_ports.physical_sink_calls.load(Ordering::SeqCst), 0);
    assert!(
        replay_ports
            .delivered
            .lock()
            .expect("replay deliveries lock poisoned")
            .is_empty(),
        "strict replay must not touch the external sink port"
    );
    assert_eq!(logging_count(&replay_run).await, 0);

    let replay_verification =
        verify_run_dirs(&treatment_run, &replay_run, &VerifyOptions::default())
            .expect("live/replay verification executes");
    assert_eq!(
        replay_verification.exit_code(),
        0,
        "strict replay must be a fully certified match"
    );

    // Re-open the original archive after replay so the proof also detects any
    // accidental mutation of the source-of-truth evidence.
    assert_logging_contract(&treatment_run).await;
}
