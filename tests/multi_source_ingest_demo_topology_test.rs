// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c regression test: structural assertions on the
//! `multi_source_ingest_demo` example.
//!
//! We construct the same topology shape as the example (3 sources of 3
//! different types -> 3 alignment transforms -> 1 aggregator -> 1 sink) and
//! assert that:
//!
//! 1. Every stage has `typing_metadata().is_some()`.
//! 2. `validate_stage_typing_metadata` accepts every stage.
//! 3. `validate_edge_typing` returns `Ok(())`.
//! 4. The three inbound edges into the aggregator all carry the same
//!    `IngestedEvent` `TypeId` (homogeneous fan-in).
//!
use std::collections::HashMap;

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::id::StageId;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::stage_descriptor::StageDescriptor;
use obzenflow_dsl::dsl::typing::{validate_edge_typing, validate_stage_typing_metadata, TypeHint};
use obzenflow_dsl::{sink, source, stateful, transform};
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    StatefulEmission, TypedFiniteSourceHandler, TypedStatefulHandler,
};
use obzenflow_runtime::stages::transform::MapTyped;
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct KafkaRawEvent {
    topic: String,
}
impl TypedPayload for KafkaRawEvent {
    const EVENT_TYPE: &'static str = "ingest.kafka_raw";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WebhookEnvelope {
    source: String,
}
impl TypedPayload for WebhookEnvelope {
    const EVENT_TYPE: &'static str = "ingest.webhook";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileLine {
    line_no: usize,
}
impl TypedPayload for FileLine {
    const EVENT_TYPE: &'static str = "ingest.file_line";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IngestedEvent {
    origin: String,
}
impl TypedPayload for IngestedEvent {
    const EVENT_TYPE: &'static str = "ingest.normalised";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct IngestSummary {
    total: u64,
}
impl TypedPayload for IngestSummary {
    const EVENT_TYPE: &'static str = "ingest.summary";
}

#[derive(Clone, Debug)]
struct OneShotSource<P>(std::marker::PhantomData<P>);
impl<P> OneShotSource<P> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}
impl TypedFiniteSourceHandler for OneShotSource<KafkaRawEvent> {
    type Output = KafkaRawEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}
impl TypedFiniteSourceHandler for OneShotSource<WebhookEnvelope> {
    type Output = WebhookEnvelope;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}
impl TypedFiniteSourceHandler for OneShotSource<FileLine> {
    type Output = FileLine;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

fn align_to_ingested<P>(
) -> MapTyped<P, IngestedEvent, impl Fn(P) -> IngestedEvent + Send + Sync + Clone>
where
    P: TypedPayload + Send + Sync + 'static,
{
    MapTyped::new(|_event: P| IngestedEvent {
        origin: "test".to_string(),
    })
}

#[derive(Clone, Debug, Default)]
struct IngestAggregator;
impl TypedStatefulHandler for IngestAggregator {
    type State = IngestSummary;
    type Input = IngestedEvent;
    type Output = IngestSummary;

    fn accumulate(&self, state: &mut Self::State, _event: IngestedEvent) {
        state.total += 1;
    }
    fn initial_state(&self) -> Self::State {
        IngestSummary::default()
    }
    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: state.clone(),
            outputs: vec![state.clone()],
        })
    }
}

#[derive(Clone, Debug)]
struct NullSink;
#[async_trait]
impl InlineSink for NullSink {
    type Input = IngestSummary;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: IngestSummary,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
    }
}

#[test]
fn multi_source_ingest_demo_satisfies_typed_fan_in_invariants() {
    let kafka_id = StageId::new();
    let webhook_id = StageId::new();
    let file_id = StageId::new();
    let align_kafka_id = StageId::new();
    let align_webhook_id = StageId::new();
    let align_file_id = StageId::new();
    let aggregator_id = StageId::new();
    let sink_id = StageId::new();
    let kafka_source_handler = OneShotSource::<KafkaRawEvent>::new();
    let webhook_source_handler = OneShotSource::<WebhookEnvelope>::new();
    let file_source_handler = OneShotSource::<FileLine>::new();
    let align_kafka_handler = align_to_ingested::<KafkaRawEvent>();
    let align_webhook_handler = align_to_ingested::<WebhookEnvelope>();
    let align_file_handler = align_to_ingested::<FileLine>();
    let aggregator_handler = IngestAggregator;
    let summary_sink_handler = NullSink;

    let mut descriptors: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
    descriptors.insert(
        "kafka_source".to_string(),
        source!(name: "kafka_source", KafkaRawEvent => kafka_source_handler),
    );
    descriptors.insert(
        "webhook_source".to_string(),
        source!(
            name: "webhook_source",
            WebhookEnvelope => webhook_source_handler
        ),
    );
    descriptors.insert(
        "file_source".to_string(),
        source!(name: "file_source", FileLine => file_source_handler),
    );
    descriptors.insert(
        "align_kafka".to_string(),
        transform!(name: "align_kafka", KafkaRawEvent -> IngestedEvent => align_kafka_handler),
    );
    descriptors.insert(
        "align_webhook".to_string(),
        transform!(
            name: "align_webhook",
            WebhookEnvelope -> IngestedEvent => align_webhook_handler
        ),
    );
    descriptors.insert(
        "align_file".to_string(),
        transform!(name: "align_file", FileLine -> IngestedEvent => align_file_handler),
    );
    descriptors.insert(
        "aggregator".to_string(),
        stateful!(name: "aggregator", IngestedEvent -> IngestSummary => aggregator_handler),
    );
    descriptors.insert(
        "summary_sink".to_string(),
        sink!(name: "summary_sink", IngestSummary => summary_sink_handler),
    );

    let mut name_to_id = HashMap::new();
    name_to_id.insert("kafka_source".to_string(), kafka_id);
    name_to_id.insert("webhook_source".to_string(), webhook_id);
    name_to_id.insert("file_source".to_string(), file_id);
    name_to_id.insert("align_kafka".to_string(), align_kafka_id);
    name_to_id.insert("align_webhook".to_string(), align_webhook_id);
    name_to_id.insert("align_file".to_string(), align_file_id);
    name_to_id.insert("aggregator".to_string(), aggregator_id);
    name_to_id.insert("summary_sink".to_string(), sink_id);

    // Invariant 1: every stage has typing metadata.
    for (name, desc) in &descriptors {
        assert!(
            desc.typing_metadata().is_some(),
            "stage '{name}' must have typing_metadata()"
        );
    }

    // Invariant 2: stage-metadata validator accepts every stage.
    validate_stage_typing_metadata(&descriptors)
        .expect("multi_source_ingest_demo: stage typing must validate clean");

    // Invariant 3: edge validator returns Ok(()).
    let mut topology = TopologyBuilder::new();
    for (name, id, kind) in [
        ("kafka_source", kafka_id, TopologyStageType::FiniteSource),
        (
            "webhook_source",
            webhook_id,
            TopologyStageType::FiniteSource,
        ),
        ("file_source", file_id, TopologyStageType::FiniteSource),
        ("align_kafka", align_kafka_id, TopologyStageType::Transform),
        (
            "align_webhook",
            align_webhook_id,
            TopologyStageType::Transform,
        ),
        ("align_file", align_file_id, TopologyStageType::Transform),
        ("aggregator", aggregator_id, TopologyStageType::Stateful),
        ("summary_sink", sink_id, TopologyStageType::Sink),
    ] {
        topology.add_stage_with_id(id.to_topology_id(), Some(name.to_string()), kind);
        topology.reset_current();
    }
    topology.add_edge(kafka_id.to_topology_id(), align_kafka_id.to_topology_id());
    topology.add_edge(
        webhook_id.to_topology_id(),
        align_webhook_id.to_topology_id(),
    );
    topology.add_edge(file_id.to_topology_id(), align_file_id.to_topology_id());
    topology.add_edge(
        align_kafka_id.to_topology_id(),
        aggregator_id.to_topology_id(),
    );
    topology.add_edge(
        align_webhook_id.to_topology_id(),
        aggregator_id.to_topology_id(),
    );
    topology.add_edge(
        align_file_id.to_topology_id(),
        aggregator_id.to_topology_id(),
    );
    topology.add_edge(aggregator_id.to_topology_id(), sink_id.to_topology_id());
    let topology = topology.build_unchecked().unwrap();

    validate_edge_typing(&topology, &descriptors, &name_to_id, &[])
        .expect("multi_source_ingest_demo: edge validator must return Ok(())");

    // Invariant 4: the three alignment transforms emit IngestedEvent.
    let expected = TypeHint::exact_payload::<IngestedEvent>();
    for align_name in ["align_kafka", "align_webhook", "align_file"] {
        let meta = descriptors[align_name]
            .typing_metadata()
            .expect("alignment transform has metadata");
        assert_eq!(
            meta.output_type, expected,
            "{align_name} must emit IngestedEvent (homogeneous fan-in pre-condition)"
        );
    }
    // And the aggregator consumes IngestedEvent.
    let agg_meta = descriptors["aggregator"]
        .typing_metadata()
        .expect("aggregator has metadata");
    assert_eq!(agg_meta.input_type, expected);
}
