// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c canonical example: heterogeneous fan-in via per-branch alignment.
//!
//! Three sources of three different concrete types feed one downstream
//! aggregator. The fan-in into the aggregator is type-homogeneous because
//! each branch is normalised through a small alignment transform first.
//!
//! Pictured:
//! ```text
//! kafka_source   : KafkaRawEvent  -> align_kafka   : KafkaRawEvent  -> IngestedEvent ┐
//! webhook_source : WebhookEnvelope -> align_webhook : WebhookEnvelope -> IngestedEvent ┼-> aggregator -> sink
//! file_source    : FileLine       -> align_file    : FileLine       -> IngestedEvent ┘
//! ```
//!
//! Every stage is typed; every edge is typed; every alignment step is a
//! visible stage. New authors writing N-input typed fan-in should read this
//! example first. See FLOWIP-114c "How to handle heterogeneous fan-in".

use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::id::StageId;
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
use obzenflow_runtime::stages::transform::Map;
use serde::{Deserialize, Serialize};
use serde_json::json;

// ─── Source-specific types ────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
struct KafkaRawEvent {
    topic: String,
    value: i64,
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

// ─── Common envelope (post-alignment fan-in input type) ──────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IngestedEvent {
    origin: String,
}

impl TypedPayload for IngestedEvent {
    const EVENT_TYPE: &'static str = "ingest.normalised";
}

// ─── Aggregator output ────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct IngestSummary {
    total: u64,
    per_origin: BTreeMap<String, u64>,
}

impl TypedPayload for IngestSummary {
    const EVENT_TYPE: &'static str = "ingest.summary";
}

// ─── Sources ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct KafkaSource {
    writer_id: WriterId,
    remaining: u32,
}

impl FiniteSourceHandler for KafkaSource {
    fn next(&mut self) -> std::result::Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            KafkaRawEvent::EVENT_TYPE,
            json!({"topic": "orders", "value": self.remaining as i64}),
        )]))
    }
}

#[derive(Clone, Debug)]
struct WebhookSource {
    writer_id: WriterId,
    remaining: u32,
}

impl FiniteSourceHandler for WebhookSource {
    fn next(&mut self) -> std::result::Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            WebhookEnvelope::EVENT_TYPE,
            json!({"source": "stripe"}),
        )]))
    }
}

#[derive(Clone, Debug)]
struct FileSource {
    writer_id: WriterId,
    remaining: u32,
}

impl FiniteSourceHandler for FileSource {
    fn next(&mut self) -> std::result::Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            FileLine::EVENT_TYPE,
            json!({"line_no": self.remaining as usize}),
        )]))
    }
}

// ─── Per-branch alignment transforms (the load-bearing pattern) ─────────
//
// Each alignment is a Map: it consumes the source-specific concrete type and
// emits the common IngestedEvent type. After this, the fan-in into the
// aggregator is type-homogeneous on IngestedEvent.

fn align_kafka_fn() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event: ChainEvent| {
        let topic = event.payload()["topic"].as_str().unwrap_or("?").to_string();
        let writer = WriterId::from(StageId::new());
        ChainEventFactory::data_event(
            writer,
            IngestedEvent::EVENT_TYPE,
            json!({"origin": format!("kafka:{topic}")}),
        )
    })
}

fn align_webhook_fn() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event: ChainEvent| {
        let source = event.payload()["source"].as_str().unwrap_or("?").to_string();
        let writer = WriterId::from(StageId::new());
        ChainEventFactory::data_event(
            writer,
            IngestedEvent::EVENT_TYPE,
            json!({"origin": format!("webhook:{source}")}),
        )
    })
}

fn align_file_fn() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|_event: ChainEvent| {
        let writer = WriterId::from(StageId::new());
        ChainEventFactory::data_event(
            writer,
            IngestedEvent::EVENT_TYPE,
            json!({"origin": "file:/var/log/app.log"}),
        )
    })
}

// ─── Aggregator: homogeneous fan-in input, typed output ─────────────────

#[derive(Clone, Debug, Default)]
struct IngestAggregator;

#[async_trait]
impl StatefulHandler for IngestAggregator {
    type State = IngestSummary;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let origin = event.payload()["origin"].as_str().unwrap_or("?").to_string();
        state.total += 1;
        *state.per_origin.entry(origin).or_insert(0) += 1;
    }

    fn initial_state(&self) -> Self::State {
        IngestSummary::default()
    }

    fn create_events(&self, state: &Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let writer_id = WriterId::from(StageId::new());
        let body = serde_json::to_value(state).map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(vec![ChainEventFactory::data_event(
            writer_id,
            IngestSummary::EVENT_TYPE,
            body,
        )])
    }
}

// ─── Sink ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct SummaryConsole;

#[async_trait]
impl SinkHandler for SummaryConsole {
    async fn consume(&mut self, event: ChainEvent) -> std::result::Result<DeliveryPayload, HandlerError> {
        println!("=== IngestSummary ===");
        println!("{}", serde_json::to_string_pretty(&event.payload()).unwrap_or_default());
        Ok(DeliveryPayload::success(
            "summary_console",
            DeliveryMethod::Custom("Stdout".to_string()),
            None,
        ))
    }
}

// ─── Flow definition ────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let kafka_writer = WriterId::from(StageId::new());
    let webhook_writer = WriterId::from(StageId::new());
    let file_writer = WriterId::from(StageId::new());

    let kafka_handler = KafkaSource {
        writer_id: kafka_writer,
        remaining: 3,
    };
    let webhook_handler = WebhookSource {
        writer_id: webhook_writer,
        remaining: 3,
    };
    let file_handler = FileSource {
        writer_id: file_writer,
        remaining: 3,
    };

    let definition = flow! {
        name: "multi_source_ingest_demo",
        journals: disk_journals(std::path::PathBuf::from("target/multi_source_ingest_demo")),
        middleware: [],

        stages: {
            kafka_source   = source!(KafkaRawEvent   => kafka_handler);
            webhook_source = source!(WebhookEnvelope => webhook_handler);
            file_source    = source!(FileLine        => file_handler);

            align_kafka   = transform!(KafkaRawEvent   -> IngestedEvent => align_kafka_fn());
            align_webhook = transform!(WebhookEnvelope -> IngestedEvent => align_webhook_fn());
            align_file    = transform!(FileLine        -> IngestedEvent => align_file_fn());

            aggregator   = stateful!(IngestedEvent -> IngestSummary => IngestAggregator);
            summary_sink = sink!(IngestSummary => SummaryConsole);
        },

        topology: {
            kafka_source   |> align_kafka;
            webhook_source |> align_webhook;
            file_source    |> align_file;

            align_kafka   |> aggregator;
            align_webhook |> aggregator;
            align_file    |> aggregator;

            aggregator |> summary_sink;
        }
    };

    FlowApplication::run(definition).await?;
    Ok(())
}
