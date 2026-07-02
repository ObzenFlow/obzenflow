// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler implementations for the multi_source_ingest_demo example.
//!
//! Three finite sources emit source-specific types; three `Map`-based
//! alignment transforms normalise to `IngestedEvent`; one stateful
//! aggregator emits `IngestSummary`; one sink prints the summary.
//!
//! The interesting structural decision lives in `flow.rs`, not here. This
//! file is intentionally mechanical.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::id::StageId;
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
use obzenflow_runtime::stages::transform::Map;
use serde_json::json;

use crate::domain::{FileLine, IngestSummary, IngestedEvent, KafkaRawEvent, WebhookEnvelope};

// ─── Sources ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct KafkaSource {
    pub writer_id: WriterId,
    pub remaining: u32,
}

impl FiniteSourceHandler for KafkaSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
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
pub struct WebhookSource {
    pub writer_id: WriterId,
    pub remaining: u32,
}

impl FiniteSourceHandler for WebhookSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
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
pub struct FileSource {
    pub writer_id: WriterId,
    pub remaining: u32,
}

impl FiniteSourceHandler for FileSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
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

pub fn align_kafka_fn() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
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

pub fn align_webhook_fn() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event: ChainEvent| {
        let source = event.payload()["source"]
            .as_str()
            .unwrap_or("?")
            .to_string();
        let writer = WriterId::from(StageId::new());
        ChainEventFactory::data_event(
            writer,
            IngestedEvent::EVENT_TYPE,
            json!({"origin": format!("webhook:{source}")}),
        )
    })
}

pub fn align_file_fn() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
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
pub struct IngestAggregator;

#[async_trait]
impl StatefulHandler for IngestAggregator {
    type State = IngestSummary;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let origin = event.payload()["origin"]
            .as_str()
            .unwrap_or("?")
            .to_string();
        state.total += 1;
        *state.per_origin.entry(origin).or_insert(0) += 1;
    }

    fn initial_state(&self) -> Self::State {
        IngestSummary::default()
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
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
pub struct SummaryConsole;

#[async_trait]
impl SinkHandler for SummaryConsole {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        println!("=== IngestSummary ===");
        println!(
            "{}",
            serde_json::to_string_pretty(&event.payload()).unwrap_or_default()
        );
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Stdout".to_string()),
            None,
        ))
    }

    // Console print: re-delivery under either archive verb is safe.
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}
