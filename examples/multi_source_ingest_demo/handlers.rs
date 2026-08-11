// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler implementations for the multi_source_ingest_demo example.
//!
//! Three finite sources emit source-specific types; three typed map
//! alignment transforms normalise to `IngestedEvent`; one stateful
//! aggregator emits `IngestSummary`; one sink prints the summary.
//!
//! The interesting structural decision lives in `flow.rs`, not here. This
//! file is intentionally mechanical.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    SinkHandler, StatefulEmission, TypedFiniteSourceHandler, TypedStatefulHandler,
};
use obzenflow_runtime::stages::transform::MapTyped;

use crate::domain::{FileLine, IngestSummary, IngestedEvent, KafkaRawEvent, WebhookEnvelope};

// ─── Sources ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct KafkaSource {
    pub remaining: u32,
}

impl TypedFiniteSourceHandler for KafkaSource {
    type Output = KafkaRawEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![KafkaRawEvent {
            topic: "orders".to_string(),
            value: self.remaining as i64,
        }]))
    }
}

#[derive(Clone, Debug)]
pub struct WebhookSource {
    pub remaining: u32,
}

impl TypedFiniteSourceHandler for WebhookSource {
    type Output = WebhookEnvelope;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![WebhookEnvelope {
            source: "stripe".to_string(),
        }]))
    }
}

#[derive(Clone, Debug)]
pub struct FileSource {
    pub remaining: u32,
}

impl TypedFiniteSourceHandler for FileSource {
    type Output = FileLine;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        Ok(Some(vec![FileLine {
            line_no: self.remaining as usize,
        }]))
    }
}

// ─── Per-branch alignment transforms (the load-bearing pattern) ─────────
//
// Each alignment is a Map: it consumes the source-specific concrete type and
// emits the common IngestedEvent type. After this, the fan-in into the
// aggregator is type-homogeneous on IngestedEvent.

pub fn align_kafka_fn() -> MapTyped<
    KafkaRawEvent,
    IngestedEvent,
    impl Fn(KafkaRawEvent) -> IngestedEvent + Send + Sync + Clone,
> {
    MapTyped::new(|event: KafkaRawEvent| IngestedEvent {
        origin: format!("kafka:{}", event.topic),
    })
}

pub fn align_webhook_fn() -> MapTyped<
    WebhookEnvelope,
    IngestedEvent,
    impl Fn(WebhookEnvelope) -> IngestedEvent + Send + Sync + Clone,
> {
    MapTyped::new(|event: WebhookEnvelope| IngestedEvent {
        origin: format!("webhook:{}", event.source),
    })
}

pub fn align_file_fn(
) -> MapTyped<FileLine, IngestedEvent, impl Fn(FileLine) -> IngestedEvent + Send + Sync + Clone> {
    MapTyped::new(|_event: FileLine| IngestedEvent {
        origin: "file:/var/log/app.log".to_string(),
    })
}

// ─── Aggregator: homogeneous fan-in input, typed output ─────────────────

#[derive(Clone, Debug, Default)]
pub struct IngestAggregator;

impl TypedStatefulHandler for IngestAggregator {
    type State = IngestSummary;
    type Input = IngestedEvent;
    type Output = IngestSummary;

    fn accumulate(&self, state: &mut Self::State, event: IngestedEvent) {
        state.total += 1;
        *state.per_origin.entry(event.origin).or_insert(0) += 1;
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
