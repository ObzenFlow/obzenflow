// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Domain types for the multi_source_ingest_demo example.
//!
//! Three source-specific concrete types (`KafkaRawEvent`, `WebhookEnvelope`,
//! `FileLine`) flow into a common `IngestedEvent` envelope via per-branch
//! alignment transforms, then a stateful aggregator emits `IngestSummary`.
//! See `flow.rs` for how these types compose into the topology.

use std::collections::BTreeMap;

use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

// ─── Source-specific types ────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaRawEvent {
    pub topic: String,
    pub value: i64,
}

impl TypedPayload for KafkaRawEvent {
    const EVENT_TYPE: &'static str = "ingest.kafka_raw";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEnvelope {
    pub source: String,
}

impl TypedPayload for WebhookEnvelope {
    const EVENT_TYPE: &'static str = "ingest.webhook";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileLine {
    pub line_no: usize,
}

impl TypedPayload for FileLine {
    const EVENT_TYPE: &'static str = "ingest.file_line";
}

// ─── Common envelope (post-alignment fan-in input type) ──────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestedEvent {
    pub origin: String,
}

impl TypedPayload for IngestedEvent {
    const EVENT_TYPE: &'static str = "ingest.normalised";
}

// ─── Aggregator output ────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IngestSummary {
    pub total: u64,
    pub per_origin: BTreeMap<String, u64>,
}

impl TypedPayload for IngestSummary {
    const EVENT_TYPE: &'static str = "ingest.summary";
}
