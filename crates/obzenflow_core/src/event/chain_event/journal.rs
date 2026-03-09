// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ChainEvent, ChainEventContent};
use crate::event::journal_event::{JournalEvent, Sealed};
use crate::event::payloads::flow_control_payload::FlowControlPayload;
use crate::event::payloads::observability_payload::{MetricsLifecycle, ObservabilityPayload};
use crate::event::types::{EventId, WriterId};

impl Sealed for ChainEvent {}

impl JournalEvent for ChainEvent {
    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    /// Zero-alloc category string for metrics & fast logs.
    /// Falls back to generic labels when the name is dynamic (e.g. custom metrics).
    fn event_type_name(&self) -> &'static str {
        match &self.content {
            ChainEventContent::Data { .. } => "data",
            ChainEventContent::FlowControl(sig) => match sig {
                FlowControlPayload::Eof { .. } => "control.eof",
                FlowControlPayload::Watermark { .. } => "control.watermark",
                FlowControlPayload::Checkpoint { .. } => "control.checkpoint",
                FlowControlPayload::Drain => "control.drain",
                FlowControlPayload::PipelineAbort { .. } => "control.pipeline_abort",
                FlowControlPayload::SourceContract { .. } => "control.source_contract",
                FlowControlPayload::ConsumptionProgress { .. } => "control.consumption_progress",
                FlowControlPayload::ConsumptionGap { .. } => "control.consumption_gap",
                FlowControlPayload::ConsumptionFinal { .. } => "control.consumption_final",
                FlowControlPayload::ReaderStalled { .. } => "control.reader_stalled",
                FlowControlPayload::AtLeastOnceViolation { .. } => {
                    "control.at_least_once_violation"
                }
            },
            ChainEventContent::Delivery(_) => "sink.delivery",
            ChainEventContent::Observability(obs) => match obs {
                ObservabilityPayload::Stage(_) => "lifecycle.stage",
                ObservabilityPayload::Metrics(m) => match m {
                    MetricsLifecycle::Custom { .. } => "lifecycle.metrics.custom",
                    _ => "lifecycle.metrics",
                },
                ObservabilityPayload::Middleware(_) => "lifecycle.middleware",
            },
        }
    }
}
