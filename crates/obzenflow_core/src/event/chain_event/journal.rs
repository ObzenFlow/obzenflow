// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ChainEvent, ChainEventContent};
use crate::event::journal_event::{JournalAdmissionRole, JournalCausalLane, JournalEvent, Sealed};
use crate::event::payloads::flow_control_payload::FlowControlPayload;
use crate::event::payloads::observability_payload::{
    MetricsLifecycle, MiddlewareLifecycle, ObservabilityPayload,
};
use crate::event::types::{AdmissionSeq, EventId, WriterId};

impl Sealed for ChainEvent {}

fn is_observer_evidence(content: &ChainEventContent) -> bool {
    match content {
        ChainEventContent::Data { .. }
        | ChainEventContent::FlowControl(_)
        | ChainEventContent::Delivery(_) => false,
        ChainEventContent::Observability(observability) => match observability {
            ObservabilityPayload::Stage(_)
            | ObservabilityPayload::Metrics(_)
            | ObservabilityPayload::Backpressure(_) => false,
            ObservabilityPayload::Middleware(middleware) => match middleware {
                MiddlewareLifecycle::CircuitBreaker(_) | MiddlewareLifecycle::RateLimiter(_) => {
                    false
                }
                MiddlewareLifecycle::Indicator(_) => true,
            },
        },
    }
}

impl JournalEvent for ChainEvent {
    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    fn admission_role(&self) -> JournalAdmissionRole {
        if is_observer_evidence(&self.content) {
            JournalAdmissionRole::ObserverEvidence
        } else {
            JournalAdmissionRole::Flow
        }
    }

    fn causal_lane(&self) -> JournalCausalLane {
        if is_observer_evidence(&self.content) {
            JournalCausalLane::ObserverEvidence(self.writer_id)
        } else {
            JournalCausalLane::Flow(self.writer_id)
        }
    }

    fn admission_seq(&self) -> Option<AdmissionSeq> {
        self.admission_seq
    }

    fn set_admission_seq(&mut self, seq: AdmissionSeq) {
        self.admission_seq = Some(seq);
    }

    fn clear_admission_seq(&mut self) {
        self.admission_seq = None;
    }

    /// Zero-alloc category string for metrics & fast logs.
    /// Falls back to generic labels when the name is dynamic (e.g. custom metrics).
    fn event_type_name(&self) -> &'static str {
        match &self.content {
            ChainEventContent::Data { .. } => "data",
            ChainEventContent::FlowControl(sig) => match sig {
                FlowControlPayload::Eof { .. } => "control.eof",
                FlowControlPayload::Watermark { .. } => "control.watermark",
                FlowControlPayload::CatchUpComplete { .. } => "control.catch_up_complete",
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
                    MetricsLifecycle::HttpPullSnapshot { .. } => {
                        "lifecycle.metrics.http_pull_snapshot"
                    }
                    _ => "lifecycle.metrics",
                },
                ObservabilityPayload::Middleware(_) => "lifecycle.middleware",
                ObservabilityPayload::Backpressure(_) => "lifecycle.backpressure",
            },
        }
    }
}
