// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::composite_data_payload::CompositeDataPayload;
use super::delivery_payload::DeliveryPayload;
use super::execution_payload::ExecutionPayload;
use super::flow_control_payload::FlowControlPayload;
use crate::event::chain_event::ReplayDisposition;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

/// Semantic record meaning, independent of transport selection or physical credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Fact,
    CompositeData,
    FlowSignal,
    Delivery,
    Execution,
    System,
}

#[derive(Debug, Clone)]
pub enum ChainPayload {
    Fact(Value),
    CompositeData(CompositeDataPayload),
    FlowControl(FlowControlPayload),
    Delivery(DeliveryPayload),
    Execution(ExecutionPayload),
}

impl ChainPayload {
    pub const fn kind(&self) -> EventKind {
        match self {
            Self::Fact(_) => EventKind::Fact,
            Self::CompositeData(_) => EventKind::CompositeData,
            Self::FlowControl(_) => EventKind::FlowSignal,
            Self::Delivery(_) => EventKind::Delivery,
            Self::Execution(_) => EventKind::Execution,
        }
    }

    pub fn framework_event_type(&self) -> Option<&'static str> {
        match self {
            Self::Fact(_) => None,
            Self::CompositeData(payload) => Some(payload.event_type()),
            Self::Execution(payload) => Some(payload.event_type()),
            Self::Delivery(_) => Some("sink.delivery"),
            Self::FlowControl(payload) => Some(match payload {
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
            }),
        }
    }

    pub fn decode(
        kind: EventKind,
        event_type: &str,
        value: Value,
    ) -> Result<Self, serde_json::Error> {
        let payload = match kind {
            EventKind::Fact => Self::Fact(value),
            EventKind::CompositeData => {
                Self::CompositeData(CompositeDataPayload::decode(event_type, value)?)
            }
            EventKind::FlowSignal => Self::FlowControl(serde_json::from_value(value)?),
            EventKind::Delivery => Self::Delivery(serde_json::from_value(value)?),
            EventKind::Execution => Self::Execution(serde_json::from_value(value)?),
            EventKind::System => {
                return Err(<serde_json::Error as serde::de::Error>::custom(
                    "system payload is not a chain record",
                ))
            }
        };
        if payload
            .framework_event_type()
            .is_some_and(|expected| expected != event_type)
        {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "event descriptor does not match payload",
            ));
        }
        Ok(payload)
    }

    pub const fn consumes_data_credit(&self) -> bool {
        match self {
            Self::Fact(_) | Self::CompositeData(_) => true,
            Self::Execution(payload) => payload.consumes_data_credit(),
            Self::FlowControl(_) | Self::Delivery(_) => false,
        }
    }
}

impl Serialize for ChainPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Fact(payload) => payload.serialize(serializer),
            Self::CompositeData(payload) => payload.serialize(serializer),
            Self::FlowControl(payload) => payload.serialize(serializer),
            Self::Delivery(payload) => payload.serialize(serializer),
            Self::Execution(payload) => payload.serialize(serializer),
        }
    }
}

impl EventKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fact => "fact",
            Self::CompositeData => "composite_data",
            Self::FlowSignal => "flow_signal",
            Self::Delivery => "delivery",
            Self::Execution => "execution",
            Self::System => "system",
        }
    }
}

impl ChainPayload {
    /// Body of an existing selected contract; framework effect history retains
    /// its typed body beneath the execution discriminator.
    pub fn contract_body(&self) -> Result<Value, serde_json::Error> {
        match self {
            Self::Execution(ExecutionPayload::EffectRecord(p)) => serde_json::to_value(p),
            Self::Execution(ExecutionPayload::EffectAttemptStarted(p)) => serde_json::to_value(p),
            Self::Execution(ExecutionPayload::EffectRecoveryAbandoned(p)) => {
                serde_json::to_value(p)
            }
            _ => serde_json::to_value(self),
        }
    }
}

impl ChainPayload {
    pub fn replay_disposition(&self) -> ReplayDisposition {
        use crate::event::chain_event::ReplayDisposition;
        match self {
            ChainPayload::Fact(_) | ChainPayload::CompositeData(_) => ReplayDisposition::ReAdmit,
            ChainPayload::Execution(payload) => match payload {
                // These two previously position-bearing source rows must retain
                // their disposition; the history loader also inspects them.
                ExecutionPayload::EffectAttemptStarted(_)
                | ExecutionPayload::EffectRecoveryAbandoned(_) => ReplayDisposition::ReAdmit,
                ExecutionPayload::EffectRecord(_)
                | ExecutionPayload::StageLifecycle(_)
                | ExecutionPayload::MetricsCoordination(_)
                | ExecutionPayload::CircuitBreaker(_)
                | ExecutionPayload::RateLimiter(_)
                | ExecutionPayload::Backpressure(_)
                | ExecutionPayload::SourcePollError(_)
                | ExecutionPayload::HttpPullState(_)
                | ExecutionPayload::AiChunkingPlanned(_)
                | ExecutionPayload::AccumulatorProgress { .. }
                | ExecutionPayload::JoinReferenceProgress { .. } => ReplayDisposition::ReAuthor,
            },
            ChainPayload::FlowControl(payload) => match payload {
                // The catch-up boundary's meaning is its stream position, so
                // it re-admits like Watermark; EOF re-authors because replay
                // reproduces source exhaustion (FLOWIP-120n F8).
                FlowControlPayload::Watermark { .. }
                | FlowControlPayload::CatchUpComplete { .. } => ReplayDisposition::ReAdmit,
                FlowControlPayload::Eof { .. }
                | FlowControlPayload::Checkpoint { .. }
                | FlowControlPayload::Drain
                | FlowControlPayload::PipelineAbort { .. }
                | FlowControlPayload::SourceContract { .. }
                | FlowControlPayload::ConsumptionProgress { .. }
                | FlowControlPayload::ConsumptionGap { .. }
                | FlowControlPayload::ConsumptionFinal { .. }
                | FlowControlPayload::ReaderStalled { .. }
                | FlowControlPayload::AtLeastOnceViolation { .. } => ReplayDisposition::ReAuthor,
            },
            ChainPayload::Delivery(_) => ReplayDisposition::ReAuthor,
        }
    }
}
