// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::composite_data_payload::CompositeDataPayload;
use super::delivery_payload::DeliveryPayload;
use super::execution_payload::ExecutionPayload;
use super::flow_control_payload::FlowControlPayload;
use crate::event::chain_event::ReplayDisposition;
use crate::event::vocabulary;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::num::NonZeroU32;

/// Semantic record meaning, independent of transport selection or physical credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    pub fn framework_schema_version(&self) -> Option<NonZeroU32> {
        match self {
            Self::Fact(_) => None,
            Self::CompositeData(payload) => Some(payload.payload_schema_version()),
            Self::Execution(payload) => Some(payload.payload_schema_version()),
            Self::FlowControl(_) | Self::Delivery(_) => Some(NonZeroU32::MIN),
        }
    }

    pub const fn kind(&self) -> EventKind {
        match self {
            Self::Fact(_) => EventKind::Fact,
            Self::CompositeData(_) => EventKind::CompositeData,
            Self::FlowControl(_) => EventKind::FlowSignal,
            Self::Delivery(_) => EventKind::Delivery,
            Self::Execution(_) => EventKind::Execution,
        }
    }

    pub fn framework_event_type(&self, stage_name: &str) -> Option<std::borrow::Cow<'static, str>> {
        match self {
            Self::Fact(_) => None,
            Self::CompositeData(payload) => Some(payload.event_type().into()),
            Self::Execution(payload) => Some(payload.event_type(stage_name)),
            Self::Delivery(payload) => Some(payload.event_type().into()),
            Self::FlowControl(payload) => Some(
                match payload {
                    FlowControlPayload::Eof { .. } => vocabulary::stream::EOF_DECLARED,
                    FlowControlPayload::Watermark { .. } => vocabulary::stream::WATERMARK_DECLARED,
                    FlowControlPayload::CatchUpComplete { .. } => {
                        vocabulary::stream::CATCH_UP_COMPLETED
                    }
                    FlowControlPayload::Checkpoint { .. } => {
                        vocabulary::stream::CHECKPOINT_DECLARED
                    }
                    FlowControlPayload::Drain => vocabulary::stream::DRAIN_REQUESTED,
                    FlowControlPayload::PipelineAbort { .. } => {
                        vocabulary::pipeline::ABORT_REQUESTED
                    }
                    FlowControlPayload::SourceContract { .. } => {
                        vocabulary::source::PRODUCTION_DECLARED
                    }
                    FlowControlPayload::ConsumptionProgress { .. } => {
                        vocabulary::subscription::PROGRESS_REPORTED
                    }
                    FlowControlPayload::ConsumptionGap { .. } => {
                        vocabulary::subscription::GAP_DETECTED
                    }
                    FlowControlPayload::ProductionFinal { .. } => {
                        vocabulary::source::PRODUCTION_FINALIZED
                    }
                    FlowControlPayload::ConsumptionFinal { .. } => {
                        vocabulary::subscription::CONSUMPTION_FINALIZED
                    }
                    FlowControlPayload::ReaderStalled { .. } => {
                        vocabulary::subscription::STALL_DETECTED
                    }
                    FlowControlPayload::AtLeastOnceViolation { .. } => {
                        vocabulary::subscription::AT_LEAST_ONCE_VIOLATED
                    }
                }
                .into(),
            ),
        }
    }

    pub fn decode(
        kind: EventKind,
        event_type: &str,
        payload_schema_version: NonZeroU32,
        value: Value,
    ) -> Result<Self, serde_json::Error> {
        if kind != EventKind::Fact {
            let expected = match (kind, event_type) {
                (
                    EventKind::CompositeData,
                    "ai.map_reduce.reduce_input" | "ai.map_reduce.chunk_failed",
                )
                | (EventKind::Execution, "obzenflow.sink_operation_failed") => 2,
                _ => 1,
            };
            if payload_schema_version.get() != expected {
                return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                    "unsupported payload schema version {payload_schema_version} for {event_type}: expected {expected}"
                )));
            }
        }
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
        // Stage names are validated against recorded flow context at the journal boundary.
        if !matches!(
            payload,
            Self::Execution(ExecutionPayload::StageLifecycle(_))
        ) && payload
            .framework_event_type("")
            .is_some_and(|expected| expected != event_type)
        {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "event descriptor does not match payload",
            ));
        }
        payload.validate_semantics()?;
        Ok(payload)
    }

    pub fn validate_semantics(&self) -> Result<(), serde_json::Error> {
        if let Self::Execution(ExecutionPayload::ContractResult {
            upstream,
            reader,
            contract_name,
            status,
            result,
            ..
        }) = self
        {
            let (name, result_upstream, result_reader) = result.subject();
            if name != contract_name
                || result_upstream != *upstream
                || result_reader != *reader
                || result.status() != *status
            {
                return Err(<serde_json::Error as serde::de::Error>::custom(
                    "contract result disagrees with its subject or status",
                ));
            }
        }
        match self {
            Self::Delivery(payload) => payload
                .validate()
                .map_err(<serde_json::Error as serde::de::Error>::custom)?,
            Self::Execution(ExecutionPayload::SinkAudit(payload)) => payload
                .validate()
                .map_err(<serde_json::Error as serde::de::Error>::custom)?,
            _ => {}
        }
        if let Self::Execution(ExecutionPayload::EffectRecord(record)) = self {
            record
                .validate()
                .map_err(<serde_json::Error as serde::de::Error>::custom)?;
            if matches!(
                record.outcome,
                super::effect_payload::EffectOutcomePayload::SucceededFact { .. }
            ) {
                return Err(<serde_json::Error as serde::de::Error>::custom(
                    "domain successes must retain their application descriptor",
                ));
            }
        }
        Ok(())
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
            Self::Execution(ExecutionPayload::StageFatalRecorded(p)) => serde_json::to_value(p),
            Self::Execution(ExecutionPayload::SinkOperationFailed(p)) => serde_json::to_value(p),
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
                ExecutionPayload::StageFatalRecorded(_)
                | ExecutionPayload::SinkOperationFailed(_)
                | ExecutionPayload::EffectAttemptStarted(_)
                | ExecutionPayload::EffectRecoveryAbandoned(_) => ReplayDisposition::ReAdmit,
                ExecutionPayload::SinkAudit(_)
                | ExecutionPayload::EffectRecord(_)
                | ExecutionPayload::ReplayLifecycle(_)
                | ExecutionPayload::SupervisorRegistered { .. }
                | ExecutionPayload::SupervisorCommandDiscarded { .. }
                | ExecutionPayload::SourceCleanupFailed { .. }
                | ExecutionPayload::ContractStatus { .. }
                | ExecutionPayload::ContractResult { .. }
                | ExecutionPayload::StageLifecycle(_)
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
                | FlowControlPayload::ProductionFinal { .. }
                | FlowControlPayload::ReaderStalled { .. }
                | FlowControlPayload::AtLeastOnceViolation { .. } => ReplayDisposition::ReAuthor,
            },
            ChainPayload::Delivery(_) => ReplayDisposition::ReAuthor,
        }
    }
}
