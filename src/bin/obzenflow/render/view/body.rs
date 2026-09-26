// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! JSON body views. The fallback delegates to the recorded payload's serializer;
//! application JSON keys never select framework presentation rules.

use obzenflow::journal::read::*;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{
    EffectAttemptOrdinal, EffectAttemptStarted, EffectFailureCause, EffectOutcomePayload,
    EffectRecord, EffectRecoveryAbandoned, EffectType, MetricsCoordinationEvent,
};
use serde::Serialize;

#[derive(Serialize)]
#[serde(untagged)]
pub(in crate::render) enum PayloadRef<'a> {
    Chain(&'a ChainPayload),
    System(&'a SystemPayload),
}

impl<'a> PayloadRef<'a> {
    fn from_record(record: &'a RunRecord) -> Self {
        match &record.record {
            RunRecordData::Chain(row) => Self::Chain(&row.payload),
            RunRecordData::System(row) => Self::System(&row.payload),
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub(in crate::render) enum BodyView<'a> {
    Verbatim(PayloadRef<'a>),
    EffectOutcome(EffectOutcomeView<'a>),
    EffectAttempt(EffectAttemptView<'a>),
    EffectRecovery(EffectRecoveryView<'a>),
}

#[derive(Serialize)]
pub(in crate::render) struct EffectOutcomeView<'a> {
    pub effect_type: &'a EffectType,
    #[serde(flatten)]
    pub outcome: &'a EffectOutcomePayload,
}

impl<'a> From<&'a EffectRecord> for EffectOutcomeView<'a> {
    fn from(record: &'a EffectRecord) -> Self {
        Self {
            effect_type: &record.descriptor.effect_type,
            outcome: &record.outcome,
        }
    }
}

#[derive(Serialize)]
pub(in crate::render) struct EffectAttemptView<'a> {
    pub effect_type: &'a EffectType,
    pub attempt: EffectAttemptOrdinal,
}

impl<'a> From<&'a EffectAttemptStarted> for EffectAttemptView<'a> {
    fn from(record: &'a EffectAttemptStarted) -> Self {
        Self {
            effect_type: &record.effect_type,
            attempt: record.attempt,
        }
    }
}

#[derive(Serialize)]
pub(in crate::render) struct EffectRecoveryView<'a> {
    pub effect_type: &'a EffectType,
    pub cause: &'a EffectFailureCause,
    pub message: &'a str,
    pub highest_started_attempt: EffectAttemptOrdinal,
}

impl<'a> From<&'a EffectRecoveryAbandoned> for EffectRecoveryView<'a> {
    fn from(record: &'a EffectRecoveryAbandoned) -> Self {
        Self {
            effect_type: &record.effect_type,
            cause: &record.cause,
            message: &record.message,
            highest_started_attempt: record.highest_started_attempt,
        }
    }
}

impl<'a> BodyView<'a> {
    pub fn from_record(record: &'a RunRecord) -> Self {
        match &record.record {
            RunRecordData::Chain(row) => match &row.payload {
                ChainPayload::Execution(ExecutionPayload::EffectRecord(effect)) => {
                    Self::EffectOutcome(effect.into())
                }
                ChainPayload::Execution(ExecutionPayload::EffectAttemptStarted(effect)) => {
                    Self::EffectAttempt(effect.into())
                }
                ChainPayload::Execution(ExecutionPayload::EffectRecoveryAbandoned(effect)) => {
                    Self::EffectRecovery(effect.into())
                }
                _ => Self::Verbatim(PayloadRef::from_record(record)),
            },
            RunRecordData::System(_) => Self::Verbatim(PayloadRef::from_record(record)),
        }
    }
}

/// Discover typed payload-clock identities on observation, including hidden
/// records, so --explain and --full never allocate aliases in rendering order.
pub(in crate::render) fn payload_clocks(record: &RunRecord) -> impl Iterator<Item = &VectorClock> {
    let clocks = match &record.record {
        RunRecordData::Chain(row) => match &row.payload {
            ChainPayload::FlowControl(FlowControlPayload::ConsumptionProgress {
                vector_clock,
                advertised_vector_clock,
                ..
            }) => [vector_clock.as_ref(), advertised_vector_clock.as_ref()],
            _ => [None, None],
        },
        RunRecordData::System(row) => match &row.payload {
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Exported {
                watermark,
            }) => [Some(watermark), None],
            _ => [None, None],
        },
    };
    clocks.into_iter().flatten()
}
