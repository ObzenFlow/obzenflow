// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! JSON body views. Known runtime events select useful tailing fields here;
//! JSON is the encoding, not a requirement to repeat the complete payload.
//! The fallback delegates to the recorded payload's serializer. Application
//! JSON keys never select framework presentation rules.

use super::{Context, JournalNumber};
use obzenflow::journal::read::*;
use obzenflow_core::event::types::{DurationMs, JournalIndex, JournalPath, SeqNo};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{
    EffectAttemptOrdinal, EffectAttemptStarted, EffectFailureCause, EffectOutcomePayload,
    EffectRecord, EffectRecoveryAbandoned, EffectType, MetricsCoordinationEvent,
};
use obzenflow_core::StageId;
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
    ConsumptionProgress(ProgressView<'a>),
    MetricsExport(MetricsExportView),
    EffectOutcome(EffectOutcomeView<'a>),
    EffectAttempt(EffectAttemptView<'a>),
    EffectRecovery(EffectRecoveryView<'a>),
}

#[derive(Serialize)]
pub(in crate::render) struct ProgressView<'a> {
    #[serde(flatten)]
    input: InputView<'a>,
    reader_seq: SeqNo,
    eof_seen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    advertised_writer_seq: Option<SeqNo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stalled_ms: Option<DurationMs>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum InputView<'a> {
    Known {
        upstream: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        upstream_journal: Option<JournalNumber>,
    },
    Unresolved {
        reader_path: &'a str,
        reader_index: JournalIndex,
    },
}

impl<'a> InputView<'a> {
    fn from_reader(path: &'a JournalPath, index: JournalIndex, context: &'a Context) -> Self {
        if let Ok(stage_id) = path.0.parse::<StageId>() {
            let mut journals = context.journals.values().filter(|journal| {
                journal.journal.kind == RunJournalKind::Data
                    && journal
                        .journal
                        .stage
                        .as_ref()
                        .is_some_and(|stage| stage.id == stage_id)
            });
            if let Some(journal) = journals.next() {
                return Self::Known {
                    upstream: &journal.name,
                    // A stage ID alone cannot select one physical incarnation
                    // when several are known. Never substitute reader_index.
                    upstream_journal: journals
                        .next()
                        .is_none()
                        .then(|| context.journal_number(&journal.journal.id).into()),
                };
            }
        }
        Self::Unresolved {
            reader_path: &path.0,
            reader_index: index,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(in crate::render) enum MetricsExportView {
    // The notice records snapshot publication. It does not assert that all
    // history was consumed or that an external metrics reader has fetched it.
    Exported,
}

#[derive(Serialize)]
pub(in crate::render) struct EffectOutcomeView<'a> {
    pub effect_type: &'a EffectType,
    pub observation: &'a obzenflow_core::event::payloads::effect_payload::EffectObservation,
    #[serde(flatten)]
    pub outcome: &'a EffectOutcomePayload,
}

impl<'a> From<&'a EffectRecord> for EffectOutcomeView<'a> {
    fn from(record: &'a EffectRecord) -> Self {
        Self {
            effect_type: &record.descriptor.effect_type,
            observation: &record.observation,
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
    pub fn from_record(record: &'a RunRecord, context: &'a Context) -> Self {
        match &record.record {
            RunRecordData::Chain(row) => match &row.payload {
                ChainPayload::FlowControl(FlowControlPayload::ConsumptionProgress {
                    reader_seq,
                    last_event_id: _,
                    vector_clock: _,
                    eof_seen,
                    reader_path,
                    reader_index,
                    advertised_writer_seq,
                    advertised_vector_clock: _,
                    stalled_since,
                }) => Self::ConsumptionProgress(ProgressView {
                    input: InputView::from_reader(reader_path, *reader_index, context),
                    reader_seq: *reader_seq,
                    eof_seen: *eof_seen,
                    advertised_writer_seq: *advertised_writer_seq,
                    stalled_ms: *stalled_since,
                }),
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
            RunRecordData::System(row) => match &row.payload {
                SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Exported {
                    watermark: _,
                }) => Self::MetricsExport(MetricsExportView::Exported),
                _ => Self::Verbatim(PayloadRef::from_record(record)),
            },
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
