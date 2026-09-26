// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Explicit body treatments. The fallback delegates to the recorded payload's
//! serializer; application JSON keys never select framework presentation rules.

use super::{ClockView, Context, JournalNumber};
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

pub(in crate::render) enum BodyView<'a> {
    Verbatim(PayloadRef<'a>),
    ConsumptionProgress(ProgressView<'a>),
    MetricsExport(ClockView<'a>),
    EffectOutcome(EffectOutcomeView<'a>),
    EffectAttempt(EffectAttemptView<'a>),
    EffectRecovery(EffectRecoveryView<'a>),
}

pub(in crate::render) struct ProgressView<'a> {
    pub upstream: UpstreamView<'a>,
    pub sequence: SeqNo,
    pub eof_seen: bool,
    pub advertised: Option<SeqNo>,
    pub stalled: Option<DurationMs>,
    pub input_clock: Option<ClockView<'a>>,
    pub advertised_clock: Option<ClockView<'a>>,
}

pub(in crate::render) enum UpstreamView<'a> {
    Known {
        name: &'a str,
        journal: Option<JournalNumber>,
    },
    Unresolved {
        path: &'a str,
        index: JournalIndex,
    },
}

impl<'a> UpstreamView<'a> {
    fn from_input(path: &'a JournalPath, index: JournalIndex, context: &'a Context) -> Self {
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
                    name: &journal.name,
                    // A stage ID cannot identify one of several incarnations.
                    journal: journals
                        .next()
                        .is_none()
                        .then(|| context.journal_number(&journal.journal.id).into()),
                };
            }
        }
        Self::Unresolved {
            path: &path.0,
            index,
        }
    }
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
    pub fn from_record(record: &'a RunRecord, context: &'a Context) -> Self {
        match &record.record {
            RunRecordData::Chain(row) => match &row.payload {
                ChainPayload::FlowControl(FlowControlPayload::ConsumptionProgress {
                    reader_seq,
                    last_event_id: _,
                    vector_clock,
                    eof_seen,
                    reader_path,
                    reader_index,
                    advertised_writer_seq,
                    advertised_vector_clock,
                    stalled_since,
                }) => Self::ConsumptionProgress(ProgressView {
                    upstream: UpstreamView::from_input(reader_path, *reader_index, context),
                    sequence: *reader_seq,
                    eof_seen: *eof_seen,
                    advertised: *advertised_writer_seq,
                    stalled: *stalled_since,
                    input_clock: vector_clock
                        .as_ref()
                        .map(|clock| ClockView::new(&clock.clocks, context, None)),
                    advertised_clock: advertised_vector_clock
                        .as_ref()
                        .map(|clock| ClockView::new(&clock.clocks, context, None)),
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
                    watermark,
                }) => Self::MetricsExport(ClockView::new(&watermark.clocks, context, None)),
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
