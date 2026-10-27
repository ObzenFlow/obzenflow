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
use serde::ser::{SerializeMap, SerializeStruct, Serializer};
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
    Execution(Untagged<'a, ExecutionPayload>),
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
    scope: &'a obzenflow_core::contracts::SubscriptionScope,
    consumed_count: obzenflow_core::event::types::Count,
    receipts: &'a Option<obzenflow_core::contracts::SubscriptionReceipts>,
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
                    scope,
                    consumed_count,
                    receipts,
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
                    scope,
                    consumed_count: *consumed_count,
                    receipts,
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
                ChainPayload::Execution(execution) => Self::Execution(Untagged(execution)),
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

pub(in crate::render) struct Untagged<'a, T>(&'a T);

const TAG: &str = "execution_type";

impl<T: Serialize> Serialize for Untagged<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(OmitTag(serializer))
    }
}

struct OmitTag<S>(S);

struct OmitField<S>(S);

struct OmitEntry<S> {
    map: S,
    skip_value: bool,
}

impl<S: SerializeStruct> SerializeStruct for OmitField<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), S::Error> {
        if key == TAG {
            return Ok(());
        }
        self.0.serialize_field(key, value)
    }

    fn skip_field(&mut self, key: &'static str) -> Result<(), S::Error> {
        self.0.skip_field(key)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.0.end()
    }
}

impl<S: SerializeMap> SerializeMap for OmitEntry<S> {
    type Ok = S::Ok;
    type Error = S::Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), S::Error> {
        self.skip_value = serde_json::to_value(key).is_ok_and(|key| key == TAG);
        if self.skip_value {
            return Ok(());
        }
        self.map.serialize_key(key)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), S::Error> {
        if std::mem::take(&mut self.skip_value) {
            return Ok(());
        }
        self.map.serialize_value(value)
    }

    fn end(self) -> Result<S::Ok, S::Error> {
        self.map.end()
    }
}

impl<S: Serializer> Serializer for OmitTag<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = S::SerializeSeq;
    type SerializeTuple = S::SerializeTuple;
    type SerializeTupleStruct = S::SerializeTupleStruct;
    type SerializeTupleVariant = S::SerializeTupleVariant;
    type SerializeMap = OmitEntry<S::SerializeMap>;
    type SerializeStruct = OmitField<S::SerializeStruct>;
    type SerializeStructVariant = S::SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> Result<S::Ok, S::Error> {
        self.0.serialize_bool(v)
    }
    fn serialize_i8(self, v: i8) -> Result<S::Ok, S::Error> {
        self.0.serialize_i8(v)
    }
    fn serialize_i16(self, v: i16) -> Result<S::Ok, S::Error> {
        self.0.serialize_i16(v)
    }
    fn serialize_i32(self, v: i32) -> Result<S::Ok, S::Error> {
        self.0.serialize_i32(v)
    }
    fn serialize_i64(self, v: i64) -> Result<S::Ok, S::Error> {
        self.0.serialize_i64(v)
    }
    fn serialize_u8(self, v: u8) -> Result<S::Ok, S::Error> {
        self.0.serialize_u8(v)
    }
    fn serialize_u16(self, v: u16) -> Result<S::Ok, S::Error> {
        self.0.serialize_u16(v)
    }
    fn serialize_u32(self, v: u32) -> Result<S::Ok, S::Error> {
        self.0.serialize_u32(v)
    }
    fn serialize_u64(self, v: u64) -> Result<S::Ok, S::Error> {
        self.0.serialize_u64(v)
    }
    fn serialize_f32(self, v: f32) -> Result<S::Ok, S::Error> {
        self.0.serialize_f32(v)
    }
    fn serialize_f64(self, v: f64) -> Result<S::Ok, S::Error> {
        self.0.serialize_f64(v)
    }
    fn serialize_char(self, v: char) -> Result<S::Ok, S::Error> {
        self.0.serialize_char(v)
    }
    fn serialize_str(self, v: &str) -> Result<S::Ok, S::Error> {
        self.0.serialize_str(v)
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<S::Ok, S::Error> {
        self.0.serialize_bytes(v)
    }
    fn serialize_none(self) -> Result<S::Ok, S::Error> {
        self.0.serialize_none()
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<S::Ok, S::Error> {
        self.0.serialize_some(value)
    }
    fn serialize_unit(self) -> Result<S::Ok, S::Error> {
        self.0.serialize_unit()
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<S::Ok, S::Error> {
        self.0.serialize_unit_struct(name)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<S::Ok, S::Error> {
        self.0.serialize_unit_variant(name, index, variant)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        self.0.serialize_newtype_struct(name, value)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<S::Ok, S::Error> {
        self.0
            .serialize_newtype_variant(name, index, variant, value)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<S::SerializeSeq, S::Error> {
        self.0.serialize_seq(len)
    }
    fn serialize_tuple(self, len: usize) -> Result<S::SerializeTuple, S::Error> {
        self.0.serialize_tuple(len)
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<S::SerializeTupleStruct, S::Error> {
        self.0.serialize_tuple_struct(name, len)
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<S::SerializeTupleVariant, S::Error> {
        self.0.serialize_tuple_variant(name, index, variant, len)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, S::Error> {
        Ok(OmitEntry {
            map: self.0.serialize_map(len)?,
            skip_value: false,
        })
    }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, S::Error> {
        Ok(OmitField(self.0.serialize_struct(name, len)?))
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<S::SerializeStructVariant, S::Error> {
        self.0.serialize_struct_variant(name, index, variant, len)
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
