// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Borrowed presentation adapters. Projection never mutates evidence or writes
//! output. Human fields are resolved at render time; JSONL only needs source().

use super::context::{clock, event_type, Context};
use obzenflow::journal::read::*;
use obzenflow::journal::ProcessingStatus;
use obzenflow_core::event::{CausalCoordinate, CausalWitnesses};
use obzenflow_core::journal::causal::CausalProof;
use obzenflow_core::JournalId;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;

#[path = "view/body.rs"]
mod body;
pub(super) use body::{payload_clocks, BodyView, ProgressView, UpstreamView};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct JournalNumber(usize);

impl From<usize> for JournalNumber {
    fn from(number: usize) -> Self {
        Self(number)
    }
}

impl fmt::Display for JournalNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

pub(super) struct EventView<'a> {
    record: &'a RunRecord,
    context: &'a Context,
}

pub(super) struct HeaderView<'a> {
    pub category: Category,
    pub heading: &'static str,
    pub reporter: Cow<'a, str>,
    pub journal: JournalNumber,
    pub read_model: bool,
}

pub(super) enum InputsView<'a> {
    Source,
    Recorded(Vec<Option<&'a str>>),
    None,
}

pub(super) struct RelationView<'a> {
    pub output: &'a str,
    pub inputs: InputsView<'a>,
    pub replay: ReplayNote,
    pub processing_error: Option<&'a str>,
}

pub(super) struct ClockView<'a> {
    values: &'a BTreeMap<CausalCoordinate, u64>,
    context: &'a Context,
    reporting: Option<JournalId>,
}

pub(super) struct ClockCell {
    pub journal: JournalNumber,
    pub counter: u64,
    pub reporting: bool,
}

impl<'a> ClockView<'a> {
    pub fn new(
        values: &'a BTreeMap<CausalCoordinate, u64>,
        context: &'a Context,
        reporting: Option<JournalId>,
    ) -> Self {
        Self {
            values,
            context,
            reporting,
        }
    }

    pub fn cells(&self) -> Vec<ClockCell> {
        self.context
            .clock_components(self.values)
            .into_iter()
            .map(|component| ClockCell {
                journal: component.number.into(),
                counter: component.value,
                reporting: self.reporting.as_ref()
                    == Some(component.coordinate.journal_writer_id.as_journal_id()),
            })
            .collect()
    }

    pub fn same_history(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

pub(super) struct EvidenceView<'a> {
    pub proof: CausalProof,
    pub witnesses: &'a CausalWitnesses,
}

impl<'a> EventView<'a> {
    pub fn from_record(record: &'a RunRecord, context: &'a Context) -> Self {
        Self { record, context }
    }

    pub fn source(&self) -> &'a RunRecord {
        self.record
    }

    pub fn header(&self) -> HeaderView<'a> {
        let category = Category::of(self.record);
        let reporter = if let Some(stage) = &self.record.journal.stage {
            Cow::Borrowed(stage.key.as_str())
        } else {
            match &self.record.record {
                RunRecordData::System(row)
                    if *row.writer_id() == self.record.run.pipeline_writer_id =>
                {
                    Cow::Borrowed("pipeline")
                }
                RunRecordData::System(row) => Cow::Owned(
                    self.context
                        .writer_name(&row.writer_id().to_string())
                        .into(),
                ),
                _ => Cow::Borrowed("system"),
            }
        };
        HeaderView {
            category,
            heading: category.heading(self.record),
            reporter,
            journal: self.context.journal_number(&self.record.journal.id).into(),
            read_model: self.record.journal.stage.as_ref().is_some_and(|stage| {
                matches!(stage.stage_type, StageType::Stateful | StageType::Join)
            }),
        }
    }

    pub fn relation(&self) -> RelationView<'a> {
        let inputs = if self.record.kind == RunRecordKind::SourceFact {
            InputsView::Source
        } else if uses_inputs(self.record) {
            InputsView::Recorded(
                self.context
                    .inputs(self.record)
                    .into_iter()
                    .map(|reference| reference.map(|reference| reference.event_type.as_str()))
                    .collect(),
            )
        } else {
            InputsView::None
        };
        RelationView {
            output: event_type(self.record),
            inputs,
            replay: replay_note(self.record),
            processing_error: fact_error(self.record),
        }
    }

    pub fn clock(&self) -> ClockView<'a> {
        ClockView::new(
            clock(self.record),
            self.context,
            Some(self.record.journal.id),
        )
    }

    pub fn body(&self) -> BodyView<'a> {
        BodyView::from_record(self.record, self.context)
    }

    pub fn evidence(&self) -> EvidenceView<'a> {
        EvidenceView {
            proof: self.context.causal_proof(self.record),
            witnesses: self.record.causal_witnesses(),
        }
    }

    pub fn explanation(&self) -> &'static str {
        gloss(self.record)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReplayNote {
    None,
    Replayed,
    RecordedEffect,
}

pub(super) fn replay_note(record: &RunRecord) -> ReplayNote {
    if !replayed(record) {
        ReplayNote::None
    } else if record.kind == RunRecordKind::Effect || effect_fact(record) {
        ReplayNote::RecordedEffect
    } else {
        ReplayNote::Replayed
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Category {
    Fact,
    Effect,
    Delivery,
    Runtime,
}

impl Category {
    pub fn of(record: &RunRecord) -> Self {
        match &record.record {
            RunRecordData::Chain(row) => match &row.payload {
                ChainPayload::Fact(_) => Self::Fact,
                ChainPayload::Delivery(_) => Self::Delivery,
                ChainPayload::Execution(
                    ExecutionPayload::EffectRecord(_)
                    | ExecutionPayload::EffectAttemptStarted(_)
                    | ExecutionPayload::EffectRecoveryAbandoned(_),
                ) => Self::Effect,
                _ => Self::Runtime,
            },
            RunRecordData::System(_) => Self::Runtime,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Fact => "FACT",
            Self::Effect => "EFFECT",
            Self::Delivery => "DELIVERY",
            Self::Runtime => "RUNTIME",
        }
    }

    fn heading(self, record: &RunRecord) -> &'static str {
        if self != Self::Fact {
            return self.label();
        }
        record.journal.stage.as_ref().map_or("FACT", |stage| {
            stage_heading(stage.stage_type, stage.is_effectful)
        })
    }
}

pub(super) fn stage_heading(stage_type: StageType, is_effectful: bool) -> &'static str {
    match stage_type {
        StageType::FiniteSource | StageType::InfiniteSource => "SOURCE",
        StageType::Transform if is_effectful => "EFFECTFUL TRANSFORM",
        StageType::Transform => "TRANSFORM",
        StageType::Stateful if is_effectful => "EFFECTFUL STATEFUL",
        StageType::Stateful => "STATEFUL",
        StageType::Join => "JOIN",
        StageType::Sink => "SINK",
    }
}

fn effect_fact(record: &RunRecord) -> bool {
    matches!(&record.record, RunRecordData::Chain(row) if matches!(row.payload, ChainPayload::Fact(_)) && row.envelope.provenance.event.effect_provenance.is_some())
}

fn replayed(record: &RunRecord) -> bool {
    matches!(&record.record, RunRecordData::Chain(row) if row.envelope.provenance.event.replay_context.is_some())
}

fn uses_inputs(record: &RunRecord) -> bool {
    matches!(
        record.kind,
        RunRecordKind::StageOutput
            | RunRecordKind::CompositeData
            | RunRecordKind::Effect
            | RunRecordKind::Delivery
    )
}

pub(super) fn fact_error(record: &RunRecord) -> Option<&str> {
    if let RunRecordData::Chain(row) = &record.record {
        if let (ChainPayload::Fact(_), ProcessingStatus::Error { message, .. }) = (
            &row.payload,
            &row.envelope.provenance.event.processing.status,
        ) {
            return Some(message);
        }
    }
    None
}

fn gloss(record: &RunRecord) -> &'static str {
    if replayed(record) && (record.kind == RunRecordKind::Effect || effect_fact(record)) {
        return "Read from journal with replay provenance; this row does not establish that the effect fired again.";
    }
    if effect_fact(record) {
        return "Effects are data: the effect's outcome was recorded as a domain fact.";
    }
    match &record.record {
        RunRecordData::Chain(row) => match &row.payload {
            ChainPayload::Fact(_) if replayed(record) => "This fact carries replay provenance; the original flow and event are available in --full.",
            ChainPayload::Fact(_) => match record.journal.stage.as_ref().map(|s| s.stage_type) {
                Some(StageType::FiniteSource | StageType::InfiniteSource) => "Source stage admitted a domain fact into this run.",
                Some(StageType::Stateful) if record.journal.stage.as_ref().is_some_and(|stage| stage.is_effectful) => "Effectful stateful stage committed a domain fact; apply folds committed facts into state.",
                Some(StageType::Stateful) => "Stateful stage emitted a fact from its accumulated state.",
                Some(StageType::Join) => "Join stage emitted a fact using its recorded inputs.",
                Some(StageType::Transform) if record.journal.stage.as_ref().is_some_and(|stage| stage.is_effectful) => "Effectful transform emitted a domain fact; this fact alone does not imply an external call.",
                Some(StageType::Transform) => "Transform stage derived a fact from upstream input.",
                _ => "This stage recorded a domain fact; its recorded parents appear on the input side when available.",
            },
            ChainPayload::Execution(ExecutionPayload::EffectAttemptStarted(_)) => "The runtime recorded the start of an effect attempt; this does not prove external completion.",
            ChainPayload::Execution(ExecutionPayload::EffectRecord(_)) => "Effects are data: a committed outcome can be read during replay without executing the effect again.",
            ChainPayload::Execution(ExecutionPayload::EffectRecoveryAbandoned(_)) => "Recorded effect recovery was abandoned; this row is not a new effect invocation.",
            ChainPayload::Execution(ExecutionPayload::AccumulatorProgress { .. }) => "Stateful stage folded inputs into its accumulated state.",
            ChainPayload::Execution(ExecutionPayload::JoinReferenceProgress { .. }) => "Join stage accumulated reference inputs for matching later stream records.",
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(StageLifecycleFact::Drained { .. })) => "This stage finished draining; whole-run coverage is tracked separately.",
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(_)) => "The stage recorded a lifecycle transition.",
            ChainPayload::Execution(_) => "The runtime recorded execution evidence, such as admission, retry or backpressure.",
            ChainPayload::FlowControl(FlowControlPayload::ConsumptionProgress { .. }) => "Progress is the recorded input consumption or receipt sequence, not a journal append counter. Input and advertised clocks do not establish that the reader has caught up.",
            ChainPayload::FlowControl(FlowControlPayload::Eof { .. }) => "This journal recorded an end-of-input signal; it does not by itself settle the whole run.",
            ChainPayload::FlowControl(_) => "A flow signal coordinates delivery or progress between stages.",
            ChainPayload::Delivery(delivery) => match &delivery.result {
                DeliveryResult::Buffered { .. } => "The sink buffered this input; durable delivery is not yet confirmed.",
                DeliveryResult::Success { .. } => "The sink recorded a successful delivery outcome.",
                DeliveryResult::Failed { .. } => "The sink recorded a failed delivery outcome.",
                DeliveryResult::Partial { .. } => "The sink recorded a mixture of successful and failed deliveries.",
            },
            ChainPayload::CompositeData(_) => "A composite stage recorded protocol data; the original envelope and payload remain available in --full.",
        },
        RunRecordData::System(row) if matches!(row.payload, SystemPayload::PipelineLifecycle(_)) && *row.writer_id() != record.run.pipeline_writer_id => "This lifecycle row is from another writer; it does not establish this run's pipeline outcome.",
        RunRecordData::System(row) => match &row.payload {
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained) => "Pipeline drain was recorded; follow still verifies that every stage journal has been consumed.",
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed { .. }) => "Pipeline completion was recorded; drain and reader coverage are separate evidence.",
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Failed { .. }) => "Pipeline failure was recorded; successful inspection does not mean execution succeeded.",
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Cancelled { .. }) => "Pipeline cancellation was recorded; the viewer can still inspect committed evidence.",
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::NotStarted) => "The application closed this run before execution started.",
            SystemPayload::PipelineLifecycle(_) => "The pipeline recorded a lifecycle transition.",
            _ => "A system component recorded coordination or lifecycle evidence.",
        },
    }
}
