// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Append-only teaching presentation. All meaning comes from the shared typed
//! record; colors, prose and observed-row counts carry no execution authority.

use super::{Error, ViewArgs};
use obzenflow::journal::read::*;
use obzenflow::journal::ProcessingStatus;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;

#[path = "render/context.rs"]
mod context;
#[path = "render/event_counts.rs"]
mod event_counts;
#[path = "render/payload.rs"]
mod payload;
#[path = "render/summary.rs"]
mod summary;
use context::{clock, event_id, event_type, parent_ids, writer_id, Context};
use payload::{abbreviated, compact, pretty, safe_text, wrap_fields};

const MAX_PENDING: usize = 128;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Category {
    Fact,
    Effect,
    Delivery,
    Runtime,
}

impl Category {
    fn of(record: &RunRecord) -> Self {
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

    fn heading(self, record: &RunRecord, context: &Context) -> &'static str {
        if self != Self::Fact {
            return self.label();
        }
        match record.journal.stage.as_ref().map(|stage| stage.stage_type) {
            Some(StageType::FiniteSource | StageType::InfiniteSource) => "SOURCE",
            Some(StageType::Transform) if context.is_effectful(record) => "EFFECTFUL TRANSFORM",
            Some(StageType::Transform) => "TRANSFORM",
            Some(StageType::Stateful) if context.is_effectful(record) => "EFFECTFUL STATEFUL",
            Some(StageType::Stateful) => "STATEFUL",
            Some(StageType::Join) => "JOIN",
            Some(StageType::Sink) => "SINK",
            None => "FACT",
        }
    }
}

#[cfg(test)]
#[path = "render/tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub(super) enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

pub(super) enum ObservationEnd {
    Snapshot,
    Settled,
    Detached,
}

impl ObservationEnd {
    fn label(&self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot exhausted",
            Self::Settled => "settled execution covered",
            Self::Detached => "detached; application execution is independent",
        }
    }
}

pub(super) struct Renderer {
    jsonl: bool,
    full: bool,
    compact: bool,
    include_runtime: bool,
    color: bool,
    explain: bool,
    width: usize,
    context: Context,
    pending: VecDeque<RunRecord>,
    records: u64,
    shown_records: u64,
    journals: BTreeMap<String, u64>,
    shown_journals: BTreeMap<String, u64>,
    event_types: BTreeMap<String, u64>,
    other_event_types: u64,
    event_counts: event_counts::EventCounts,
}

impl Renderer {
    pub(super) fn new<'a>(
        view: &ViewArgs,
        terminal: bool,
        no_color: bool,
        journals: impl Iterator<Item = &'a RunJournal>,
    ) -> Self {
        Self {
            jsonl: view.jsonl,
            full: view.full,
            compact: view.compact,
            include_runtime: view.include_runtime,
            color: !view.jsonl
                && match view.color {
                    ColorMode::Auto => terminal && !no_color,
                    ColorMode::Always => true,
                    ColorMode::Never => false,
                },
            explain: view.explain,
            width: std::env::var("COLUMNS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(90)
                .clamp(40, 90),
            context: Context::new(journals),
            pending: VecDeque::new(),
            records: 0,
            shown_records: 0,
            journals: BTreeMap::new(),
            shown_journals: BTreeMap::new(),
            event_types: BTreeMap::new(),
            other_event_types: 0,
            event_counts: event_counts::EventCounts::default(),
        }
    }

    pub(super) fn begin(
        &self,
        output: &mut impl Write,
        run: &RunIdentity,
        follow: bool,
    ) -> Result<(), Error> {
        if !self.jsonl && !self.compact {
            writeln!(
                output,
                "Observing {} · {}",
                run.flow_id,
                if follow { "follow" } else { "snapshot" }
            )?;
            writeln!(output, "{}", self.dim("Clocks ⟨author@journal:sequence⟩"))?;
            writeln!(
                output,
                "{}",
                self.dim("Facts orange · stateful/join outputs green · effects/deliveries pink · runtime gray")
            )?;
            writeln!(output, "{}", self.dim("Output ← stage(recorded inputs). Clocks belong to outputs; the writer's digits are underlined."))?;
            writeln!(
                output,
                "{}",
                self.dim(
                    "Counters track writer history, including hidden runtime records (--include-runtime)."
                )
            )?;
            writeln!(output, "{}\n", self.dim("Headings name stage kinds, or EFFECT/DELIVERY evidence. … marks shortened values; --full shows complete records."))?;
        }
        output.flush()?;
        Ok(())
    }

    pub(super) fn record(
        &mut self,
        output: &mut impl Write,
        record: RunRecord,
    ) -> Result<(), Error> {
        let stage = record
            .journal
            .stage
            .as_ref()
            .map_or("system", |s| s.key.as_str());
        let journal = format!("{stage}/{}", journal_label(record.journal.kind));
        self.records += 1;
        *self.journals.entry(journal.clone()).or_default() += 1;
        if !self.jsonl {
            self.context.register_supervisor(&record)?;
        }
        if self.visible(&record) {
            self.shown_records += 1;
            *self.shown_journals.entry(journal).or_default() += 1;
            let event_type = event_type(&record);
            if let Some(count) = self.event_types.get_mut(event_type) {
                *count += 1;
            } else if self.event_types.len() < 1024 {
                self.event_types.insert(event_type.into(), 1);
            } else {
                self.other_event_types += 1;
            }
            if !self.jsonl {
                self.event_counts.record(&record);
            }
        }
        if self.jsonl {
            if self.visible(&record) {
                serde_json::to_writer(&mut *output, &record)?;
                writeln!(output)?;
            }
        } else {
            // Hidden rows still supply causal context and journal boundaries.
            // Filtering them before buffering could join non-adjacent facts.
            self.context.remember(&record);
            self.pending.push_back(record);
            self.drain_ready(output, false)?;
            if self.pending.len() >= MAX_PENDING {
                // Never let an absent/evicted parent stall a live display.
                self.render_pending(output, self.group_indices(0).0)?;
                self.drain_ready(output, false)?;
            }
        }
        output.flush()?;
        Ok(())
    }

    pub(super) fn flush_pending(&mut self, output: &mut impl Write) -> Result<(), Error> {
        self.drain_ready(output, true)?;
        output.flush()?;
        Ok(())
    }

    fn drain_ready(&mut self, output: &mut impl Write, force: bool) -> Result<(), Error> {
        loop {
            let mut journals = BTreeSet::new();
            let pending_ids: BTreeSet<_> = self.pending.iter().map(event_id).collect();
            let mut ready = None;
            for (index, record) in self.pending.iter().enumerate() {
                // Retain each journal's own order even when another journal's
                // head is waiting for its input to be observed.
                if !journals.insert(record.journal.id) {
                    continue;
                }
                let (indices, closed) = self.group_indices(index);
                let parents = parent_ids(record);
                if (force || closed)
                    && (force
                        || !(self.explain || self.full)
                        || indices.iter().all(|index| {
                            !matches!(
                                self.context.causal_proof(&self.pending[*index]),
                                obzenflow_core::journal::causal::CausalProof::Unresolved { .. }
                            )
                        }))
                    && self.context.parents_available(record)
                    && parents.iter().all(|parent| !pending_ids.contains(parent))
                {
                    ready = Some(indices);
                    break;
                }
            }
            let indices = match ready {
                Some(indices) => indices,
                None if force && !self.pending.is_empty() => self.group_indices(0).0,
                None => break,
            };
            self.render_pending(output, indices)?;
        }
        Ok(())
    }

    fn group_indices(&self, first: usize) -> (Vec<usize>, bool) {
        let record = &self.pending[first];
        let mut indices = vec![first];
        if self.compact || self.full || !branchable(record) {
            return (indices, true);
        }
        for (index, next) in self.pending.iter().enumerate().skip(first + 1) {
            if next.journal.id != record.journal.id {
                continue;
            }
            if same_branch(record, next) {
                indices.push(index);
            } else {
                return (indices, true);
            }
        }
        (indices, false)
    }

    fn render_pending(
        &mut self,
        output: &mut impl Write,
        indices: Vec<usize>,
    ) -> Result<(), Error> {
        let mut records: Vec<_> = indices
            .into_iter()
            .rev()
            .map(|index| self.pending.remove(index).expect("pending index"))
            .collect();
        records.reverse();
        self.render_group(output, &records)
    }

    fn render_group(&self, output: &mut impl Write, records: &[RunRecord]) -> Result<(), Error> {
        let first = &records[0];
        if !self.visible(first) {
            return Ok(());
        }
        let category = Category::of(first);
        let stage = safe_text(&self.speaker(first));
        let input = self.input_name(first);
        for record in records {
            let heading = category.heading(record, &self.context);
            let value = display_payload(record)?;
            // The recorded output leads once; the arrow explains its origin.
            // Sources have no upstream event argument. This is observation,
            // not evaluation or a claim that the stage is a pure function.
            let output_type = safe_text(event_type(record));
            let mut relation = if record.kind == RunRecordKind::SourceFact {
                format!("{output_type} ← {stage}()")
            } else if uses_inputs(record) {
                format!("{output_type} ← {stage}({input})")
            } else {
                format!("{output_type} ← {stage}")
            };
            relation.push_str(replay_label(record));
            if fact_error(record).is_some() {
                relation.push_str(" [processing error]");
            }
            if self.compact {
                let line = abbreviated(
                    &format!("{heading}  {relation}  {}", compact(&value)),
                    self.width,
                );
                writeln!(output, "{}", self.record_text(record, &line, false))?;
                continue;
            }
            writeln!(output, "{}", self.record_text(record, heading, false))?;
            for line in wrap_fields(&[relation], self.width) {
                writeln!(output, "{}", self.record_text(record, &line, false))?;
            }
            writeln!(output, "{}", self.record_clock(record))?;
            if self.explain || self.full {
                writeln!(
                    output,
                    "causal proof: {}",
                    serde_json::to_string(&self.context.causal_proof(record))?
                )?;
                writeln!(
                    output,
                    "committed witnesses: {}",
                    serde_json::to_string(record.causal_witnesses())?
                )?;
            }
            if let Some(message) = fact_error(record) {
                for line in wrap_fields(
                    &[format!("processing error: {}", safe_text(message))],
                    self.width,
                ) {
                    writeln!(output, "{line}")?;
                }
            }
            self.payload_lines(output, &value, category)?;
            if self.explain {
                for line in wrap_fields(&[gloss(record, &self.context).into()], self.width) {
                    writeln!(output, "{}", self.dim(&line))?;
                }
            }
            if self.full {
                // Serialized JSON escapes untrusted text; it is never styled.
                serde_json::to_writer_pretty(&mut *output, record)?;
                writeln!(output)?;
            }
            writeln!(output)?;
        }
        Ok(())
    }

    fn input_name(&self, record: &RunRecord) -> String {
        let inputs = self.context.inputs(record);
        if inputs.is_empty() {
            return "input not recorded".into();
        }
        let mut labels: Vec<_> = inputs
            .iter()
            .take(3)
            .map(|reference| match reference {
                Some(reference) => safe_text(&reference.event_type),
                None => "recorded input (unresolved)".into(),
            })
            .collect();
        if inputs.len() > 3 {
            labels.push(format!("{} more recorded parents", inputs.len() - 3));
        }
        labels.join(", ")
    }

    fn payload_lines(
        &self,
        output: &mut impl Write,
        value: &Value,
        category: Category,
    ) -> Result<(), Error> {
        let (json, shortened) = pretty(value, self.width);
        for line in json.lines() {
            let line = if category == Category::Runtime {
                self.dim(line)
            } else {
                line.into()
            };
            writeln!(output, "{line}")?;
        }
        if shortened {
            writeln!(
                output,
                "{}",
                self.dim("Payload shortened; --full shows complete values.")
            )?;
        }
        Ok(())
    }

    fn speaker(&self, record: &RunRecord) -> String {
        if let Some(stage) = &record.journal.stage {
            return stage.key.clone();
        }
        match &record.record {
            RunRecordData::System(row) if *row.writer_id() == record.run.pipeline_writer_id => {
                "pipeline".into()
            }
            RunRecordData::System(row) => self
                .context
                .writer_name(&row.writer_id().to_string())
                .into(),
            _ => "system".into(),
        }
    }

    fn record_clock(&self, record: &RunRecord) -> String {
        let values = clock(record);
        let writer = writer_id(record);
        let runtime = Category::of(record) == Category::Runtime;
        let components = self.context.clock_components(
            values,
            uses_inputs(record) || record.kind == RunRecordKind::SourceFact,
            &record.run,
        );
        let components = components
            .into_iter()
            .map(|component| {
                let digits = component.value.to_string();
                let digits = if component.coordinate.writer_id.to_string() == writer
                    && component.coordinate.journal_writer_id.as_journal_id() == &record.journal.id
                    && !runtime
                {
                    self.record_text(record, &digits, true)
                } else {
                    self.dim(&digits)
                };
                format!(
                    "{}{digits}",
                    self.dim(&format!("{}:", safe_text(&component.name)))
                )
            })
            .collect::<Vec<_>>();
        format!(
            "{}{}{}",
            self.dim("⟨"),
            components.join(&self.dim(",")),
            self.dim("⟩")
        )
    }

    fn record_text(&self, record: &RunRecord, text: &str, writer: bool) -> String {
        let text = safe_text(text);
        if !self.color {
            return text;
        }
        let category = Category::of(record);
        let color = match category {
            Category::Fact => match record.journal.stage.as_ref().map(|stage| stage.stage_type) {
                // Read-model styling is a stage-role cue. These remain facts
                // in the journal; no mutable state snapshot is inferred.
                Some(StageType::Stateful | StageType::Join) => 114, // green
                _ => 208,                                           // orange
            },
            Category::Effect | Category::Delivery => 217, // light pink
            Category::Runtime => 245,                     // gray
        };
        let emphasis = if writer {
            "1;4;"
        } else if category == Category::Fact {
            "1;"
        } else {
            ""
        };
        format!("\x1b[{emphasis}38;5;{color}m{text}\x1b[0m")
    }

    fn visible(&self, record: &RunRecord) -> bool {
        self.include_runtime || Category::of(record) != Category::Runtime
    }

    fn dim(&self, text: &str) -> String {
        if self.color {
            format!("\x1b[38;5;245m{text}\x1b[0m")
        } else {
            text.into()
        }
    }
}

fn journal_label(kind: RunJournalKind) -> &'static str {
    match kind {
        RunJournalKind::System => "system",
        RunJournalKind::Data => "data",
        RunJournalKind::Error => "error",
    }
}

fn effect_fact(record: &RunRecord) -> bool {
    matches!(&record.record, RunRecordData::Chain(row) if matches!(row.payload, ChainPayload::Fact(_)) && row.envelope.provenance.event.effect_provenance.is_some())
}

fn replayed(record: &RunRecord) -> bool {
    matches!(&record.record, RunRecordData::Chain(row) if row.envelope.provenance.event.replay_context.is_some())
}

fn replay_label(record: &RunRecord) -> &'static str {
    if !replayed(record) {
        ""
    } else if record.kind == RunRecordKind::Effect || effect_fact(record) {
        " [read from journal]"
    } else {
        " [replay]"
    }
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

fn branchable(record: &RunRecord) -> bool {
    fact_error(record).is_none()
        && matches!(&record.record, RunRecordData::Chain(row) if matches!(row.payload, ChainPayload::Fact(_)) && !row.envelope.provenance.event.causality.parent_ids.is_empty())
}

fn fact_error(record: &RunRecord) -> Option<&str> {
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

fn same_branch(a: &RunRecord, b: &RunRecord) -> bool {
    if !branchable(b) || parent_ids(a) != parent_ids(b) || replay_label(a) != replay_label(b) {
        return false;
    }
    match (&a.record, &b.record) {
        (RunRecordData::Chain(a), RunRecordData::Chain(b)) => {
            // Effect grouping uses the recorded outcome-group identity as well
            // as parents. A derived fact must not masquerade as an effect fact.
            match (
                &a.envelope.provenance.event.effect_provenance,
                &b.envelope.provenance.event.effect_provenance,
            ) {
                (None, None) => true,
                (Some(a), Some(b)) => a.cursor == b.cursor && a.group_id == b.group_id,
                _ => false,
            }
        }
        _ => false,
    }
}

fn display_payload(record: &RunRecord) -> Result<Value, Error> {
    Ok(match &record.record {
        RunRecordData::Chain(row) => match &row.payload {
            // The outcome is the teaching surface. Cursor hashes and descriptor
            // plumbing remain available in --full and --jsonl records.
            ChainPayload::Execution(ExecutionPayload::EffectRecord(effect)) => {
                let mut payload = serde_json::to_value(&effect.outcome)?;
                if let Value::Object(fields) = &mut payload {
                    fields.insert(
                        "effect_type".into(),
                        serde_json::to_value(&effect.descriptor.effect_type)?,
                    );
                }
                payload
            }
            ChainPayload::Execution(ExecutionPayload::EffectAttemptStarted(effect)) => {
                serde_json::json!({"effect_type": effect.effect_type, "attempt": effect.attempt})
            }
            ChainPayload::Execution(ExecutionPayload::EffectRecoveryAbandoned(effect)) => {
                serde_json::json!({"effect_type": effect.effect_type, "cause": effect.cause, "message": effect.message, "highest_started_attempt": effect.highest_started_attempt})
            }
            _ => serde_json::to_value(&row.payload)?,
        },
        RunRecordData::System(row) => serde_json::to_value(&row.payload)?,
    })
}

fn gloss(record: &RunRecord, context: &Context) -> &'static str {
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
                Some(StageType::Stateful) if context.is_effectful(record) => "Effectful stateful stage committed a domain fact; apply folds committed facts into state.",
                Some(StageType::Stateful) => "Stateful stage emitted a fact from its accumulated state.",
                Some(StageType::Join) => "Join stage emitted a fact using its recorded inputs.",
                Some(StageType::Transform) if context.is_effectful(record) => "Effectful transform emitted a domain fact; this fact alone does not imply an external call.",
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
