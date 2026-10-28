// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Append-only teaching presentation. All meaning comes from the shared typed
//! record; colors, prose and observed-row counts carry no execution authority.

use super::{Error, ViewArgs};
use obzenflow::journal::read::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;

#[path = "render/context.rs"]
mod context;
#[path = "render/continuation.rs"]
mod continuation;
#[path = "render/event_counts.rs"]
mod event_counts;
#[path = "render/join_phase.rs"]
mod join_phase;
#[path = "render/payload.rs"]
mod payload;
#[path = "render/record_counts.rs"]
mod record_counts;
#[path = "render/summary.rs"]
mod summary;
#[path = "render/terminal.rs"]
mod terminal;
#[path = "render/view.rs"]
mod view;
use context::{event_id, parent_ids, Context};
use payload::{pretty, safe_text, wrap_fields};
use terminal::{OutputMode, RenderOptions, TerminalRenderer};
use view::{fact_error, replay_note, stage_heading, Category, EventView};

const MAX_PENDING: usize = 128;

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

pub(super) struct Observed<'a> {
    pub run: &'a RunIdentity,
    pub manifest: &'a RunManifest,
    pub progress: &'a RunReadProgress,
    pub lineage: Result<RunLineage, JournalReadError>,
    pub config_changes: Result<Vec<RunConfigChanges>, JournalReadError>,
}

impl<'a> Observed<'a> {
    pub(super) fn of(tail: &'a RunTail) -> Self {
        Self {
            run: tail.identity(),
            manifest: tail.manifest(),
            progress: tail.progress(),
            lineage: tail.lineage(),
            config_changes: tail.config_changes(),
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
    held_errors: BTreeMap<obzenflow_core::JournalId, VecDeque<RunRecord>>,
    printed_counts: BTreeMap<obzenflow_core::JournalId, u64>,
    records: u64,
    shown_records: u64,
    journals: BTreeMap<String, u64>,
    shown_journals: BTreeMap<String, u64>,
    event_types: BTreeMap<String, u64>,
    other_event_types: u64,
    event_counts: event_counts::EventCounts,
    record_counts: record_counts::RecordCounts,
    rejections: LineageRejections,
    continuations: continuation::Continuations,
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
            held_errors: BTreeMap::new(),
            printed_counts: BTreeMap::new(),
            records: 0,
            shown_records: 0,
            journals: BTreeMap::new(),
            shown_journals: BTreeMap::new(),
            event_types: BTreeMap::new(),
            other_event_types: 0,
            event_counts: event_counts::EventCounts::default(),
            record_counts: record_counts::RecordCounts::default(),
            rejections: LineageRejections::default(),
            continuations: continuation::Continuations::default(),
        }
    }

    pub(super) fn name_ancestor_journals(
        &mut self,
        journals: impl IntoIterator<Item = (RunJournal, String)>,
    ) {
        self.context.name_ancestor_journals(journals);
    }

    pub(super) fn begin(
        &mut self,
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
            for text in [
                "Clocks ⟨journal number:counter⟩. Numbers stay fixed in this view, including the final matrix.",
                "Facts orange · stateful/join outputs green · effects/deliveries pink · runtime gray",
                "Output ← stage(recorded inputs). The reporting journal's counter is underlined.",
                "Counters track journal history, including hidden runtime records (--include-runtime).",
                "Headings name stage kinds, or EFFECT/DELIVERY evidence. --explain adds context; --full shows complete records.",
            ] {
                for line in wrap_fields(&[text.into()], self.width) {
                    writeln!(output, "{}", self.dim(&line))?;
                }
            }
            writeln!(output)?;
            self.journal_legend(output)?;
        }
        output.flush()?;
        Ok(())
    }

    fn journal_legend(&mut self, output: &mut impl Write) -> Result<(), Error> {
        if self.jsonl || self.compact {
            return Ok(());
        }
        let labels = self.context.take_journal_labels();
        if labels.is_empty() {
            return Ok(());
        }
        writeln!(output, "{}", self.dim("Journal numbers:"))?;
        let fields: Vec<_> = labels
            .into_iter()
            .map(|(number, name)| format!("{number} {}", safe_text(&name)))
            .collect();
        for line in wrap_fields(&fields, self.width.saturating_sub(2)) {
            writeln!(output, "{}", self.dim(&format!("  {line}")))?;
        }
        writeln!(output)?;
        Ok(())
    }

    pub(super) fn lineage(
        &mut self,
        output: &mut impl Write,
        records: Vec<RunRecord>,
        rejections: LineageRejections,
    ) -> Result<(), Error> {
        self.rejections = rejections;
        for record in &records {
            self.record_counts
                .record(record, self.rejections.repeat_of(record).is_some());
            if !self.jsonl {
                self.context.remember(record);
                self.journal_legend(output)?;
            }
            self.terminal().render(
                output,
                &EventView::from_record(record, &self.context)
                    .held_by_earlier_run()
                    .repeating(self.rejections.repeat_of(record)),
            )?;
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
        self.record_counts
            .record(&record, self.rejections.repeat_of(&record).is_some());
        self.continuations.record(&record);
        self.context.join_phases.record(&record);
        if !self.jsonl {
            self.context.register_supervisor(&record)?;
        }
        if self.visible(&record) {
            self.shown_records += 1;
            *self.shown_journals.entry(journal).or_default() += 1;
            let descriptor = context::descriptor_label(&record);
            if let Some(count) = self.event_types.get_mut(&descriptor) {
                *count += 1;
            } else if self.event_types.len() < 1024 {
                self.event_types.insert(descriptor, 1);
            } else {
                self.other_event_types += 1;
            }
            if !self.jsonl {
                self.event_counts.record(&record);
            }
        }
        if self.jsonl {
            if self.visible(&record) {
                self.terminal().render(
                    output,
                    &EventView::from_record(&record, &self.context)
                        .repeating(self.rejections.repeat_of(&record)),
                )?;
            }
        } else {
            // Hidden rows still supply causal context and journal boundaries.
            // Filtering them before buffering could join non-adjacent facts.
            self.context.remember(&record);
            self.journal_legend(output)?;
            if record.journal.kind == RunJournalKind::Error {
                self.held_errors
                    .entry(record.journal.id)
                    .or_default()
                    .push_back(record);
                self.render_released(output, false)?;
            } else {
                self.pending.push_back(record);
            }
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
        self.render_released(output, true)?;
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
        self.render_counted(output, &records)?;
        self.render_released(output, false)
    }

    fn render_counted(
        &mut self,
        output: &mut impl Write,
        records: &[RunRecord],
    ) -> Result<(), Error> {
        for record in records {
            if let Some(count) = context::clock_count(record, &record.journal.id) {
                let printed = self.printed_counts.entry(record.journal.id).or_default();
                *printed = (*printed).max(count);
            }
        }
        self.render_group(output, records)
    }

    fn render_released(&mut self, output: &mut impl Write, force: bool) -> Result<(), Error> {
        loop {
            let released = self.release_held(force);
            if released.is_empty() {
                return Ok(());
            }
            self.render_counted(output, &released)?;
        }
    }

    fn release_held(&mut self, force: bool) -> Vec<RunRecord> {
        let (context, printed) = (&self.context, &self.printed_counts);
        let mut released = Vec::new();
        self.held_errors.retain(|_, held| {
            while held
                .front()
                .is_some_and(|record| force || !awaits_its_clock(context, printed, record))
            {
                released.extend(held.pop_front());
            }
            !held.is_empty()
        });
        released
    }

    fn render_group(&self, output: &mut impl Write, records: &[RunRecord]) -> Result<(), Error> {
        let terminal = self.terminal();
        for record in records.iter().filter(|record| self.visible(record)) {
            // Resolve the presentation only after pending parents have had a
            // chance to arrive. The view borrows the unmodified source record.
            terminal.render(
                output,
                &EventView::from_record(record, &self.context)
                    .repeating(self.rejections.repeat_of(record)),
            )?;
        }
        Ok(())
    }

    fn terminal(&self) -> TerminalRenderer {
        TerminalRenderer {
            options: RenderOptions {
                mode: if self.jsonl {
                    OutputMode::Jsonl
                } else if self.compact {
                    OutputMode::Compact
                } else {
                    OutputMode::Expanded {
                        full: self.full,
                        explain: self.explain,
                    }
                },
                width: self.width,
                color: self.color,
            },
        }
    }

    fn visible(&self, record: &RunRecord) -> bool {
        self.include_runtime
            || record.journal.kind == RunJournalKind::Error
            || Category::of(record) != Category::Runtime
            || matches!(&record.record, RunRecordData::Chain(row) if matches!(&row.payload, ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(_) | ExecutionPayload::SinkOperationFailed(_))))
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
        RunJournalKind::System => "pipeline",
        RunJournalKind::MetricsCoordination => "metrics/coordination",
        RunJournalKind::MetricsExport => "metrics/export",
        RunJournalKind::Data => "data",
        RunJournalKind::Error => "error",
    }
}

fn awaits_its_clock(
    context: &Context,
    printed: &BTreeMap<obzenflow_core::JournalId, u64>,
    record: &RunRecord,
) -> bool {
    context::clock(record).iter().any(|(coordinate, count)| {
        let journal = coordinate.journal_writer_id.as_journal_id();
        *journal != record.journal.id
            && context.in_run(journal)
            && printed.get(journal).is_none_or(|printed| printed < count)
    })
}

fn branchable(record: &RunRecord) -> bool {
    fact_error(record).is_none()
        && matches!(&record.record, RunRecordData::Chain(row) if matches!(row.payload, ChainPayload::Fact(_)) && !row.envelope.provenance.event.causality.parent_ids.is_empty())
}

fn same_branch(a: &RunRecord, b: &RunRecord) -> bool {
    if !branchable(b) || parent_ids(a) != parent_ids(b) || replay_note(a) != replay_note(b) {
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
