// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Append-only teaching presentation. All meaning comes from the shared typed
//! record; colors, prose and observed-row counts carry no execution authority.

use super::{Error, ViewArgs};
use obzenflow::journal::read::*;
use std::collections::BTreeMap;
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
use context::Context;
use payload::{pretty, safe_text, wrap_fields};
use terminal::{OutputMode, RenderOptions, TerminalRenderer};
use view::{stage_heading, Category, EventView};

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
    run: Option<RunIdentity>,
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
            run: None,
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
        self.run = Some(run.clone());
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

    pub(super) fn lineage(&mut self, rejections: LineageRejections) {
        self.rejections = rejections;
    }

    pub(super) fn record(
        &mut self,
        output: &mut impl Write,
        record: RunRecord,
    ) -> Result<(), Error> {
        let this_run = self.run.as_ref().is_none_or(|run| *run == record.run);
        self.record_counts
            .record(&record, self.rejections.repeat_of(&record).is_some());
        if this_run {
            self.count(&record)?;
        }
        if !self.jsonl {
            self.context.remember(&record);
            self.journal_legend(output)?;
        }
        if self.visible(&record) {
            let view = EventView::from_record(&record, &self.context)
                .repeating(self.rejections.repeat_of(&record));
            if this_run {
                self.terminal().render(output, &view)?;
            } else {
                self.terminal()
                    .render(output, &view.held_by_earlier_run())?;
            }
        }
        output.flush()?;
        Ok(())
    }

    fn count(&mut self, record: &RunRecord) -> Result<(), Error> {
        let stage = record
            .journal
            .stage
            .as_ref()
            .map_or("system", |s| s.key.as_str());
        let journal = format!("{stage}/{}", journal_label(record.journal.kind));
        self.records += 1;
        *self.journals.entry(journal.clone()).or_default() += 1;
        self.continuations.record(record);
        self.context.join_phases.record(record);
        if !self.jsonl {
            self.context.register_supervisor(record)?;
        }
        if self.visible(record) {
            self.shown_records += 1;
            *self.shown_journals.entry(journal).or_default() += 1;
            let descriptor = context::descriptor_label(record);
            if let Some(count) = self.event_types.get_mut(&descriptor) {
                *count += 1;
            } else if self.event_types.len() < 1024 {
                self.event_types.insert(descriptor, 1);
            } else {
                self.other_event_types += 1;
            }
            if !self.jsonl {
                self.event_counts.record(record);
            }
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
