// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Group physical journal counts by their manifest owner, independently of
//! event authorship. Reader descriptions reflect the current framework wiring;
//! application data edges come from the recorded topology.

use super::*;
use crate::render::event_counts::JournalEventCounts;

const JOURNAL_INDENT: usize = 4;
const TABLE_INDENT: usize = 6;

impl Renderer {
    pub(super) fn event_summary(
        &self,
        output: &mut impl Write,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        let mut journals: Vec<_> = self.event_counts.journals().collect();
        // Pipeline first, then both metrics journals, then application stages
        // in inventory order. A forwarded author's identity never changes this.
        journals.sort_by_key(|counts| {
            (
                self.event_owner_order(&counts.journal),
                matches!(
                    counts.journal.kind,
                    RunJournalKind::Error | RunJournalKind::MetricsExport
                ),
                counts.journal.id,
            )
        });
        if journals.is_empty() {
            return Ok(());
        }
        writeln!(output)?;
        for line in [
            "Event counts cover displayed entries in each journal, including replayed evidence.",
            "Journals with no displayed entries remain in the inventory above.",
            "Each journal has one owner; an owner can have several journals.",
            "Runtime readers show framework wiring; CLI/Studio can also observe journal histories.",
        ] {
            self.summary_line(output, MUTED, line)?;
        }
        let mut previous_owner = None;
        let mut previous_section = None;
        for counts in journals {
            let owner = self.event_owner_order(&counts.journal);
            let application = owner >= 2;
            if previous_section != Some(application) {
                writeln!(output)?;
                self.summary_line(
                    output,
                    HEADING,
                    if application {
                        "APPLICATION STAGES"
                    } else {
                        "SYSTEM SUPERVISORS"
                    },
                )?;
                previous_section = Some(application);
            }
            writeln!(output)?;
            if previous_owner != Some(owner) {
                self.event_owner_heading(output, &counts.journal, manifest)?;
                self.summary_indented(output, BODY, 2, "Owns:")?;
                previous_owner = Some(owner);
            }
            self.event_journal_heading(output, &counts.journal, manifest)?;
            self.event_count_table(output, counts)?;
        }
        writeln!(output)?;
        self.summary_line(
            output,
            MUTED,
            "Each stage writes business outputs to its own data journal for subscribers to read.",
        )?;
        writeln!(output)?;
        for line in [
            "- Forwarded control signals keep their original Author.",
            "- EOF from all required upstreams lets a supervisor drain and complete.",
        ] {
            self.summary_line(output, MUTED, line)?;
        }
        Ok(())
    }

    fn event_owner_order(&self, journal: &RunJournal) -> usize {
        match journal.kind {
            RunJournalKind::System => 0,
            RunJournalKind::MetricsCoordination | RunJournalKind::MetricsExport => 1,
            RunJournalKind::Data | RunJournalKind::Error => {
                self.context
                    .stages
                    .iter()
                    .position(|stage| {
                        journal
                            .stage
                            .as_ref()
                            .is_some_and(|owner| stage.key == owner.key)
                    })
                    .unwrap_or(self.context.stages.len())
                    + 2
            }
        }
    }

    fn summary_owner_name(
        &self,
        writer: &obzenflow_core::event::WriterId,
        fallback: &'static str,
    ) -> &str {
        self.context
            .supervisors
            .get(&writer.to_string())
            .map_or(fallback, |descriptor| descriptor.name.as_str())
    }

    fn event_owner_heading(
        &self,
        output: &mut impl Write,
        journal: &RunJournal,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        match journal.kind {
            RunJournalKind::System => {
                let name =
                    self.summary_owner_name(&manifest.pipeline_writer_id, "pipeline_supervisor");
                self.summary_line(output, HEADING, &format!("Supervisor: {name}"))?;
                let mut inputs = Vec::new();
                if !manifest.stages.is_empty() {
                    inputs.push("stage data journals (supervision)".to_owned());
                }
                inputs.push(format!("{} (self)", manifest.system_journal_file));
                if let Some(metrics) = &manifest.metrics_journals {
                    inputs.push(metrics.coordination_journal_file.clone());
                }
                self.summary_indented(
                    output,
                    BODY,
                    2,
                    &format!("Reads from: {}", inputs.join(", ")),
                )?;
            }
            RunJournalKind::MetricsCoordination | RunJournalKind::MetricsExport => {
                let metrics = manifest
                    .metrics_journals
                    .as_ref()
                    .ok_or("metrics journal is missing its manifest entry")?;
                let name = self.summary_owner_name(&metrics.writer_id, "metrics_aggregator");
                self.summary_line(output, HEADING, &format!("Supervisor: {name}"))?;
                let inputs = if manifest.stages.is_empty() {
                    manifest.system_journal_file.clone()
                } else {
                    format!(
                        "stage data/error journals, {}",
                        manifest.system_journal_file
                    )
                };
                self.summary_indented(
                    output,
                    BODY,
                    2,
                    &format!("Reads from: {inputs} (metrics tails)"),
                )?;
            }
            RunJournalKind::Data | RunJournalKind::Error => {
                let owner = journal
                    .stage
                    .as_ref()
                    .ok_or("stage journal is missing its recorded stage identity")?;
                let stage = manifest
                    .stages
                    .get(&owner.key)
                    .ok_or("stage journal is missing its manifest entry")?;
                let mut inputs: Vec<_> = stage.inbound.iter().map(String::as_str).collect();
                inputs.sort_unstable();
                inputs.dedup();
                let mut subscribers: Vec<_> = manifest
                    .stages
                    .iter()
                    .filter(|(_, candidate)| candidate.inbound.contains(&owner.key))
                    .map(|(key, _)| key.as_str())
                    .collect();
                subscribers.sort_unstable();
                let heading = stage_heading(stage.stage_type, stage.is_effectful);
                self.summary_line(output, HEADING, &format!("{heading}: {}", owner.key))?;
                for (label, stages) in [("Reads from", inputs), ("Data subscribers", subscribers)] {
                    let value = if stages.is_empty() {
                        "—".into()
                    } else {
                        stages.join(", ")
                    };
                    self.summary_indented(output, BODY, 2, &format!("{label}: {value}"))?;
                }
            }
        }
        Ok(())
    }

    fn event_journal_heading(
        &self,
        output: &mut impl Write,
        journal: &RunJournal,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        let pipeline = self.summary_owner_name(&manifest.pipeline_writer_id, "pipeline_supervisor");
        let metrics = manifest
            .metrics_journals
            .as_ref()
            .map(|metrics| self.summary_owner_name(&metrics.writer_id, "metrics_aggregator"));
        let (file, purpose, mut readers) = match journal.kind {
            RunJournalKind::System => (
                &manifest.system_journal_file,
                "pipeline lifecycle and coordination",
                vec![format!("{pipeline} (self)")],
            ),
            RunJournalKind::MetricsCoordination | RunJournalKind::MetricsExport => {
                let metrics = manifest
                    .metrics_journals
                    .as_ref()
                    .ok_or("metrics journal is missing its manifest entry")?;
                if journal.kind == RunJournalKind::MetricsCoordination {
                    (
                        &metrics.coordination_journal_file,
                        "lifecycle and parent coordination",
                        vec![pipeline.to_owned()],
                    )
                } else {
                    (
                        &metrics.export_journal_file,
                        "export notices and freshness",
                        vec![],
                    )
                }
            }
            RunJournalKind::Data | RunJournalKind::Error => {
                let owner = journal
                    .stage
                    .as_ref()
                    .ok_or("stage journal is missing its recorded stage identity")?;
                let stage = manifest
                    .stages
                    .get(&owner.key)
                    .ok_or("stage journal is missing its manifest entry")?;
                if journal.kind == RunJournalKind::Data {
                    (
                        &stage.data_journal_file,
                        "data, control and stage lifecycle",
                        vec![pipeline.to_owned()],
                    )
                } else {
                    (&stage.error_journal_file, "processing errors", vec![])
                }
            }
        };
        if matches!(
            journal.kind,
            RunJournalKind::System | RunJournalKind::Data | RunJournalKind::Error
        ) {
            if let Some(metrics) = metrics {
                readers.push(metrics.to_owned());
            }
        }
        self.summary_indented(output, HEADING, JOURNAL_INDENT, file)?;
        self.summary_indented(output, BODY, TABLE_INDENT, &format!("Purpose: {purpose}"))?;
        let readers = if readers.is_empty() {
            "—".into()
        } else {
            readers.join(", ")
        };
        self.summary_indented(
            output,
            BODY,
            TABLE_INDENT,
            &format!("Runtime readers: {readers}"),
        )?;
        Ok(())
    }

    fn event_count_table(
        &self,
        output: &mut impl Write,
        counts: &JournalEventCounts,
    ) -> Result<(), Error> {
        // Retain every cell's text even in a narrow terminal. Nesting consumes
        // columns, so headers wrap by the same rules as their values.
        let count_width = counts
            .event_types
            .values()
            .map(|count| count.to_string().len())
            .max()
            .unwrap_or(0)
            .max(5);
        let minimums = [1, 1, 1];
        let mut widths = [10, 6, 11]; // Event type, Author, Author type.
        for (event_type, writer) in counts.event_types.keys() {
            let kind = self
                .context
                .supervisors
                .get(writer)
                .map_or("Not recorded", |descriptor| descriptor.kind.label());
            for (width, text) in
                widths
                    .iter_mut()
                    .zip([event_type.as_str(), self.context.writer_name(writer), kind])
            {
                *width = (*width).max(safe_text(text).chars().count());
            }
        }
        let available = self.width.saturating_sub(count_width + TABLE_INDENT + 6);
        while widths.iter().sum::<usize>() > available {
            let column = (0..3)
                .max_by_key(|&index| widths[index] - minimums[index])
                .unwrap();
            if widths[column] == minimums[column] {
                break;
            }
            widths[column] -= 1;
        }
        self.event_count_row(
            output,
            MUTED,
            "Count",
            ["Event type", "Author", "Author type"],
            count_width,
            widths,
        )?;
        let mut rows: Vec<_> = counts.event_types.iter().collect();
        rows.sort_by_key(|((event_type, writer), _)| {
            (
                event_type.as_str(),
                self.context.writer_name(writer),
                writer.as_str(),
            )
        });
        for ((event_type, writer), count) in rows {
            let kind = self
                .context
                .supervisors
                .get(writer)
                .map_or("Not recorded", |descriptor| descriptor.kind.label());
            self.event_count_row(
                output,
                BODY,
                &count.to_string(),
                [event_type, self.context.writer_name(writer), kind],
                count_width,
                widths,
            )?;
        }
        if counts.omitted > 0 {
            self.summary_indented(
                output,
                MUTED,
                TABLE_INDENT,
                &format!(
                    "{} additional entries omitted (event summary limit reached).",
                    counts.omitted
                ),
            )?;
        }
        Ok(())
    }

    fn event_count_row(
        &self,
        output: &mut impl Write,
        shade: &str,
        count: &str,
        cells: [&str; 3],
        count_width: usize,
        widths: [usize; 3],
    ) -> Result<(), Error> {
        let [type_width, writer_width, _] = widths;
        let [types, writers, kinds] =
            std::array::from_fn(|index| cell_lines(&safe_text(cells[index]), widths[index]));
        for index in 0..types.len().max(writers.len()).max(kinds.len()) {
            let count = if index == 0 { count } else { "" };
            let event_type = types.get(index).map_or("", String::as_str);
            let writer = writers.get(index).map_or("", String::as_str);
            let kind = kinds.get(index).map_or("", String::as_str);
            let line = format!("{:TABLE_INDENT$}{count:>count_width$}  {event_type:<type_width$}  {writer:<writer_width$}  {kind}", "");
            self.summary_write(output, shade, line.trim_end())?;
        }
        Ok(())
    }
}
