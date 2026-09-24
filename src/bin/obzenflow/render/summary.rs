// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Manifest metadata and observed journal counts. Completion comes only from
//! the shared reader's journal evidence, never the manifest or a live lookup.

use super::*;

const HEADING: &str = "1;38;5;255";
const BODY: &str = "38;5;252";
const MUTED: &str = "38;5;245";

impl Renderer {
    pub(crate) fn finish(
        &mut self,
        output: &mut impl Write,
        diagnostics: &mut impl Write,
        run: &RunIdentity,
        manifest: &RunManifest,
        end: ObservationEnd,
        progress: &RunReadProgress,
    ) -> Result<(), Error> {
        self.flush_pending(output)?;
        if self.jsonl {
            writeln!(
                diagnostics,
                "{}",
                serde_json::json!({"event":"run_observation_summary", "run":run, "reason":end.label(), "records":self.shown_records, "journals":self.shown_journals, "event_types":self.event_types, "other_event_types":self.other_event_types, "progress":progress})
            )?;
        } else {
            if !self.compact {
                if self.shown_records < self.records {
                    self.summary_line(
                        output,
                        MUTED,
                        &format!(
                            "{} displayed; {} runtime entries hidden (--include-runtime).",
                            self.shown_records,
                            self.records - self.shown_records,
                        ),
                    )?;
                }
                self.manifest_summary(output, manifest)?;
                self.journal_summary(output, manifest)?;
                self.event_summary(output, manifest)?;
                writeln!(output)?;
            }
            self.summary_line(
                output,
                HEADING,
                &format!(
                    "CLI observed {} journal {} across {} journals.",
                    self.records,
                    if self.records == 1 {
                        "entry"
                    } else {
                        "entries"
                    },
                    1 + manifest.stages.len() * 2,
                ),
            )?;
            let outcome = match progress.outcome.as_ref().map(|recorded| &recorded.outcome) {
                Some(RunOutcome::Completed) => "Run completed.".into(),
                Some(RunOutcome::NotStarted) => "Run did not start.".into(),
                Some(RunOutcome::Failed { reason }) => format!("Run failed: {reason}."),
                Some(RunOutcome::Cancelled { reason }) => format!("Run cancelled: {reason}."),
                None => "Run outcome not recorded in the observed entries.".into(),
            };
            let coverage = match end {
                ObservationEnd::Settled => "CLI reached the recorded end of execution.",
                ObservationEnd::Snapshot => "CLI reached the end of its snapshot.",
                ObservationEnd::Detached => "CLI stopped observing; execution is independent.",
            };
            self.summary_line(output, BODY, &format!("{outcome} {coverage}"))?;
        }
        output.flush()?;
        diagnostics.flush()?;
        Ok(())
    }

    fn manifest_summary(
        &self,
        output: &mut impl Write,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        writeln!(output)?;
        self.summary_line(
            output,
            HEADING,
            &format!("{:<13}{RUN_MANIFEST_FILENAME}", "MANIFEST"),
        )?;
        if self.full {
            // No shortening: --full exposes the recorded fields, including
            // exact filenames. JSON escapes keep terminal controls inert.
            let (json, _) = pretty(&serde_json::to_value(manifest)?, usize::MAX);
            for line in json.lines() {
                self.summary_write(output, BODY, line)?;
            }
        } else {
            for (label, value) in [
                ("Flow", manifest.flow_name.clone()),
                ("Run", manifest.flow_id.clone()),
                (
                    "Created",
                    manifest
                        .created_at
                        .format("%Y-%m-%d %H:%M:%S UTC")
                        .to_string(),
                ),
                ("ObzenFlow", manifest.obzenflow_version.clone()),
                ("Schema", manifest.journal_schema_version.clone()),
            ] {
                self.summary_line(output, BODY, &format!("  {label:<10} {value}"))?;
            }
        }
        Ok(())
    }

    fn journal_summary(
        &self,
        output: &mut impl Write,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        let stages = manifest.stages.len();
        writeln!(output)?;
        self.summary_line(output, HEADING, "JOURNALS")?;
        let data_width = self.records.to_string().len().max(12);
        let error_width = self.records.to_string().len().max(13);
        let name_width = self
            .context
            .stages
            .iter()
            .map(|stage| safe_text(&stage.key).chars().count())
            .max()
            .unwrap_or(5)
            .max(20)
            .min(self.width.saturating_sub(data_width + error_width + 6));
        self.summary_write(
            output,
            MUTED,
            &format!(
                "  {:<name_width$}  {:>data_width$}",
                "System", "Data journal"
            ),
        )?;
        let system_file = fit(&safe_text(&manifest.system_journal_file), name_width);
        let count = self.observed("system/system");
        self.summary_write(
            output,
            BODY,
            &format!("  {system_file:<name_width$}  {count:>data_width$}"),
        )?;

        if stages > 0 {
            writeln!(output)?;
            self.summary_write(
                output,
                MUTED,
                &format!(
                    "  {:<name_width$}  {:>data_width$}  {:>error_width$}",
                    "Stage", "Data journal", "Error journal"
                ),
            )?;
            // Match the stage order in the clock legend; both are derived from
            // admitted manifest metadata, including journals with no entries.
            for stage in &self.context.stages {
                let data = self.observed(&format!("{}/data", stage.key));
                let error = self.observed(&format!("{}/error", stage.key));
                let key = fit(&safe_text(&stage.key), name_width);
                self.summary_write(
                    output,
                    BODY,
                    &format!("  {key:<name_width$}  {data:>data_width$}  {error:>error_width$}"),
                )?;
            }
        }
        writeln!(output)?;
        self.summary_line(
            output,
            MUTED,
            "Journal counts include every observed entry, including hidden runtime entries.",
        )?;
        if stages > 0 {
            self.summary_line(
                output,
                MUTED,
                "Each stage has separate data and error journal files.",
            )?;
        }
        self.summary_line(
            output,
            MUTED,
            "The manifest identifies those files and describes the run.",
        )?;
        if !self.full {
            self.summary_line(output, MUTED, "Use --full for the complete manifest.")?;
        }
        Ok(())
    }

    fn observed(&self, journal: &str) -> u64 {
        self.journals.get(journal).copied().unwrap_or(0)
    }

    fn event_summary(&self, output: &mut impl Write, manifest: &RunManifest) -> Result<(), Error> {
        let mut journals: Vec<_> = self.event_counts.journals().collect();
        // Follow the inventory order: system first, then each stage's data and
        // error journals. Only journals with displayed entries need a table.
        journals.sort_by_key(|counts| {
            let stage_order = counts.journal.stage.as_ref().map_or(0, |owner| {
                self.context
                    .stages
                    .iter()
                    .position(|stage| stage.key == owner.key)
                    .unwrap_or(self.context.stages.len())
                    + 1
            });
            (
                stage_order,
                counts.journal.kind == RunJournalKind::Error,
                counts.journal.id,
            )
        });
        if journals.is_empty() {
            return Ok(());
        }
        writeln!(output)?;
        self.summary_line(
            output,
            MUTED,
            "Event counts cover displayed entries in each journal, including replayed evidence.",
        )?;
        for counts in journals {
            // Size each journal's columns to its own contents, leaving only
            // the two-space gutters needed to distinguish adjacent columns.
            let count_width = counts
                .event_types
                .values()
                .map(|count| count.to_string().len())
                .max()
                .unwrap_or(0)
                .max(5);
            let minimums = [10, 6, 11]; // Event type, Author, Author type.
            let mut widths = minimums;
            for (event_type, writer) in counts.event_types.keys() {
                let kind = self
                    .context
                    .supervisors
                    .get(writer)
                    .map_or("Not recorded", |descriptor| descriptor.kind.label());
                for (width, text) in widths.iter_mut().zip([
                    event_type.as_str(),
                    self.context.writer_name(writer),
                    kind,
                ]) {
                    *width = (*width).max(safe_text(text).chars().count());
                }
            }
            let available = self.width.saturating_sub(count_width + 8);
            while widths.iter().sum::<usize>() > available {
                let column = (0..3)
                    .max_by_key(|&index| widths[index] - minimums[index])
                    .unwrap();
                if widths[column] == minimums[column] {
                    break;
                }
                widths[column] -= 1;
            }
            let [type_width, writer_width, kind_width] = widths;
            writeln!(output)?;
            self.event_journal_heading(output, &counts.journal, manifest)?;
            self.summary_write(
                output,
                MUTED,
                &format!(
                    "  {:>count_width$}  {:<type_width$}  {:<writer_width$}  Author type",
                    "Count", "Event type", "Author"
                ),
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
                let types = cell_lines(&safe_text(event_type), type_width);
                let writers =
                    cell_lines(&safe_text(self.context.writer_name(writer)), writer_width);
                let kinds = cell_lines(kind, kind_width);
                for index in 0..types.len().max(writers.len()).max(kinds.len()) {
                    let count = if index == 0 {
                        count.to_string()
                    } else {
                        String::new()
                    };
                    let event_type = types.get(index).map_or("", String::as_str);
                    let writer = writers.get(index).map_or("", String::as_str);
                    let kind = kinds.get(index).map_or("", String::as_str);
                    let line = format!("  {count:>count_width$}  {event_type:<type_width$}  {writer:<writer_width$}  {kind}");
                    self.summary_write(output, BODY, line.trim_end())?;
                }
            }
            if counts.omitted > 0 {
                self.summary_line(
                    output,
                    MUTED,
                    &format!(
                        "  {} additional entries omitted (event summary limit reached).",
                        counts.omitted,
                    ),
                )?;
            }
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

    fn event_journal_heading(
        &self,
        output: &mut impl Write,
        journal: &RunJournal,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        if journal.kind == RunJournalKind::System {
            return self.summary_line(output, HEADING, &manifest.system_journal_file);
        }
        let owner = journal
            .stage
            .as_ref()
            .ok_or("stage journal is missing its recorded stage identity")?;
        let stage = manifest
            .stages
            .get(&owner.key)
            .ok_or("stage journal is missing its manifest entry")?;
        if journal.kind == RunJournalKind::Error {
            self.summary_line(output, HEADING, &stage.error_journal_file)?;
            return self.summary_line(output, BODY, &format!("  Stage: {}", owner.key));
        }

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
        self.summary_line(output, HEADING, &format!("Stage: {}", owner.key))?;
        for (label, value) in [
            ("Subscribes to", stage_list(&inputs)),
            ("Writes to", stage.data_journal_file.clone()),
            ("Subscribers", stage_list(&subscribers)),
        ] {
            self.summary_line(output, BODY, &format!("  {label}: {value}"))?;
        }
        Ok(())
    }

    fn summary_line(&self, output: &mut impl Write, shade: &str, text: &str) -> Result<(), Error> {
        for line in wrap_fields(&[safe_text(text)], self.width) {
            self.summary_write(output, shade, &line)?;
        }
        Ok(())
    }

    fn summary_write(&self, output: &mut impl Write, shade: &str, text: &str) -> Result<(), Error> {
        if self.color {
            writeln!(output, "\x1b[{shade}m{text}\x1b[0m")?;
        } else {
            writeln!(output, "{text}")?;
        }
        Ok(())
    }
}

fn stage_list(stages: &[&str]) -> String {
    if stages.is_empty() {
        "—".into()
    } else {
        stages.join(", ")
    }
}

fn fit(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        text.into()
    } else {
        text.chars()
            .take(width.saturating_sub(1))
            .chain(['…'])
            .collect()
    }
}

fn cell_lines(text: &str, width: usize) -> Vec<String> {
    text.chars()
        .collect::<Vec<_>>()
        .chunks(width)
        .map(|part| part.iter().collect())
        .collect()
}
