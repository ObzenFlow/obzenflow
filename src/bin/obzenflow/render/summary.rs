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
        if self.json {
            writeln!(
                diagnostics,
                "{}",
                serde_json::json!({"event":"run_observation_summary", "run":run, "reason":end.label(), "records":self.records, "journals":self.journals, "event_types":self.event_types, "other_event_types":self.other_event_types, "progress":progress})
            )?;
        } else {
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
            if !self.quiet {
                if self.shown_records < self.records {
                    self.summary_line(
                        output,
                        MUTED,
                        &format!(
                            "{} displayed; {} runtime entries hidden (--verbose).",
                            self.shown_records,
                            self.records - self.shown_records,
                        ),
                    )?;
                }
                self.manifest_summary(output, manifest)?;
                self.journal_summary(output, manifest)?;
                writeln!(output)?;
                self.summary_line(output, HEADING, "By event type")?;
                for (event_type, count) in &self.event_types {
                    self.summary_line(output, BODY, &format!("  {count:>7}  {event_type}"))?;
                }
                if self.other_event_types > 0 {
                    self.summary_line(
                        output,
                        MUTED,
                        &format!(
                            "  {:>7}  other event types (summary limit reached)",
                            self.other_event_types
                        ),
                    )?;
                }
                self.summary_line(output, MUTED, "Event-type counts cover displayed entries, including replayed evidence; not physical calls.")?;
            }
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
        if self.detail {
            // No shortening: --detail exposes the recorded fields, including
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
        if !self.detail {
            self.summary_line(
                output,
                MUTED,
                "Use --detail for the complete manifest and exact journal filenames.",
            )?;
        }
        Ok(())
    }

    fn observed(&self, journal: &str) -> u64 {
        self.journals.get(journal).copied().unwrap_or(0)
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
