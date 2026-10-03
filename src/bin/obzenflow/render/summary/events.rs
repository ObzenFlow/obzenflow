// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Group physical journal counts by their manifest owner, independently of
//! event authorship. Reader descriptions reflect the current framework wiring;
//! application data edges come from the recorded topology.

use super::*;
use crate::render::event_counts::JournalEventCounts;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::vocabulary::{
    supervisor::{METRICS_NAME, PIPELINE_NAME},
    RUNTIME_PREFIX,
};
use obzenflow_core::event::EventKind;
use obzenflow_core::{EventDescriptor, WriterId};

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
            "Event labels use kind/name@version; supervisor subjects appear as stage, flow or metrics.",
            "Runtime and repeated delivery prefixes are omitted. Full names remain in records/JSONL.",
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
                let name = self.summary_owner_name(&manifest.pipeline_writer_id, PIPELINE_NAME);
                self.summary_line(output, HEADING, &format!("Supervisor: {name}"))?;
                self.summary_indented(
                    output,
                    BODY,
                    2,
                    "Observes: child acknowledgements and results (supervisor handles)",
                )?;
            }
            RunJournalKind::MetricsCoordination | RunJournalKind::MetricsExport => {
                let metrics = manifest
                    .metrics_journals
                    .as_ref()
                    .ok_or("metrics journal is missing its manifest entry")?;
                let name = self.summary_owner_name(&metrics.writer_id, METRICS_NAME);
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
        let metrics = manifest
            .metrics_journals
            .as_ref()
            .map(|metrics| self.summary_owner_name(&metrics.writer_id, METRICS_NAME));
        let (file, purpose) = match journal.kind {
            RunJournalKind::System => (
                &manifest.system_journal_file,
                "pipeline lifecycle and coordination",
            ),
            RunJournalKind::MetricsCoordination | RunJournalKind::MetricsExport => {
                let metrics = manifest
                    .metrics_journals
                    .as_ref()
                    .ok_or("metrics journal is missing its manifest entry")?;
                if journal.kind == RunJournalKind::MetricsCoordination {
                    (
                        &metrics.coordination_journal_file,
                        "metrics lifecycle and finalization",
                    )
                } else {
                    (&metrics.export_journal_file, "export notices and freshness")
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
                    )
                } else {
                    (&stage.error_journal_file, "processing errors")
                }
            }
        };
        let readers = if matches!(
            journal.kind,
            RunJournalKind::System | RunJournalKind::Data | RunJournalKind::Error
        ) {
            metrics.unwrap_or("—")
        } else {
            "—"
        };
        self.summary_indented(output, HEADING, JOURNAL_INDENT, file)?;
        self.summary_indented(output, BODY, TABLE_INDENT, &format!("Purpose: {purpose}"))?;
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
        let mut rows: Vec<_> = counts
            .event_types
            .iter()
            .map(|((descriptor, writer), count)| {
                let event = self.summary_event_label(descriptor, writer);
                let author = self.context.writer_name(&writer.to_string()).to_owned();
                (event, author, writer, count)
            })
            .collect();
        // Keep the owner's events first, followed by forwarded authors, in one
        // table. Writer identity remains a counting key and tie-breaker.
        let forwarded = |writer: &WriterId| {
            counts
                .journal
                .stage
                .as_ref()
                .is_some_and(|stage| *writer != stage.id.into())
        };
        rows.sort_by(|left, right| {
            (forwarded(left.2), &left.0, &left.1, left.2).cmp(&(
                forwarded(right.2),
                &right.0,
                &right.1,
                right.2,
            ))
        });
        let count_width = rows
            .iter()
            .map(|row| row.3.to_string().len())
            .max()
            .unwrap_or(0)
            .max(5);
        let available = self.width.saturating_sub(TABLE_INDENT + count_width + 4);
        let author_width = rows
            .iter()
            .map(|row| safe_text(&row.1).chars().count())
            .max()
            .unwrap_or(0)
            .max(6)
            .min((available / 3).max(1));
        let event_width = rows
            .iter()
            .map(|row| safe_text(&row.0).chars().count())
            .max()
            .unwrap_or(0)
            .max(5)
            .min(available.saturating_sub(author_width).max(1));
        self.summary_write(
            output,
            MUTED,
            &format!(
                "{:TABLE_INDENT$}{:>count_width$}  {:<event_width$}  Author",
                "", "Count", "Event"
            ),
        )?;
        for (event, author, _, count) in rows {
            let events = event_name_lines(&safe_text(&event), event_width);
            let authors = cell_lines(&safe_text(&author), author_width);
            let count = count.to_string();
            for index in 0..events.len().max(authors.len()) {
                let count = if index == 0 { count.as_str() } else { "" };
                let event = events.get(index).map_or("", String::as_str);
                let author = authors.get(index).map_or("", String::as_str);
                self.summary_write(
                    output,
                    BODY,
                    format!(
                        "{:TABLE_INDENT$}{count:>count_width$}  {event:<event_width$}  {author}",
                        ""
                    )
                    .trim_end(),
                )?;
            }
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

    fn summary_event_label(&self, descriptor: &EventDescriptor, writer: &WriterId) -> String {
        let kind = descriptor.event_kind;
        let name = descriptor.event_type.as_str();
        // Application names remain verbatim, even when they resemble a
        // framework namespace. Shortening affects only this human table.
        let short = match kind {
            EventKind::Execution | EventKind::FlowSignal | EventKind::System => {
                if let Some(suffix) = name.strip_prefix(RUNTIME_PREFIX) {
                    suffix.to_owned()
                } else if let Some(label) = self
                    .context
                    .supervisors
                    .get(&writer.to_string())
                    .and_then(|supervisor| {
                        name.strip_prefix(&format!("{}.", supervisor.event_prefix()))
                            .map(|suffix| {
                                let subject = match supervisor.kind {
                                    SupervisorKind::Pipeline => "flow",
                                    SupervisorKind::MetricsAggregator => "metrics",
                                    SupervisorKind::FiniteSource
                                    | SupervisorKind::AsyncFiniteSource
                                    | SupervisorKind::InfiniteSource
                                    | SupervisorKind::AsyncInfiniteSource
                                    | SupervisorKind::Transform
                                    | SupervisorKind::Stateful
                                    | SupervisorKind::Join
                                    | SupervisorKind::Sink => "stage",
                                };
                                format!("{subject}.{suffix}")
                            })
                    })
                {
                    label
                } else {
                    name.to_owned()
                }
            }
            EventKind::Delivery => name
                .strip_prefix(&format!("{}.", kind.as_str()))
                .unwrap_or(name)
                .to_owned(),
            _ => name.to_owned(),
        };
        format!(
            "{}/{short}@{}",
            kind.as_str(),
            descriptor.payload_schema_version
        )
    }
}

/// Prefer name boundaries to splitting words. An individual segment longer
/// than the available width still wraps without discarding any characters.
fn event_name_lines(text: &str, width: usize) -> Vec<String> {
    let mut remaining = text;
    let mut lines = Vec::new();
    while let Some((limit, _)) = remaining.char_indices().nth(width) {
        let end = remaining[..limit]
            .char_indices()
            .rev()
            .find(|(_, ch)| matches!(ch, '.' | '_' | ' '))
            .map_or(limit, |(index, ch)| index + ch.len_utf8());
        lines.push(remaining[..end].to_owned());
        remaining = &remaining[end..];
    }
    lines.push(remaining.to_owned());
    lines
}
