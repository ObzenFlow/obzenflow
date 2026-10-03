// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Group physical journal counts by their manifest owner, independently of
//! event authorship. Reader descriptions reflect the current framework wiring;
//! application data edges come from the recorded topology.

use super::*;
use crate::render::event_counts::JournalEventCounts;
use obzenflow_core::event::vocabulary::supervisor::{METRICS_NAME, PIPELINE_NAME};

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
                let name = self.summary_owner_name(&manifest.pipeline_writer_id, PIPELINE_NAME);
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
        let pipeline = self.summary_owner_name(&manifest.pipeline_writer_id, PIPELINE_NAME);
        let metrics = manifest
            .metrics_journals
            .as_ref()
            .map(|metrics| self.summary_owner_name(&metrics.writer_id, METRICS_NAME));
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
        // These are presentation groups within one physical journal. Keep the
        // writer ID in the key even when two recorded authors share a name.
        let mut groups = BTreeMap::<_, Vec<CountRow<'_>>>::new();
        for ((descriptor, writer), count) in &counts.event_types {
            let kind = descriptor.event_kind.as_str();
            let event = descriptor.event_type.as_str();
            let version = descriptor.payload_schema_version.to_string();
            let writer = writer.to_string();
            let namespace = event.split_once('.').map_or("", |(namespace, _)| namespace);
            groups
                .entry((
                    self.context.writer_name(&writer).to_owned(),
                    writer,
                    kind,
                    namespace,
                ))
                .or_default()
                .push(CountRow {
                    event,
                    version,
                    count: *count,
                });
        }

        let mut previous_writer = None;
        let mut previous_kind = None;
        for ((author, writer, kind, _), mut rows) in groups {
            // Retain the existing lexical descriptor order, independently of
            // the typed map's enum and numeric version ordering.
            rows.sort_by_cached_key(|row| format!("{}@{}", row.event, row.version));
            writeln!(output)?;
            if previous_writer.as_ref() != Some(&writer) {
                let author_type = self
                    .context
                    .supervisors
                    .get(&writer)
                    .map_or("Not recorded", |descriptor| descriptor.kind.label());
                self.summary_indented(
                    output,
                    BODY,
                    TABLE_INDENT,
                    &format!("Author: {author} ({author_type})"),
                )?;
                previous_writer = Some(writer);
                previous_kind = None;
            }
            if previous_kind != Some(kind) {
                self.summary_indented(output, BODY, TABLE_INDENT, &format!("Kind: {kind}"))?;
                previous_kind = Some(kind);
            }
            let prefix = shared_event_prefix(&rows);
            let heading = if prefix.is_empty() {
                "Event prefix: —".to_owned()
            } else {
                format!("Event prefix: {}", safe_text(prefix))
            };
            for line in event_name_lines(&heading, self.width.saturating_sub(TABLE_INDENT)) {
                self.summary_write(output, BODY, &format!("{:TABLE_INDENT$}{line}", ""))?;
            }
            self.event_group_table(output, prefix, &rows)?;
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

    fn event_group_table(
        &self,
        output: &mut impl Write,
        prefix: &str,
        rows: &[CountRow<'_>],
    ) -> Result<(), Error> {
        let count_width = rows
            .iter()
            .map(|row| row.count.to_string().len())
            .max()
            .unwrap_or(0)
            .max(5);
        let version_width = rows
            .iter()
            .map(|row| row.version.len())
            .max()
            .unwrap_or(0)
            .max(7);
        let event_width = rows
            .iter()
            .map(|row| safe_text(&row.event[prefix.len()..]).chars().count())
            .max()
            .unwrap_or(0)
            .max(5)
            .min(
                self.width
                    .saturating_sub(TABLE_INDENT + count_width + version_width + 4)
                    .max(1),
            );
        self.summary_write(
            output,
            MUTED,
            &format!(
                "{:TABLE_INDENT$}{:>count_width$}  {:<event_width$}  {:>version_width$}",
                "", "Count", "Event", "Version"
            ),
        )?;
        for row in rows {
            let event = safe_text(&row.event[prefix.len()..]);
            let count = row.count.to_string();
            for (index, line) in event_name_lines(&event, event_width).iter().enumerate() {
                let (count, version) = if index == 0 {
                    (count.as_str(), row.version.as_str())
                } else {
                    ("", "")
                };
                self.summary_write(output, BODY, format!(
                    "{:TABLE_INDENT$}{count:>count_width$}  {line:<event_width$}  {version:>version_width$}", ""
                ).trim_end())?;
            }
        }
        Ok(())
    }
}

struct CountRow<'a> {
    event: &'a str,
    version: String,
    count: u64,
}

/// Factor only complete, literal dot-separated segments shared by the rows.
/// Joining this prefix and the displayed suffix recovers the original name.
fn shared_event_prefix<'a>(rows: &[CountRow<'a>]) -> &'a str {
    let first = rows[0].event;
    let mut prefix = first.rfind('.').map_or("", |end| &first[..=end]);
    for row in &rows[1..] {
        while !row.event.starts_with(prefix) {
            prefix = prefix[..prefix.len() - 1]
                .rfind('.')
                .map_or("", |end| &prefix[..=end]);
        }
    }
    prefix
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
