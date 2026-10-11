// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Where the run stands in its history and how each source continued
//! (FLOWIP-134i B6, B7, B12, B13, B15, B16). Lineage is the library's read of
//! manifests and claims; continuation projects the run's own source facts.

use super::*;
use continuation::{RecordingEnd, SourceContinuation};
use obzenflow_core::event::payloads::execution_payload::SourceContinuationRefusal;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use std::path::PathBuf;

impl Renderer {
    pub(super) fn lineage_summary(
        &self,
        output: &mut impl Write,
        lineage: &Result<RunLineage, JournalReadError>,
    ) -> Result<(), Error> {
        let lines = match lineage {
            Ok(lineage) => lineage_lines(lineage, self.continuations.recording_end()),
            Err(error) => vec![("Unreadable", error.to_string())],
        };
        if lines.is_empty() {
            return Ok(());
        }
        writeln!(output)?;
        self.summary_line(output, HEADING, "LINEAGE")?;
        for (label, value) in lines {
            self.labeled(output, label, &value)?;
        }
        Ok(())
    }

    pub(super) fn continuation_summary(
        &self,
        output: &mut impl Write,
        manifest: &RunManifest,
    ) -> Result<(), Error> {
        if manifest.resume.is_none() {
            return Ok(());
        }
        let sources: Vec<&str> = self
            .context
            .stages
            .iter()
            .map(|stage| stage.key.as_str())
            .filter(|key| {
                manifest
                    .stages
                    .get(*key)
                    .is_some_and(|stage| stage.stage_type.is_source())
            })
            .collect();
        if sources.is_empty() {
            return Ok(());
        }
        let width = sources
            .iter()
            .map(|key| safe_text(key).chars().count())
            .max()
            .unwrap_or(0)
            .min(self.width / 2);
        writeln!(output)?;
        self.summary_line(output, HEADING, "CONTINUATION")?;
        for key in sources {
            let name = fit(&safe_text(key), width);
            let status = continuation_text(self.continuations.source(key));
            self.summary_line(output, BODY, &format!("  {name:<width$}   {status}"))?;
        }
        Ok(())
    }

    /// `label` in the summary's label column; wrapped lines hang under the value.
    fn labeled(&self, output: &mut impl Write, label: &str, value: &str) -> Result<(), Error> {
        let lead = format!("  {label:<10} ");
        let indent = lead.chars().count();
        let lines = wrap_fields(&[safe_text(value)], self.width.saturating_sub(indent));
        for (index, line) in lines.into_iter().enumerate() {
            let lead = if index == 0 {
                lead.clone()
            } else {
                " ".repeat(indent)
            };
            self.summary_write(output, BODY, &format!("{lead}{line}"))?;
        }
        Ok(())
    }
}

fn lineage_lines(
    lineage: &RunLineage,
    recording_end: Option<&RecordingEnd>,
) -> Vec<(&'static str, String)> {
    let mut lines = Vec::new();
    if let Some(resumed) = &lineage.resumed_from {
        lines.push((
            "Resumed",
            format!(
                "from {}, generation {}",
                resumed.parent.display(),
                resumed.generation
            ),
        ));
    }
    match &lineage.standing {
        Standing::Replay { replayed } => {
            let ended = recording_end.map_or(String::new(), |end| {
                let input = end
                    .input_position
                    .map_or(String::new(), |input| format!(", input {input}"));
                format!("; recording ended at '{}'{input}", end.stage_key)
            });
            lines.push(("Replayed", format!("{}{ended}", replayed.display())));
        }
        Standing::Orphan { resume } => lines.push((
            "Orphan",
            format!("not part of a history; resume {} instead", resume.display()),
        )),
        Standing::Head | Standing::Continued { .. } => {}
    }
    match &lineage.rebuild {
        Some(Rebuild::Complete) => {
            lines.push(("Rebuild", "complete; resume needs no earlier run".into()))
        }
        Some(Rebuild::Incomplete {
            generation,
            requires,
        }) if requires.is_empty() => {
            lines.push(("Rebuild", format!("incomplete (generation {generation})")))
        }
        Some(Rebuild::Incomplete {
            generation,
            requires,
        }) => lines.push((
            "Rebuild",
            format!(
                "incomplete (generation {generation}); resume requires {}",
                joined(requires)
            ),
        )),
        None => {}
    }
    for holder in &lineage.evidence {
        let held = match holder.generation {
            Some(generation) => format!("{} holds generation {generation}", holder.run.display()),
            None => holder.run.display().to_string(),
        };
        let missing = if holder.missing { ", missing" } else { "" };
        lines.push(("Evidence", format!("{held}{missing}")));
    }
    if let Standing::Continued { head } = &lineage.standing {
        lines.push(("Continued", format!("by {}", head.display())));
    }
    lines
}

fn joined(runs: &[PathBuf]) -> String {
    runs.iter()
        .map(|run| run.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn continuation_text(continued: Option<&SourceContinuation>) -> String {
    match continued {
        Some(SourceContinuation::JournalOnly(kind)) => {
            format!(
                "journal-only ({} EOF in the recorded run)",
                eof_label(*kind)
            )
        }
        Some(SourceContinuation::Positioned { generation }) => {
            format!("continued in generation {generation} after its committed input")
        }
        Some(SourceContinuation::Started { generation }) => {
            format!("started in generation {generation} (the interrupted run failed to open it)")
        }
        Some(SourceContinuation::Reattached { generation }) => {
            format!("reattached live in generation {generation}")
        }
        Some(SourceContinuation::Refused(refusal)) => {
            format!("refused: {}", refusal_text(refusal))
        }
        Some(SourceContinuation::SeekFailed) => "failed to seek past its committed input".into(),
        None => "catch-up incomplete".into(),
    }
}

fn eof_label(kind: EofKind) -> &'static str {
    match kind {
        EofKind::Natural => "natural",
        EofKind::Poison => "poison",
        EofKind::Truncated => "truncated",
    }
}

fn refusal_text(refusal: &SourceContinuationRefusal) -> String {
    match refusal {
        SourceContinuationRefusal::InputChanged { part } => {
            format!("its input changed since it was pinned ({part})")
        }
        SourceContinuationRefusal::MissingEvidence => {
            "its archive lacks the recorded position of an admitted record".into()
        }
        SourceContinuationRefusal::PositionUnreadable => {
            "its recorded position was written by another version of its connector".into()
        }
        SourceContinuationRefusal::Unpositioned => {
            "its connector does not position its input".into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(lineage: &RunLineage, recording_end: Option<&RecordingEnd>) -> Vec<String> {
        lineage_lines(lineage, recording_end)
            .into_iter()
            .map(|(label, value)| format!("{label:<10} {value}"))
            .collect()
    }

    #[test]
    fn lineage_names_what_a_torn_head_requires_and_who_still_holds_evidence() {
        let torn = RunLineage {
            resumed_from: Some(ResumedFrom {
                parent: "/runs/r1".into(),
                generation: 2,
            }),
            standing: Standing::Head,
            rebuild: Some(Rebuild::Incomplete {
                generation: 2,
                requires: vec!["/runs/r1".into()],
            }),
            evidence: vec![EvidenceHolder {
                run: "/runs/r0".into(),
                generation: Some(0),
                missing: true,
            }],
        };
        assert_eq!(
            rendered(&torn, None),
            [
                "Resumed    from /runs/r1, generation 2",
                "Rebuild    incomplete (generation 2); resume requires /runs/r1",
                "Evidence   /runs/r0 holds generation 0, missing",
            ]
        );
    }

    #[test]
    fn lineage_names_the_run_to_resume_for_orphans_and_replays() {
        let orphan = RunLineage {
            resumed_from: Some(ResumedFrom {
                parent: "/runs/r0".into(),
                generation: 1,
            }),
            standing: Standing::Orphan {
                resume: "/runs/r1".into(),
            },
            ..RunLineage::default()
        };
        assert_eq!(
            rendered(&orphan, None),
            [
                "Resumed    from /runs/r0, generation 1",
                "Orphan     not part of a history; resume /runs/r1 instead",
            ]
        );
        let replay = RunLineage {
            standing: Standing::Replay {
                replayed: "/runs/r1".into(),
            },
            ..RunLineage::default()
        };
        let end = RecordingEnd {
            stage_key: "authorize_payment".into(),
            input_position: Some(9),
        };
        assert_eq!(
            rendered(&replay, Some(&end)),
            ["Replayed   /runs/r1; recording ended at 'authorize_payment', input 9"]
        );
        assert!(rendered(&RunLineage::default(), None).is_empty());
    }

    #[test]
    fn continuation_reads_as_the_proposal_names_it() {
        assert_eq!(
            continuation_text(Some(&SourceContinuation::JournalOnly(EofKind::Natural))),
            "journal-only (natural EOF in the recorded run)"
        );
        assert_eq!(
            continuation_text(Some(&SourceContinuation::Started { generation: 1 })),
            "started in generation 1 (the interrupted run failed to open it)"
        );
        assert_eq!(continuation_text(None), "catch-up incomplete");
    }
}
