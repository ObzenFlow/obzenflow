// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! `archive_actions` admits a replay, a resume or a rebuild of a run, with or
//! without a warning, or refuses it. It uses the archive evidence of the run and
//! the flow evidence. [`decide`] first checks the flow for a cycle, and then
//! checks for a live writer. For a resume, it then checks the no-work-left row
//! and the sources. For a rebuild, it then checks each named sink stage and
//! admits every archive row. It checks the archive rows of a replay and a resume
//! last.

use super::archive::EffectGuarantee;
use super::ArchiveStatus;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::source::{ResumeRefusal, SourceContinuation};
use std::fmt;
use std::path::{Path, PathBuf};

/// `ReplayVerb` is the archive verb that an operator gives for a recorded run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVerb {
    /// `Replay` reconstructs a bounded recording and drains at its end.
    Replay,
    /// `Resume` resumes a recorded run from the last fact that it committed. The
    /// new run catches up on the recording and then continues with live work.
    Resume,
    /// `Rebuild` reconstructs a recorded run and writes the destinations of the
    /// sink stages that the operator names. It answers every other effect from
    /// the recording.
    Rebuild,
}

impl ReplayVerb {
    /// `word` returns the verb as operators write it, for example `rebuild`.
    pub const fn word(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Resume => "resume",
            Self::Rebuild => "rebuild",
        }
    }

    pub(crate) const fn phrase(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Resume => "resume",
            Self::Rebuild => "rebuild from",
        }
    }
}

/// What one source journal of the archive recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEvidence {
    pub stage_key: String,
    pub finite: bool,
    pub recorded_eof: Option<EofKind>,
}

/// How a failed run's recorded failures bear on resuming it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FailureEvidence {
    /// Not a failed resume, or one that failed for another reason.
    #[default]
    Other,
    /// A failed resume whose every recorded failure departed from its archive.
    DepartedFromArchive,
}

/// `ArchiveEvidence` holds what an archive records about its status, live
/// writers, sources, sinks and guarantees, and about how a failed resume failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEvidence {
    pub status: ArchiveStatus,
    pub live_writer: bool,
    pub sources: Vec<SourceEvidence>,
    /// Consulted only when resuming a `Failed` archive.
    pub failure: FailureEvidence,
    pub sinks: Vec<String>,
    pub guarantees: Vec<(String, Vec<EffectGuarantee>)>,
}

impl ArchiveEvidence {
    fn source(&self, stage_key: &str) -> Option<&SourceEvidence> {
        self.sources
            .iter()
            .find(|source| source.stage_key == stage_key)
    }

    fn completed_naturally(&self) -> bool {
        self.status == ArchiveStatus::Completed
            && self
                .sources
                .iter()
                .all(|source| source.finite && source.recorded_eof == Some(EofKind::Natural))
    }
}

/// The facts the flow builder derives from the flow it built.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowEvidence {
    /// Stage names on a cycle.
    pub cycle_members: Vec<String>,
    /// Each source's declared continuation, by stage key.
    pub sources: Vec<(String, SourceContinuation)>,
    pub guarantees: Vec<(String, Vec<EffectGuarantee>)>,
}

/// The operator's overrides for one command.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchiveOverrides {
    pub allow_incomplete_archive: bool,
}

/// `ArchiveRefusal` tells why [`decide`] refuses an archive verb. Its `Display`
/// text is one line that gives only the cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveRefusal {
    StillRunning {
        verb: ReplayVerb,
        run: PathBuf,
    },
    NoWorkLeft {
        run: PathBuf,
    },
    Failed {
        run: PathBuf,
    },
    Interrupted {
        run: PathBuf,
    },
    NotStarted {
        run: PathBuf,
    },
    /// The flow has a cycle. `members` names the stages on that cycle.
    Cycle {
        verb: ReplayVerb,
        run: PathBuf,
        members: Vec<String>,
    },
    /// Sources whose recorded input a resume cannot continue.
    Sources {
        run: PathBuf,
        refusals: Vec<(String, ResumeRefusal)>,
    },
    /// A rebuild cannot call one or more of the sink stages that it names.
    /// `sinks` lists the sink stages that the run recorded.
    Destinations {
        run: PathBuf,
        refusals: Vec<(String, DestinationRefusal)>,
        sinks: Vec<String>,
    },
}

/// `DestinationRefusal` tells why a rebuild cannot call a named sink stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationRefusal {
    /// The run's manifest does not list the stage as a sink stage.
    Unrecorded,
    /// This build's port method declares a guarantee other than `exactly_once`.
    Declares { guarantee: EffectGuarantee },
    /// The history recorded the stage's calls under a guarantee other than
    /// `exactly_once`.
    Recorded { guarantee: EffectGuarantee },
}

impl ArchiveRefusal {
    pub fn verb(&self) -> ReplayVerb {
        match self {
            Self::StillRunning { verb, .. } | Self::Cycle { verb, .. } => *verb,
            Self::NoWorkLeft { .. } | Self::Sources { .. } => ReplayVerb::Resume,
            Self::Destinations { .. } => ReplayVerb::Rebuild,
            Self::Failed { .. } | Self::Interrupted { .. } | Self::NotStarted { .. } => {
                ReplayVerb::Replay
            }
        }
    }
}

impl fmt::Display for ArchiveRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StillRunning { verb, run } => write!(
                f,
                "ObzenFlow cannot {} {}, because the run still has a live writer.",
                verb.phrase(),
                run.display()
            ),
            Self::NoWorkLeft { run } => write!(
                f,
                "ObzenFlow cannot resume {}, because the run completed naturally and every \
                 source is finite. No work remains.",
                run.display()
            ),
            Self::Failed { run } => write!(
                f,
                "ObzenFlow cannot replay {}, because the run failed.",
                run.display()
            ),
            Self::Interrupted { run } => write!(
                f,
                "ObzenFlow cannot replay {}, because the run is interrupted and has no terminal \
                 record.",
                run.display()
            ),
            Self::NotStarted { run } => write!(
                f,
                "ObzenFlow cannot replay {}, because the run did not start.",
                run.display()
            ),
            Self::Cycle { verb, run, members } => write!(
                f,
                "ObzenFlow cannot {} {}, because the flow has a cycle through {}.",
                verb.phrase(),
                run.display(),
                members.join(", ")
            ),
            Self::Sources { run, refusals } => f.write_str(&sources_refusal(run, refusals)),
            Self::Destinations {
                run,
                refusals,
                sinks,
            } => f.write_str(&destinations_refusal(run, refusals, sinks)),
        }
    }
}

impl std::error::Error for ArchiveRefusal {}

/// `ArchiveWarning` tells why [`decide`] admits a resume with a warning. Its
/// `Display` text is one line that gives only the cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveWarning {
    Failed { run: PathBuf },
}

impl fmt::Display for ArchiveWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed { run } => write!(
                f,
                "ObzenFlow resumes {}, which failed. If the cause of that failure is still \
                 present, this run fails in the same way.",
                run.display()
            ),
        }
    }
}

/// `decide` admits `verb` over `run`, with or without a warning, or refuses it.
/// `stages` names the sink stages of a rebuild, and a replay or a resume names
/// none.
pub fn decide(
    verb: ReplayVerb,
    run: &Path,
    archive: &ArchiveEvidence,
    flow: &FlowEvidence,
    overrides: ArchiveOverrides,
    stages: &[String],
) -> Result<Option<ArchiveWarning>, ArchiveRefusal> {
    let run = run.to_path_buf();
    if !flow.cycle_members.is_empty() {
        return Err(ArchiveRefusal::Cycle {
            verb,
            run,
            members: flow.cycle_members.clone(),
        });
    }
    if archive.live_writer {
        return Err(ArchiveRefusal::StillRunning { verb, run });
    }
    if verb == ReplayVerb::Rebuild {
        let refusals = destination_refusals(archive, flow, stages);
        if !refusals.is_empty() {
            return Err(ArchiveRefusal::Destinations {
                run,
                refusals,
                sinks: archive.sinks.clone(),
            });
        }
        return Ok(None);
    }
    if verb == ReplayVerb::Resume {
        if archive.completed_naturally() {
            return Err(ArchiveRefusal::NoWorkLeft { run });
        }
        let refusals = source_refusals(archive, flow);
        if !refusals.is_empty() {
            return Err(ArchiveRefusal::Sources { run, refusals });
        }
        let warned = archive.status == ArchiveStatus::Failed
            && archive.failure != FailureEvidence::DepartedFromArchive;
        return Ok(warned.then_some(ArchiveWarning::Failed { run }));
    }
    if overrides.allow_incomplete_archive {
        return Ok(None);
    }
    match archive.status {
        ArchiveStatus::Completed | ArchiveStatus::Cancelled => Ok(None),
        ArchiveStatus::Failed => Err(ArchiveRefusal::Failed { run }),
        ArchiveStatus::NotStarted => Err(ArchiveRefusal::NotStarted { run }),
        ArchiveStatus::Unknown => Err(ArchiveRefusal::Interrupted { run }),
    }
}

fn not_exactly_once(
    guarantees: &[(String, Vec<EffectGuarantee>)],
    stage: &str,
) -> Option<EffectGuarantee> {
    let mut declared: Vec<EffectGuarantee> = guarantees
        .iter()
        .filter(|(stage_key, _)| stage_key == stage)
        .flat_map(|(_, guarantees)| guarantees.iter().copied())
        .collect();
    declared.sort();
    declared
        .into_iter()
        .find(|guarantee| *guarantee != EffectGuarantee::ExactlyOnce)
}

fn destination_refusals(
    archive: &ArchiveEvidence,
    flow: &FlowEvidence,
    stages: &[String],
) -> Vec<(String, DestinationRefusal)> {
    let mut named: Vec<&String> = stages.iter().collect();
    named.sort();
    named.dedup();
    named
        .into_iter()
        .filter_map(|stage| {
            let refusal = if !archive.sinks.contains(stage) {
                DestinationRefusal::Unrecorded
            } else if let Some(guarantee) = not_exactly_once(&flow.guarantees, stage) {
                DestinationRefusal::Declares { guarantee }
            } else {
                DestinationRefusal::Recorded {
                    guarantee: not_exactly_once(&archive.guarantees, stage)?,
                }
            };
            Some((stage.clone(), refusal))
        })
        .collect()
}

fn quoted_list(names: &[String]) -> String {
    let quoted: Vec<String> = names.iter().map(|name| format!("'{name}'")).collect();
    match quoted.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn destinations_refusal(
    run: &Path,
    refusals: &[(String, DestinationRefusal)],
    sinks: &[String],
) -> String {
    let recorded = match sinks {
        [] => "The run recorded no sink stage.".to_string(),
        [_] => format!("The run recorded the sink stage {}.", quoted_list(sinks)),
        _ => format!("The run recorded the sink stages {}.", quoted_list(sinks)),
    };
    refusals
        .iter()
        .map(|(stage, refusal)| match refusal {
            DestinationRefusal::Unrecorded => format!(
                "ObzenFlow cannot rebuild '{stage}' from {}, because the run did not record that \
                 sink stage. {recorded}",
                run.display()
            ),
            DestinationRefusal::Declares { guarantee } => format!(
                "ObzenFlow cannot rebuild '{stage}' from {}, because its port method declares \
                 {guarantee}, not exactly_once.",
                run.display()
            ),
            DestinationRefusal::Recorded { guarantee } => format!(
                "ObzenFlow cannot rebuild '{stage}' from {}, because the history of {} recorded \
                 its calls under {guarantee}, not exactly_once.",
                run.display(),
                run.display()
            ),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn source_refusals(archive: &ArchiveEvidence, flow: &FlowEvidence) -> Vec<(String, ResumeRefusal)> {
    let mut refusals: Vec<(String, ResumeRefusal)> = flow
        .sources
        .iter()
        .filter_map(|(stage_key, continuation)| {
            continuation
                .resume_refusal(
                    archive
                        .source(stage_key)
                        .and_then(|source| source.recorded_eof),
                )
                .map(|refusal| (stage_key.clone(), refusal))
        })
        .collect();
    refusals.sort_by(|(left, _), (right, _)| left.cmp(right));
    refusals
}

fn unfinished_sources_refusal(archive: &Path, sources: &[String]) -> String {
    let named = quoted_list(sources);
    let (subject, their) = if sources.len() == 1 {
        ("source", "its")
    } else {
        ("sources", "their")
    };
    format!(
        "ObzenFlow cannot resume {}, because {subject} {named} stopped before {their} input \
         finished, and only the stopped process held that input.",
        archive.display()
    )
}

fn sources_refusal(archive: &Path, refusals: &[(String, ResumeRefusal)]) -> String {
    let unfinished: Vec<String> = refusals
        .iter()
        .filter(|(_, refusal)| *refusal == ResumeRefusal::UnfinishedInput)
        .map(|(source, _)| source.clone())
        .collect();
    unfinished_sources_refusal(archive, &unfinished)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RUN: &str = "/runs/r1";

    fn source(stage_key: &str, finite: bool, recorded_eof: Option<EofKind>) -> SourceEvidence {
        SourceEvidence {
            stage_key: stage_key.to_string(),
            finite,
            recorded_eof,
        }
    }

    fn archive(status: ArchiveStatus, sources: Vec<SourceEvidence>) -> ArchiveEvidence {
        ArchiveEvidence {
            status,
            live_writer: false,
            sources,
            failure: FailureEvidence::Other,
            sinks: vec!["case_register".to_string(), "totals_report".to_string()],
            guarantees: vec![
                (
                    "case_register".to_string(),
                    vec![EffectGuarantee::ExactlyOnce],
                ),
                (
                    "totals_report".to_string(),
                    vec![EffectGuarantee::AtLeastOnce],
                ),
            ],
        }
    }

    fn finished() -> Vec<SourceEvidence> {
        vec![
            source("store_orders", true, Some(EofKind::Natural)),
            source("web_orders", true, Some(EofKind::Natural)),
        ]
    }

    fn safe_flow() -> FlowEvidence {
        FlowEvidence {
            cycle_members: Vec::new(),
            sources: vec![
                ("store_orders".to_string(), SourceContinuation::Refuse),
                ("web_orders".to_string(), SourceContinuation::Refuse),
            ],
            guarantees: vec![
                (
                    "case_register".to_string(),
                    vec![EffectGuarantee::ExactlyOnce],
                ),
                (
                    "totals_report".to_string(),
                    vec![EffectGuarantee::AtLeastOnce],
                ),
            ],
        }
    }

    fn decided(
        verb: ReplayVerb,
        archive: &ArchiveEvidence,
        flow: &FlowEvidence,
        overrides: ArchiveOverrides,
    ) -> Result<Option<ArchiveWarning>, ArchiveRefusal> {
        let stages = match verb {
            ReplayVerb::Rebuild => vec!["case_register".to_string()],
            ReplayVerb::Replay | ReplayVerb::Resume => Vec::new(),
        };
        decide(verb, Path::new(RUN), archive, flow, overrides, &stages)
    }

    fn rebuilt(
        archive: &ArchiveEvidence,
        flow: &FlowEvidence,
        stages: &[&str],
    ) -> Result<Option<ArchiveWarning>, ArchiveRefusal> {
        let stages: Vec<String> = stages.iter().map(|stage| stage.to_string()).collect();
        decide(
            ReplayVerb::Rebuild,
            Path::new(RUN),
            archive,
            flow,
            ArchiveOverrides::default(),
            &stages,
        )
    }

    fn plain() -> ArchiveOverrides {
        ArchiveOverrides::default()
    }

    fn incomplete() -> ArchiveOverrides {
        ArchiveOverrides {
            allow_incomplete_archive: true,
        }
    }

    #[test]
    fn live_writer_every_verb_refused() {
        let mut live = archive(ArchiveStatus::Failed, finished());
        live.live_writer = true;
        for (verb, copy) in [
            (
                ReplayVerb::Resume,
                "ObzenFlow cannot resume /runs/r1, because the run still has a live writer.",
            ),
            (
                ReplayVerb::Replay,
                "ObzenFlow cannot replay /runs/r1, because the run still has a live writer.",
            ),
            (
                ReplayVerb::Rebuild,
                "ObzenFlow cannot rebuild from /runs/r1, because the run still has a live writer.",
            ),
        ] {
            let refusal = decided(verb, &live, &safe_flow(), incomplete()).unwrap_err();
            assert_eq!(refusal.to_string(), copy);
            assert_eq!(refusal.verb(), verb);
        }
    }

    #[test]
    fn a_natural_all_finite_completion_admits_replay_and_refuses_resume() {
        let done = archive(ArchiveStatus::Completed, finished());
        assert!(decided(ReplayVerb::Replay, &done, &safe_flow(), plain()).is_ok());
        assert_eq!(
            decided(ReplayVerb::Resume, &done, &safe_flow(), incomplete())
                .unwrap_err()
                .to_string(),
            "ObzenFlow cannot resume /runs/r1, because the run completed naturally and every \
             source is finite. No work remains."
        );
    }

    #[test]
    fn a_completion_with_other_source_evidence_admits_resume() {
        let continuing = FlowEvidence {
            sources: vec![(
                "web_orders".to_string(),
                SourceContinuation::Reattach {
                    guarantee:
                        crate::event::payloads::execution_payload::ReattachGuarantee::AtMostOnce,
                },
            )],
            ..safe_flow()
        };
        for evidence in [
            vec![source("web_orders", false, Some(EofKind::Natural))],
            vec![source("web_orders", true, Some(EofKind::Truncated))],
            vec![source("web_orders", true, Some(EofKind::Poison))],
        ] {
            let done = archive(ArchiveStatus::Completed, evidence);
            assert!(decided(ReplayVerb::Resume, &done, &continuing, plain()).is_ok());
            assert!(decided(ReplayVerb::Replay, &done, &continuing, plain()).is_ok());
        }
    }

    #[test]
    fn a_cancelled_run_admits_both_verbs() {
        let stopped = archive(ArchiveStatus::Cancelled, finished());
        assert!(decided(ReplayVerb::Resume, &stopped, &safe_flow(), plain()).is_ok());
        assert!(decided(ReplayVerb::Replay, &stopped, &safe_flow(), plain()).is_ok());
    }

    #[test]
    fn an_interrupted_run_resumes_without_the_override_and_replays_only_with_it() {
        let killed = archive(ArchiveStatus::Unknown, finished());
        assert!(decided(ReplayVerb::Resume, &killed, &safe_flow(), plain()).is_ok());
        assert!(decided(ReplayVerb::Resume, &killed, &safe_flow(), incomplete()).is_ok());
        assert_eq!(
            decided(ReplayVerb::Replay, &killed, &safe_flow(), plain())
                .unwrap_err()
                .to_string(),
            "ObzenFlow cannot replay /runs/r1, because the run is interrupted and has no \
             terminal record."
        );
        assert!(decided(ReplayVerb::Replay, &killed, &safe_flow(), incomplete()).is_ok());
    }

    #[test]
    fn failed_run_plain_replay_refused() {
        let failed = archive(ArchiveStatus::Failed, finished());
        let refusal = decided(ReplayVerb::Replay, &failed, &safe_flow(), plain()).unwrap_err();
        assert_eq!(
            refusal.to_string(),
            "ObzenFlow cannot replay /runs/r1, because the run failed."
        );
        assert_eq!(refusal.verb(), ReplayVerb::Replay);
        assert_eq!(
            decided(ReplayVerb::Replay, &failed, &safe_flow(), incomplete()),
            Ok(None)
        );
    }

    #[test]
    fn failed_resume_plain_returns_warning() {
        let failed = archive(ArchiveStatus::Failed, finished());
        for overrides in [plain(), incomplete()] {
            let warning = decided(ReplayVerb::Resume, &failed, &safe_flow(), overrides)
                .unwrap()
                .expect("a failed run resumes with a warning");
            assert_eq!(
                warning,
                ArchiveWarning::Failed {
                    run: PathBuf::from(RUN)
                }
            );
            assert_eq!(
                warning.to_string(),
                "ObzenFlow resumes /runs/r1, which failed. If the cause of that failure is \
                 still present, this run fails in the same way."
            );
        }
    }

    #[test]
    fn a_departed_resume_resumes_plainly_and_replays_only_with_the_override() {
        let departed = ArchiveEvidence {
            failure: FailureEvidence::DepartedFromArchive,
            ..archive(ArchiveStatus::Failed, finished())
        };
        assert_eq!(
            decided(ReplayVerb::Resume, &departed, &safe_flow(), plain()),
            Ok(None),
            "a departed resume carries no warning"
        );
        assert!(matches!(
            decided(ReplayVerb::Replay, &departed, &safe_flow(), plain()),
            Err(ArchiveRefusal::Failed { .. })
        ));
        assert!(decided(ReplayVerb::Replay, &departed, &safe_flow(), incomplete()).is_ok());
        let cyclic = FlowEvidence {
            cycle_members: vec!["triage".to_string()],
            ..safe_flow()
        };
        assert!(
            matches!(
                decided(ReplayVerb::Resume, &departed, &cyclic, plain()),
                Err(ArchiveRefusal::Cycle { .. })
            ),
            "the flow's rows still apply"
        );
    }

    #[test]
    fn cyclic_flow_replay_refused() {
        let stopped = archive(ArchiveStatus::Cancelled, finished());
        let cyclic = FlowEvidence {
            cycle_members: vec!["review".to_string(), "triage".to_string()],
            ..safe_flow()
        };
        for verb in [ReplayVerb::Replay, ReplayVerb::Resume] {
            assert_eq!(
                decided(verb, &stopped, &cyclic, plain()),
                Err(ArchiveRefusal::Cycle {
                    verb,
                    run: PathBuf::from(RUN),
                    members: cyclic.cycle_members.clone(),
                })
            );
        }
    }

    #[test]
    fn unfinished_refusing_sources_resume_refused() {
        let early = archive(ArchiveStatus::Cancelled, unfinished());
        assert!(decided(ReplayVerb::Replay, &early, &safe_flow(), plain()).is_ok());
        assert_eq!(
            decided(ReplayVerb::Resume, &early, &safe_flow(), plain())
                .unwrap_err()
                .to_string(),
            "ObzenFlow cannot resume /runs/r1, because sources 'store_orders' and 'web_orders' \
             stopped before their input finished, and only the stopped process held that input."
        );
    }

    #[test]
    fn unfinished_source_refusal_names_every_source() {
        let archive = Path::new("/runs/interrupted");
        assert_eq!(
            unfinished_sources_refusal(archive, &["web_orders".to_string()]),
            "ObzenFlow cannot resume /runs/interrupted, because source 'web_orders' stopped \
             before its input finished, and only the stopped process held that input."
        );
        assert!(unfinished_sources_refusal(
            archive,
            &["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .starts_with(
            "ObzenFlow cannot resume /runs/interrupted, because sources 'a', 'b' and 'c' \
             stopped before their input finished"
        ));
    }

    #[test]
    fn cyclic_flow_any_archive_returns_cycle() {
        let cyclic = FlowEvidence {
            cycle_members: vec!["review".to_string(), "triage".to_string()],
            ..safe_flow()
        };
        let mut live = archive(ArchiveStatus::Cancelled, finished());
        live.live_writer = true;
        for evidence in [
            archive(ArchiveStatus::Completed, finished()),
            archive(ArchiveStatus::Cancelled, finished()),
            archive(ArchiveStatus::NotStarted, finished()),
            archive(ArchiveStatus::Unknown, finished()),
            archive(ArchiveStatus::Failed, finished()),
            live,
        ] {
            for verb in [ReplayVerb::Replay, ReplayVerb::Resume, ReplayVerb::Rebuild] {
                for overrides in [plain(), incomplete()] {
                    assert_eq!(
                        decided(verb, &evidence, &cyclic, overrides),
                        Err(ArchiveRefusal::Cycle {
                            verb,
                            run: PathBuf::from(RUN),
                            members: cyclic.cycle_members.clone(),
                        }),
                        "{evidence:?}"
                    );
                }
            }
        }
    }

    fn unfinished() -> Vec<SourceEvidence> {
        vec![
            source("store_orders", true, Some(EofKind::Truncated)),
            source("web_orders", true, None),
        ]
    }

    #[test]
    fn not_started_replay_plain_names_cause() {
        let idle = archive(ArchiveStatus::NotStarted, Vec::new());
        let refusal = decided(ReplayVerb::Replay, &idle, &safe_flow(), plain()).unwrap_err();
        assert_eq!(
            refusal,
            ArchiveRefusal::NotStarted {
                run: PathBuf::from(RUN)
            }
        );
        assert_eq!(
            refusal.to_string(),
            "ObzenFlow cannot replay /runs/r1, because the run did not start."
        );
        assert_eq!(
            decided(ReplayVerb::Replay, &idle, &safe_flow(), incomplete()),
            Ok(None)
        );
    }

    #[test]
    fn not_started_resume_plain_admitted() {
        let idle = archive(
            ArchiveStatus::NotStarted,
            vec![source("store_orders", true, None)],
        );
        let rereadable = FlowEvidence {
            sources: vec![("store_orders".to_string(), SourceContinuation::RecordedRead)],
            ..safe_flow()
        };
        assert_eq!(
            decided(ReplayVerb::Resume, &idle, &rereadable, plain()),
            Ok(None)
        );
        assert!(matches!(
            decided(ReplayVerb::Resume, &idle, &safe_flow(), plain()),
            Err(ArchiveRefusal::Sources { .. })
        ));
    }

    #[test]
    fn failed_resume_unfinished_input_returns_sources() {
        let failed = archive(ArchiveStatus::Failed, unfinished());
        for overrides in [plain(), incomplete()] {
            assert!(matches!(
                decided(ReplayVerb::Resume, &failed, &safe_flow(), overrides),
                Err(ArchiveRefusal::Sources { .. })
            ));
        }
    }

    #[test]
    fn rebuild_unrecorded_stage_refused() {
        let done = archive(ArchiveStatus::Completed, finished());
        let refusal = rebuilt(&done, &safe_flow(), &["alert_log"]).unwrap_err();
        assert_eq!(
            refusal,
            ArchiveRefusal::Destinations {
                run: PathBuf::from(RUN),
                refusals: vec![("alert_log".to_string(), DestinationRefusal::Unrecorded)],
                sinks: done.sinks.clone(),
            }
        );
        assert_eq!(refusal.verb(), ReplayVerb::Rebuild);
        assert_eq!(
            refusal.to_string(),
            "ObzenFlow cannot rebuild 'alert_log' from /runs/r1, because the run did not record \
             that sink stage. The run recorded the sink stages 'case_register' and \
             'totals_report'."
        );
    }

    #[test]
    fn rebuild_declared_at_most_once_refused() {
        let done = archive(ArchiveStatus::Completed, finished());
        let flow = FlowEvidence {
            guarantees: vec![(
                "case_register".to_string(),
                vec![EffectGuarantee::AtMostOnce],
            )],
            ..safe_flow()
        };
        let refusal = rebuilt(&done, &flow, &["case_register"]).unwrap_err();
        assert_eq!(
            refusal.to_string(),
            "ObzenFlow cannot rebuild 'case_register' from /runs/r1, because its port method \
             declares at_most_once, not exactly_once."
        );
    }

    #[test]
    fn rebuild_declared_at_least_once_refused() {
        let done = archive(ArchiveStatus::Completed, finished());
        let refusal = rebuilt(&done, &safe_flow(), &["totals_report"]).unwrap_err();
        assert!(matches!(
            refusal,
            ArchiveRefusal::Destinations { ref refusals, .. }
                if refusals == &[(
                    "totals_report".to_string(),
                    DestinationRefusal::Declares {
                        guarantee: EffectGuarantee::AtLeastOnce,
                    },
                )]
        ));
        assert_eq!(
            refusal.to_string(),
            "ObzenFlow cannot rebuild 'totals_report' from /runs/r1, because its port method \
             declares at_least_once, not exactly_once."
        );
    }

    #[test]
    fn rebuild_recorded_at_most_once_refused() {
        let mut done = archive(ArchiveStatus::Completed, finished());
        done.guarantees = vec![(
            "case_register".to_string(),
            vec![EffectGuarantee::AtMostOnce],
        )];
        let refusal = rebuilt(&done, &safe_flow(), &["case_register"]).unwrap_err();
        assert_eq!(
            refusal.to_string(),
            "ObzenFlow cannot rebuild 'case_register' from /runs/r1, because the history of \
             /runs/r1 recorded its calls under at_most_once, not exactly_once."
        );
    }

    #[test]
    fn rebuild_exactly_once_destination_admitted() {
        let done = archive(ArchiveStatus::Completed, finished());
        assert_eq!(
            rebuilt(&done, &safe_flow(), &["case_register", "case_register"]),
            Ok(None)
        );
    }

    #[test]
    fn rebuild_every_archive_status_admitted() {
        let departed = ArchiveEvidence {
            failure: FailureEvidence::DepartedFromArchive,
            ..archive(ArchiveStatus::Failed, finished())
        };
        for evidence in [
            archive(ArchiveStatus::Completed, finished()),
            archive(ArchiveStatus::Cancelled, unfinished()),
            archive(ArchiveStatus::Unknown, unfinished()),
            archive(ArchiveStatus::NotStarted, Vec::new()),
            archive(ArchiveStatus::Failed, finished()),
            departed,
        ] {
            for overrides in [plain(), incomplete()] {
                assert_eq!(
                    decided(ReplayVerb::Rebuild, &evidence, &safe_flow(), overrides),
                    Ok(None),
                    "{evidence:?}"
                );
            }
        }
    }
}
