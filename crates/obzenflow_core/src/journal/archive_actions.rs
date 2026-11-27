// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! `archive_actions` admits or refuses a replay or a resume of a run. It uses
//! the archive evidence of the run and the flow evidence. [`decide`] first
//! checks the flow for a cycle, and then checks for a live writer. For a resume,
//! it then checks the no-work-left row and the sources. It checks the archive
//! rows last.

use super::ArchiveStatus;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::source::{ResumeRefusal, SourceContinuation};
use std::fmt;
use std::path::{Path, PathBuf};

/// The operator's archive verb, a bounded replay that drains or a resume that
/// catches up and continues live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVerb {
    /// `Replay` reconstructs a bounded recording and drains at its end.
    Replay,
    /// `Resume` resumes a recorded run from the last fact that it committed. The
    /// new run catches up on the recording and then continues with live work.
    Resume,
}

impl ReplayVerb {
    /// The verb as operators write it, `replay` or `resume`.
    pub const fn word(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Resume => "resume",
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

/// What an archive records about its status, live writers, sources and how a
/// failed resume failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEvidence {
    pub status: ArchiveStatus,
    pub live_writer: bool,
    pub sources: Vec<SourceEvidence>,
    /// Consulted only when resuming a `Failed` archive.
    pub failure: FailureEvidence,
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
        verb: ReplayVerb,
        run: PathBuf,
    },
    Interrupted {
        run: PathBuf,
    },
    /// A replay or resume of a flow with a cycle; `members` names the stages
    /// on it.
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
}

impl ArchiveRefusal {
    pub fn verb(&self) -> ReplayVerb {
        match self {
            Self::StillRunning { verb, .. }
            | Self::Failed { verb, .. }
            | Self::Cycle { verb, .. } => *verb,
            Self::NoWorkLeft { .. } | Self::Sources { .. } => ReplayVerb::Resume,
            Self::Interrupted { .. } => ReplayVerb::Replay,
        }
    }
}

impl fmt::Display for ArchiveRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StillRunning { verb, run } => write!(
                f,
                "cannot {} {}: it is still running.",
                verb.word(),
                run.display()
            ),
            Self::NoWorkLeft { run } => write!(
                f,
                "cannot resume {}: it completed naturally and every source is finite, so there \
                 is no work left to continue.",
                run.display()
            ),
            Self::Failed { verb, run } => {
                write!(f, "cannot {} {}: it failed.", verb.word(), run.display())
            }
            Self::Interrupted { run } => write!(
                f,
                "cannot replay {}: it has no terminal record, so it was interrupted.",
                run.display()
            ),
            Self::Cycle { verb, run, members } => write!(
                f,
                "cannot {} {}: cyclic topology includes {}",
                verb.word(),
                run.display(),
                members.join(", ")
            ),
            Self::Sources { run, refusals } => f.write_str(&sources_refusal(run, refusals)),
        }
    }
}

impl std::error::Error for ArchiveRefusal {}

/// Decide whether `verb` over `run` is admitted.
pub fn decide(
    verb: ReplayVerb,
    run: &Path,
    archive: &ArchiveEvidence,
    flow: &FlowEvidence,
    overrides: ArchiveOverrides,
) -> Result<(), ArchiveRefusal> {
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
    if verb == ReplayVerb::Resume {
        if archive.completed_naturally() {
            return Err(ArchiveRefusal::NoWorkLeft { run });
        }
        let refusals = source_refusals(archive, flow);
        if !refusals.is_empty() {
            return Err(ArchiveRefusal::Sources { run, refusals });
        }
    }
    match (archive.status, verb) {
        (ArchiveStatus::Completed | ArchiveStatus::Cancelled, _)
        | (ArchiveStatus::Unknown, ReplayVerb::Resume) => Ok(()),
        (ArchiveStatus::Unknown, ReplayVerb::Replay) if !overrides.allow_incomplete_archive => {
            Err(ArchiveRefusal::Interrupted { run })
        }
        (ArchiveStatus::Failed, ReplayVerb::Resume)
            if archive.failure == FailureEvidence::DepartedFromArchive =>
        {
            Ok(())
        }
        (ArchiveStatus::Failed, _) if !overrides.allow_incomplete_archive => {
            Err(ArchiveRefusal::Failed { verb, run })
        }
        (ArchiveStatus::Unknown | ArchiveStatus::Failed, _) => Ok(()),
    }
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
    let quoted: Vec<String> = sources.iter().map(|source| format!("'{source}'")).collect();
    let named = match quoted.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    };
    let (subject, their) = if sources.len() == 1 {
        ("source", "its")
    } else {
        ("sources", "their")
    };
    format!(
        "cannot resume {}: {subject} {named} stopped with {their} input unfinished, and only \
         the stopped process held that input.",
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
        }
    }

    fn decided(
        verb: ReplayVerb,
        archive: &ArchiveEvidence,
        flow: &FlowEvidence,
        overrides: ArchiveOverrides,
    ) -> Result<(), ArchiveRefusal> {
        decide(verb, Path::new(RUN), archive, flow, overrides)
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
    fn live_writer_both_verbs_refused() {
        let mut live = archive(ArchiveStatus::Failed, finished());
        live.live_writer = true;
        for (verb, copy) in [
            (
                ReplayVerb::Resume,
                "cannot resume /runs/r1: it is still running.",
            ),
            (
                ReplayVerb::Replay,
                "cannot replay /runs/r1: it is still running.",
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
            "cannot resume /runs/r1: it completed naturally and every source is finite, so \
             there is no work left to continue."
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
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted."
        );
        assert!(decided(ReplayVerb::Replay, &killed, &safe_flow(), incomplete()).is_ok());
    }

    #[test]
    fn a_failed_run_needs_the_override_for_both_verbs() {
        let failed = archive(ArchiveStatus::Failed, finished());
        for (verb, copy) in [
            (ReplayVerb::Resume, "cannot resume /runs/r1: it failed."),
            (ReplayVerb::Replay, "cannot replay /runs/r1: it failed."),
        ] {
            assert_eq!(
                decided(verb, &failed, &safe_flow(), plain())
                    .unwrap_err()
                    .to_string(),
                copy
            );
            assert!(decided(verb, &failed, &safe_flow(), incomplete()).is_ok());
        }
    }

    #[test]
    fn a_departed_resume_resumes_plainly_and_replays_only_with_the_override() {
        let departed = ArchiveEvidence {
            failure: FailureEvidence::DepartedFromArchive,
            ..archive(ArchiveStatus::Failed, finished())
        };
        assert!(decided(ReplayVerb::Resume, &departed, &safe_flow(), plain()).is_ok());
        assert!(matches!(
            decided(ReplayVerb::Replay, &departed, &safe_flow(), plain()),
            Err(ArchiveRefusal::Failed {
                verb: ReplayVerb::Replay,
                ..
            })
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
            "cannot resume /runs/r1: sources 'store_orders' and 'web_orders' stopped with their \
             input unfinished, and only the stopped process held that input."
        );
    }

    #[test]
    fn unfinished_source_refusal_names_every_source() {
        let archive = Path::new("/runs/interrupted");
        assert_eq!(
            unfinished_sources_refusal(archive, &["web_orders".to_string()]),
            "cannot resume /runs/interrupted: source 'web_orders' stopped with its input \
             unfinished, and only the stopped process held that input."
        );
        assert!(unfinished_sources_refusal(
            archive,
            &["a".to_string(), "b".to_string(), "c".to_string()],
        )
        .starts_with(
            "cannot resume /runs/interrupted: sources 'a', 'b' and 'c' stopped with their \
             input unfinished"
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
            archive(ArchiveStatus::Unknown, finished()),
            archive(ArchiveStatus::Failed, finished()),
            live,
        ] {
            for verb in [ReplayVerb::Replay, ReplayVerb::Resume] {
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
    fn failed_resume_unfinished_input_returns_sources() {
        let failed = archive(ArchiveStatus::Failed, unfinished());
        for overrides in [plain(), incomplete()] {
            assert!(matches!(
                decided(ReplayVerb::Resume, &failed, &safe_flow(), overrides),
                Err(ArchiveRefusal::Sources { .. })
            ));
        }
    }
}
