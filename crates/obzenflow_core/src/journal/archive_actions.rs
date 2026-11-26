// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! `archive_actions` admits or refuses a replay or a resume of a run. It
//! decides from the archive evidence of the run and from the facts of the flow.
//! [`decide`] checks the cycle first and a live writer next. For a resume, it
//! then checks the no-work-left row and the sources. It checks the archive rows
//! last, so no refusal names a command that an earlier check refuses.

use super::ArchiveStatus;
use crate::event::payloads::execution_payload::{ExecutionPayload, SourcePollContinuation};
use crate::event::payloads::flow_control_payload::EofKind;
use crate::event::SourceDiagnosticReason;
use crate::source::{ResumeRefusal, SourceContinuation};
use std::fmt;
use std::path::{Path, PathBuf};

/// `ADMISSION_TABLE` shows the admission that [`decide`] returns for each
/// archive status, verb and override. `--help` prints it after the options. A
/// test compares it with [`decide`].
pub const ADMISSION_TABLE: &str = "\
Archive admission (the flag is --allow-incomplete-archive):
Replay uses the named run. Resume follows replay links and reads any
ancestors needed to finish reconstruction. Apply each column to that run.
Archive          | --replay-from | --resume-from
completed, every source finite and ended naturally
                 | admitted      | refused: no work left
completed with a source stopped, or cancelled
                 | admitted      | admitted
interrupted (no terminal record)
                 | with the flag | admitted
failed resume: archive departures were its only cause
                 | with the flag | admitted
all other failures
                 | with the flag | with the flag
A still-running run refuses both.
A cyclic topology refuses both.
A resume refuses a source whose connector cannot continue its recorded input.";

/// The operator's archive verb, a bounded replay that drains or a resume that
/// catches up and continues live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVerb {
    /// `--replay-from`.
    Replay,
    /// `--resume-from`.
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

/// Whether an error-journal entry is a resume refusal for departing from the
/// archive, either a stage-fatal departure or a source whose input changed.
pub fn departs_from_archive(execution: &ExecutionPayload) -> bool {
    match execution {
        ExecutionPayload::StageFatalRecorded(fatal) => fatal.reason.departs_from_archive(),
        ExecutionPayload::SourcePollFailed(failure) => {
            failure.continuation == SourcePollContinuation::Terminal
                && failure.diagnostic.reason() == SourceDiagnosticReason::InputChanged
        }
        _ => false,
    }
}

/// Folds a failed resume's error-journal entries and terminal record into its
/// [`FailureEvidence`].
#[derive(Debug, Default)]
pub struct FailureFold {
    departed: bool,
    other: bool,
}

impl FailureFold {
    pub fn observe(&mut self, execution: &ExecutionPayload) {
        if departs_from_archive(execution) {
            self.departed = true;
        } else if match execution {
            ExecutionPayload::StageFatalRecorded(_) => true,
            ExecutionPayload::SourcePollFailed(failure) => {
                failure.continuation == SourcePollContinuation::Terminal
            }
            _ => false,
        } {
            self.other = true;
        }
    }

    /// `DepartedFromArchive` after at least one departure and no other failure,
    /// when the terminal record names no contract violation.
    pub fn finish(self, contract_violation: bool) -> FailureEvidence {
        if self.departed && !self.other && !contract_violation {
            FailureEvidence::DepartedFromArchive
        } else {
            FailureEvidence::Other
        }
    }
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
    pub resume: Option<Box<ResumeEvidence>>,
}

/// `ResumeEvidence` holds the observed head and the archive evidence for a plain resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeEvidence {
    pub run: PathBuf,
    pub continued_from: Option<PathBuf>,
    pub archive: ArchiveEvidence,
}

/// `ResumeAdvice` holds the admission decision for the specified head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeAdvice {
    pub run: PathBuf,
    pub continued_from: Option<PathBuf>,
    pub admission: Result<(), ArchiveRefusal>,
}

impl ResumeEvidence {
    /// `judge` checks a plain resume against the facts of the built flow.
    pub fn judge(&self, flow: &FlowEvidence) -> ResumeAdvice {
        let admission = decide(
            ReplayVerb::Resume,
            &self.run,
            &self.archive,
            flow,
            ArchiveOverrides::default(),
        );
        ResumeAdvice {
            run: self.run.clone(),
            continued_from: self.continued_from.clone(),
            admission,
        }
    }
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

/// Why an archive verb was refused.
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
        resume: Option<Box<ResumeAdvice>>,
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

/// `shell_path` renders an archive path as one POSIX shell argument.
pub fn shell_path(path: &Path) -> String {
    use std::fmt::Write;

    let bytes = path.as_os_str().as_encoded_bytes();
    if !bytes.is_empty()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(b))
    {
        return path.display().to_string();
    }
    let mut word = String::from("'");
    for chunk in bytes.utf8_chunks() {
        word.push_str(&chunk.valid().replace('\'', r"'\''"));
        if !chunk.invalid().is_empty() {
            word.push_str("'\"$(printf '");
            for byte in chunk.invalid() {
                write!(word, "\\{byte:03o}").expect("writing to a String succeeds");
            }
            word.push_str("')\"'");
        }
    }
    word.push('\'');
    word
}

impl fmt::Display for ArchiveRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StillRunning { verb, run } => write!(
                f,
                "cannot {verb} {run}: it is still running.\nStop it, then {verb} it again.",
                verb = verb.word(),
                run = run.display()
            ),
            Self::NoWorkLeft { run } => write!(
                f,
                "cannot resume {run}: it completed naturally and every source is finite, so \
                 there is no work left to continue.\nReplay it with --replay-from {operand}.",
                run = run.display(),
                operand = shell_path(run)
            ),
            Self::Failed { verb, run } => write!(
                f,
                "cannot {verb} {run}: it failed.\nPass --allow-incomplete-archive to {verb} it \
                 anyway.",
                verb = verb.word(),
                run = run.display()
            ),
            Self::Interrupted { run, resume } => {
                writeln!(
                    f,
                    "cannot replay {}: it has no terminal record, so it was interrupted.",
                    run.display()
                )?;
                let prefix = "Pass --allow-incomplete-archive to replay its recorded prefix.";
                let Some(advice) = resume else {
                    return f.write_str(prefix);
                };
                if advice.admission.is_ok() && advice.run == *run {
                    return write!(
                        f,
                        "Resume it with --resume-from {}, or pass --allow-incomplete-archive \
                            to replay its recorded prefix.",
                        shell_path(run)
                    );
                }
                if let Some(parent) = &advice.continued_from {
                    writeln!(
                        f,
                        "Pass --allow-incomplete-archive to replay the recorded prefix of {}.",
                        run.display()
                    )?;
                    writeln!(
                        f,
                        "{} continued {}.",
                        advice.run.display(),
                        parent.display()
                    )?;
                } else {
                    writeln!(f, "{prefix}")?;
                }
                match &advice.admission {
                    Ok(()) => write!(
                        f,
                        "Resume it with --resume-from {}.",
                        shell_path(&advice.run)
                    ),
                    Err(refusal) => write!(f, "{refusal}"),
                }
            }
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
            let resume = archive
                .resume
                .as_deref()
                .map(|target| Box::new(target.judge(flow)));
            Err(ArchiveRefusal::Interrupted { run, resume })
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
         the stopped process held that input.\nStart a new run.",
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
            resume: None,
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
                "cannot resume /runs/r1: it is still running.\nStop it, then resume it again.",
            ),
            (
                ReplayVerb::Replay,
                "cannot replay /runs/r1: it is still running.\nStop it, then replay it again.",
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
             there is no work left to continue.\nReplay it with --replay-from /runs/r1."
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
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nPass \
             --allow-incomplete-archive to replay its recorded prefix."
        );
        assert!(decided(ReplayVerb::Replay, &killed, &safe_flow(), incomplete()).is_ok());
    }

    #[test]
    fn a_failed_run_needs_the_override_for_both_verbs() {
        let failed = archive(ArchiveStatus::Failed, finished());
        for (verb, copy) in [
            (
                ReplayVerb::Resume,
                "cannot resume /runs/r1: it failed.\nPass --allow-incomplete-archive to resume \
                 it anyway.",
            ),
            (
                ReplayVerb::Replay,
                "cannot replay /runs/r1: it failed.\nPass --allow-incomplete-archive to replay \
                 it anyway.",
            ),
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

    fn fatal(reason: crate::event::StageFatalReason) -> ExecutionPayload {
        use crate::event::{StageFatalCode, StageFatalRecorded, StageFatalSeverity};
        ExecutionPayload::StageFatalRecorded(StageFatalRecorded {
            severity: StageFatalSeverity::Primary,
            stage_id: crate::StageId::new(),
            stage_key: "score".to_string(),
            causal_event_id: None,
            input_position: None,
            primary_cause_event_id: None,
            code: StageFatalCode::Replay,
            reason,
            detail: String::new(),
            departure: None,
        })
    }

    fn poll_failed(
        continuation: SourcePollContinuation,
        reason: SourceDiagnosticReason,
    ) -> ExecutionPayload {
        ExecutionPayload::SourcePollFailed(
            crate::event::payloads::execution_payload::SourcePollFailedFact {
                kind: crate::event::status::processing_status::ErrorKind::Remote,
                detail: String::new(),
                continuation,
                diagnostic: reason.into(),
            },
        )
    }

    fn folded(rows: &[ExecutionPayload], contract_violation: bool) -> FailureEvidence {
        let mut fold = FailureFold::default();
        for row in rows {
            fold.observe(row);
        }
        fold.finish(contract_violation)
    }

    #[test]
    fn only_departures_alone_fold_to_departed_from_archive() {
        use crate::event::StageFatalReason;
        let input_changed = poll_failed(
            SourcePollContinuation::Terminal,
            SourceDiagnosticReason::InputChanged,
        );
        for departures in [
            vec![fatal(StageFatalReason::DescriptorMismatch)],
            vec![fatal(StageFatalReason::MissingRecordedFact)],
            vec![
                fatal(StageFatalReason::MissingRecordedEffect),
                fatal(StageFatalReason::UnusedFactHistory),
            ],
            vec![input_changed.clone()],
        ] {
            assert_eq!(
                folded(&departures, false),
                FailureEvidence::DepartedFromArchive,
                "{departures:?}"
            );
            assert_eq!(folded(&departures, true), FailureEvidence::Other);
        }
        for other in [
            vec![],
            vec![fatal(StageFatalReason::JournalFailure)],
            vec![
                fatal(StageFatalReason::UnusedEffectHistory),
                fatal(StageFatalReason::JournalFailure),
            ],
            vec![
                input_changed,
                poll_failed(
                    SourcePollContinuation::Terminal,
                    SourceDiagnosticReason::RemoteFailed,
                ),
            ],
            vec![poll_failed(
                SourcePollContinuation::Recoverable,
                SourceDiagnosticReason::InputChanged,
            )],
            vec![fatal(StageFatalReason::RecordingEnded)],
        ] {
            assert_eq!(folded(&other, false), FailureEvidence::Other, "{other:?}");
        }
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
    fn an_unfinished_refusing_source_refuses_resume_and_names_its_remedy() {
        let early = archive(
            ArchiveStatus::Cancelled,
            vec![
                source("store_orders", true, Some(EofKind::Truncated)),
                source("web_orders", true, None),
            ],
        );
        assert!(decided(ReplayVerb::Replay, &early, &safe_flow(), plain()).is_ok());
        assert_eq!(
            decided(ReplayVerb::Resume, &early, &safe_flow(), plain())
                .unwrap_err()
                .to_string(),
            "cannot resume /runs/r1: sources 'store_orders' and 'web_orders' stopped with their \
             input unfinished, and only the stopped process held that input.\nStart a new run."
        );
    }

    #[test]
    fn unfinished_source_refusal_names_every_source() {
        let archive = Path::new("/runs/interrupted");
        assert_eq!(
            unfinished_sources_refusal(archive, &["web_orders".to_string()]),
            "cannot resume /runs/interrupted: source 'web_orders' stopped with its input \
             unfinished, and only the stopped process held that input.\nStart a new run."
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

    #[test]
    fn interrupted_replay_refused_resume_prints_refusal() {
        let killed = ArchiveEvidence {
            resume: Some(Box::new(ResumeEvidence {
                run: PathBuf::from(RUN),
                continued_from: None,
                archive: archive(ArchiveStatus::Unknown, unfinished()),
            })),
            ..archive(ArchiveStatus::Unknown, unfinished())
        };
        assert_eq!(
            decided(ReplayVerb::Replay, &killed, &safe_flow(), plain())
                .unwrap_err()
                .to_string(),
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nPass \
             --allow-incomplete-archive to replay its recorded prefix.\ncannot resume /runs/r1: \
             sources 'store_orders' and 'web_orders' stopped with their input unfinished, and \
             only the stopped process held that input.\nStart a new run."
        );
        let killed_replay = ArchiveEvidence {
            resume: Some(Box::new(ResumeEvidence {
                run: PathBuf::from("/runs/r0"),
                continued_from: None,
                archive: archive(ArchiveStatus::Completed, finished()),
            })),
            ..archive(ArchiveStatus::Unknown, unfinished())
        };
        assert_eq!(
            decided(ReplayVerb::Replay, &killed_replay, &safe_flow(), plain())
                .unwrap_err()
                .to_string(),
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nPass \
             --allow-incomplete-archive to replay its recorded prefix.\ncannot resume /runs/r0: \
             it completed naturally and every source is finite, so there is no work left to \
             continue.\nReplay it with --replay-from /runs/r0."
        );
    }

    #[test]
    fn interrupted_replay_admitted_resume_named() {
        let killed = ArchiveEvidence {
            resume: Some(Box::new(ResumeEvidence {
                run: PathBuf::from(RUN),
                continued_from: None,
                archive: archive(ArchiveStatus::Unknown, finished()),
            })),
            ..archive(ArchiveStatus::Unknown, finished())
        };
        assert_eq!(
            decided(ReplayVerb::Replay, &killed, &safe_flow(), plain())
                .unwrap_err()
                .to_string(),
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nResume \
             it with --resume-from /runs/r1, or pass --allow-incomplete-archive to replay its \
             recorded prefix."
        );
    }

    #[test]
    fn replay_claimed_head_names_resume() {
        let killed = ArchiveEvidence {
            resume: Some(Box::new(ResumeEvidence {
                run: PathBuf::from("/runs/Pat's Orders/head"),
                continued_from: Some(PathBuf::from(RUN)),
                archive: archive(ArchiveStatus::Unknown, finished()),
            })),
            ..archive(ArchiveStatus::Unknown, finished())
        };
        let refusal = decided(ReplayVerb::Replay, &killed, &safe_flow(), plain()).unwrap_err();
        assert!(refusal
            .to_string()
            .ends_with("/runs/Pat's Orders/head continued /runs/r1.\nResume it with --resume-from '/runs/Pat'\\''s Orders/head'."));
    }

    const ROWS: [&str; 5] = [
        "completed, every source finite and ended naturally",
        "completed with a source stopped, or cancelled",
        "interrupted (no terminal record)",
        "failed resume: archive departures were its only cause",
        "all other failures",
    ];

    fn row_of(evidence: &ArchiveEvidence) -> &'static str {
        match evidence.status {
            ArchiveStatus::Completed
                if evidence.sources.iter().all(|source| {
                    source.finite && source.recorded_eof == Some(EofKind::Natural)
                }) =>
            {
                ROWS[0]
            }
            ArchiveStatus::Completed | ArchiveStatus::Cancelled => ROWS[1],
            ArchiveStatus::Unknown => ROWS[2],
            ArchiveStatus::Failed if evidence.failure == FailureEvidence::DepartedFromArchive => {
                ROWS[3]
            }
            ArchiveStatus::Failed => ROWS[4],
        }
    }

    fn cell(verb: ReplayVerb, evidence: &ArchiveEvidence, flow: &FlowEvidence) -> String {
        match (
            decided(verb, evidence, flow, plain()),
            decided(verb, evidence, flow, incomplete()),
        ) {
            (Ok(()), Ok(())) => "admitted".to_string(),
            (Err(ArchiveRefusal::Failed { .. } | ArchiveRefusal::Interrupted { .. }), Ok(())) => {
                "with the flag".to_string()
            }
            (Err(ArchiveRefusal::NoWorkLeft { .. }), Err(ArchiveRefusal::NoWorkLeft { .. })) => {
                "refused: no work left".to_string()
            }
            (plain, flagged) => panic!(
                "{}: --{verb}-from {RUN}: {plain:?}; --{verb}-from {RUN} --allow-incomplete-archive: {flagged:?}",
                row_of(evidence), verb = verb.word()
            ),
        }
    }

    fn source_shapes() -> Vec<Vec<SourceEvidence>> {
        vec![
            vec![source("web_orders", true, Some(EofKind::Natural))],
            finished(),
            vec![source("web_orders", true, Some(EofKind::Truncated))],
            vec![source("web_orders", true, Some(EofKind::Poison))],
            vec![source("web_orders", true, None)],
            vec![source("web_orders", false, Some(EofKind::Natural))],
            vec![source("web_orders", false, None)],
            vec![
                source("store_orders", true, Some(EofKind::Natural)),
                source("web_orders", true, Some(EofKind::Truncated)),
            ],
        ]
    }

    fn check_line(
        subject: &str,
        evidence: &ArchiveEvidence,
        flow: &FlowEvidence,
        triggered: fn(&ArchiveRefusal) -> bool,
    ) -> String {
        let refused = |verb: ReplayVerb| {
            let decision =
                |overrides: ArchiveOverrides| match decided(verb, evidence, flow, overrides) {
                    Ok(()) => false,
                    Err(refusal) if triggered(&refusal) => true,
                    Err(other) => panic!(
                        "unexpected refusal for {subject}, {verb:?}, {overrides:?}: {other:?}"
                    ),
                };
            let plain = decision(plain());
            let flagged = decision(incomplete());
            assert_eq!(plain, flagged,
                "{subject}: --{verb}-from {RUN} and --{verb}-from {RUN} --allow-incomplete-archive disagree",
                verb = verb.word());
            plain
        };
        match (refused(ReplayVerb::Replay), refused(ReplayVerb::Resume)) {
            (true, true) => format!("A {subject} refuses both."),
            (false, true) => format!("A resume refuses {subject}."),
            (true, false) => format!("A replay refuses {subject}."),
            (false, false) => format!("No verb refuses {subject}."),
        }
    }

    #[test]
    fn admission_table_all_rows_match() {
        let continuing = FlowEvidence {
            cycle_members: Vec::new(),
            sources: vec![
                ("store_orders".to_string(), SourceContinuation::RecordedRead),
                ("web_orders".to_string(), SourceContinuation::RecordedRead),
            ],
        };
        let mut cells: Vec<Option<(String, String)>> = vec![None; ROWS.len()];
        for status in [
            ArchiveStatus::Completed,
            ArchiveStatus::Cancelled,
            ArchiveStatus::Unknown,
            ArchiveStatus::Failed,
        ] {
            for failure in [FailureEvidence::Other, FailureEvidence::DepartedFromArchive] {
                for sources in source_shapes() {
                    let evidence = ArchiveEvidence {
                        failure,
                        ..archive(status, sources)
                    };
                    let row = row_of(&evidence);
                    let index = ROWS.iter().position(|label| *label == row).unwrap();
                    let rendered = (
                        cell(ReplayVerb::Replay, &evidence, &continuing),
                        cell(ReplayVerb::Resume, &evidence, &continuing),
                    );
                    match &cells[index] {
                        Some(first) => assert_eq!(first, &rendered, "{row}: {evidence:?}"),
                        None => cells[index] = Some(rendered),
                    }
                }
            }
        }

        let mut lines = vec![
            "Archive admission (the flag is --allow-incomplete-archive):".to_string(),
            "Replay uses the named run. Resume follows replay links and reads any".to_string(),
            "ancestors needed to finish reconstruction. Apply each column to that run.".to_string(),
            "Archive          | --replay-from | --resume-from".to_string(),
        ];
        for (label, rendered) in ROWS.iter().zip(cells) {
            let (replay, resume) = rendered.expect("every row has evidence");
            lines.push((*label).to_string());
            lines.push(format!("{:17}| {replay:<13} | {resume}", ""));
        }
        let mut live = archive(ArchiveStatus::Cancelled, finished());
        live.live_writer = true;
        lines.push(check_line(
            "still-running run",
            &live,
            &continuing,
            |refusal| matches!(refusal, ArchiveRefusal::StillRunning { .. }),
        ));
        let cyclic = FlowEvidence {
            cycle_members: vec!["review".to_string(), "triage".to_string()],
            ..continuing.clone()
        };
        lines.push(check_line(
            "cyclic topology",
            &archive(ArchiveStatus::Cancelled, finished()),
            &cyclic,
            |refusal| matches!(refusal, ArchiveRefusal::Cycle { .. }),
        ));
        lines.push(check_line(
            "a source whose connector cannot continue its recorded input",
            &archive(ArchiveStatus::Cancelled, unfinished()),
            &safe_flow(),
            |refusal| matches!(refusal, ArchiveRefusal::Sources { .. }),
        ));

        assert_eq!(lines.join("\n"), ADMISSION_TABLE);
    }
}
