// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Admission of a replay or resume of a run, decided from the run's archive
//! evidence and the flow's own facts. Archive checks run before flow checks.

use super::ArchiveStatus;
use crate::effect_binding::SystemBinding;
use crate::event::payloads::execution_payload::{ExecutionPayload, SourcePollContinuation};
use crate::event::payloads::flow_control_payload::EofKind;
use crate::event::SourceDiagnosticReason;
use crate::source::{ResumeRefusal, SourceContinuation};
use std::fmt;
use std::path::{Path, PathBuf};

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
    pub binding: Option<SystemBinding>,
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
    pub systems: std::collections::BTreeMap<String, SystemBinding>,
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
    SourceSystems {
        verb: ReplayVerb,
        run: PathBuf,
        changed: Vec<ChangedSourceSystem>,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangedSourceSystem {
    pub stage: String,
    pub recorded: SystemBinding,
    pub configured: SystemBinding,
}

impl ArchiveRefusal {
    pub fn verb(&self) -> ReplayVerb {
        match self {
            Self::StillRunning { verb, .. }
            | Self::SourceSystems { verb, .. }
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
            Self::SourceSystems { verb, run, changed } => {
                for (index, change) in changed.iter().enumerate() {
                    if index > 0 {
                        write!(f, "\n\n")?;
                    }
                    let same_place = change.recorded.quoted() == change.configured.quoted();
                    let quote = |binding: &SystemBinding| match binding.format() {
                        Some(format) if same_place => format!("{} ({format})", binding.quoted()),
                        _ => binding.quoted(),
                    };
                    let recorded = quote(&change.recorded);
                    let configured = quote(&change.configured);
                    write!(
                        f,
                        "cannot {} {}: source '{}' recorded its input from {recorded}, and this flow binds {configured}.",
                        verb.word(),
                        run.display(),
                        change.stage,
                    )?;
                    match verb {
                        ReplayVerb::Replay => {
                            write!(f, "\nReplay it with a configuration that binds {recorded}.")?
                        }
                        ReplayVerb::Resume => write!(
                            f,
                            "\nResume it with a configuration that binds {recorded}. To read from {configured}, start a new run."
                        )?,
                    }
                }
                Ok(())
            }
            Self::StillRunning { verb, run } => write!(
                f,
                "cannot {verb} {run}: it is still running.\nStop it, then {verb} it again.",
                verb = verb.word(),
                run = run.display()
            ),
            Self::NoWorkLeft { run } => write!(
                f,
                "cannot resume {run}: it completed naturally and every source is finite, so \
                 there is no work left to continue.\nReplay it with --replay-from {run}.",
                run = run.display()
            ),
            Self::Failed { verb, run } => write!(
                f,
                "cannot {verb} {run}: it failed.\nPass --allow-incomplete-archive to {verb} it \
                 anyway.",
                verb = verb.word(),
                run = run.display()
            ),
            Self::Interrupted { run } => write!(
                f,
                "cannot replay {run}: it has no terminal record, so it was interrupted.\nResume \
                 it with --resume-from {run}, or pass --allow-incomplete-archive to replay its \
                 recorded prefix.",
                run = run.display()
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
    if archive.live_writer {
        return Err(ArchiveRefusal::StillRunning { verb, run });
    }
    match (archive.status, verb) {
        (ArchiveStatus::Completed, ReplayVerb::Resume) if archive.completed_naturally() => {
            return Err(ArchiveRefusal::NoWorkLeft { run });
        }
        (ArchiveStatus::Completed | ArchiveStatus::Cancelled, _)
        | (ArchiveStatus::Unknown, ReplayVerb::Resume) => {}
        (ArchiveStatus::Unknown, ReplayVerb::Replay) if !overrides.allow_incomplete_archive => {
            return Err(ArchiveRefusal::Interrupted { run });
        }
        (ArchiveStatus::Failed, ReplayVerb::Resume)
            if archive.failure == FailureEvidence::DepartedFromArchive => {}
        (ArchiveStatus::Failed, _) if !overrides.allow_incomplete_archive => {
            return Err(ArchiveRefusal::Failed { verb, run });
        }
        (ArchiveStatus::Unknown | ArchiveStatus::Failed, _) => {}
    }

    if !flow.cycle_members.is_empty() {
        return Err(ArchiveRefusal::Cycle {
            verb,
            run,
            members: flow.cycle_members.clone(),
        });
    }
    let mut changed: Vec<_> = archive
        .sources
        .iter()
        .filter_map(|source| {
            let recorded = source.binding.as_ref()?;
            let configured = flow.systems.get(&source.stage_key)?;
            (recorded != configured).then(|| ChangedSourceSystem {
                stage: source.stage_key.clone(),
                recorded: recorded.clone(),
                configured: configured.clone(),
            })
        })
        .collect();
    if !changed.is_empty() {
        changed.sort_by(|a, b| a.stage.cmp(&b.stage));
        return Err(ArchiveRefusal::SourceSystems { verb, run, changed });
    }
    if verb == ReplayVerb::Resume {
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
        if !refusals.is_empty() {
            return Err(ArchiveRefusal::Sources { run, refusals });
        }
    }
    Ok(())
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
            binding: None,
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
            systems: Default::default(),
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
    fn a_live_writer_refuses_both_verbs_before_any_other_row() {
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
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nResume \
             it with --resume-from /runs/r1, or pass --allow-incomplete-archive to replay its \
             recorded prefix."
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
    fn archive_rows_are_checked_before_the_flows() {
        let failed = archive(ArchiveStatus::Failed, finished());
        let everything_wrong = FlowEvidence {
            systems: Default::default(),
            cycle_members: vec!["triage".to_string()],
            sources: safe_flow().sources,
        };
        assert!(matches!(
            decided(ReplayVerb::Resume, &failed, &everything_wrong, plain()),
            Err(ArchiveRefusal::Failed { .. })
        ));
        let stopped = archive(ArchiveStatus::Cancelled, finished());
        assert!(matches!(
            decided(ReplayVerb::Resume, &stopped, &everything_wrong, plain()),
            Err(ArchiveRefusal::Cycle { .. })
        ));
    }

    #[test]
    fn moved_ingress_endpoint_refused() {
        use crate::effect_binding::{ConfigurationEvidence, SystemBindingEvidence};
        let endpoint = |key: &str| {
            SystemBindingEvidence::configured(
                "http-ingress",
                ConfigurationEvidence::Ingress {
                    key: key.to_owned(),
                },
            )
            .unwrap()
            .binding()
            .clone()
        };
        let mut recorded = source("live_tx", false, None);
        recorded.binding = Some(endpoint("/api/bank/tx"));
        let archive = archive(ArchiveStatus::Cancelled, vec![recorded]);
        let flow = FlowEvidence {
            systems: [("live_tx".to_string(), endpoint("/api/bank/v2/tx"))]
                .into_iter()
                .collect(),
            cycle_members: Vec::new(),
            sources: Vec::new(),
        };
        for verb in [ReplayVerb::Replay, ReplayVerb::Resume] {
            let refusal = decide(
                verb,
                Path::new(RUN),
                &archive,
                &flow,
                ArchiveOverrides::default(),
            )
            .expect_err("a moved endpoint is another supplier");
            assert!(matches!(refusal, ArchiveRefusal::SourceSystems { .. }));
            let message = refusal.to_string();
            for expected in [
                "'http-ingress' at '/api/bank/tx'",
                "'http-ingress' at '/api/bank/v2/tx'",
            ] {
                assert!(message.contains(expected), "{expected}: {message}");
            }
        }
    }
}
