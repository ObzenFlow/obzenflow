// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Archive verb admission (FLOWIP-134k §1, B4). One evaluator decides whether
//! a replay or resume of a run is admitted, from the run's archive evidence
//! and the flow's own facts, for the build and the footer alike. Archive rows
//! are checked first, then the flow's.

use super::ArchiveStatus;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::source::{RecordedSourceFrontier, RefusedBy, ResumeRefusal, SourceContinuation};
use std::fmt;
use std::path::{Path, PathBuf};

/// The operator's archive verb (FLOWIP-120n): bounded reconstruction that
/// drains, or catch-up that continues live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayVerb {
    /// `--replay-from`.
    Replay,
    /// `--resume-from`.
    Resume,
}

impl ReplayVerb {
    fn word(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Resume => "resume",
        }
    }
}

/// Replay/resume safety of a configured sink (FLOWIP-120n F16).
/// Read only by archive admission; live behaviour never consults it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkRedeliverySafety {
    /// Repeating a recorded write is acceptable for this configured sink.
    SafeToRepeat,
    /// Repeating a recorded write can create externally visible duplicates.
    /// Archive execution refuses without `allow_duplicate_sink_delivery`.
    DuplicateSensitive,
}

/// What one source journal of the archive recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEvidence {
    pub stage_key: String,
    pub finite: bool,
    pub recorded_eof: Option<EofKind>,
    pub frontier: RecordedSourceFrontier,
}

/// What Infra decoded from an archive: its derived status, whether any of its
/// journals still has a live writer, and what each source journal recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveEvidence {
    pub status: ArchiveStatus,
    pub live_writer: bool,
    pub sources: Vec<SourceEvidence>,
}

impl ArchiveEvidence {
    fn source(&self, stage_key: &str) -> Option<&SourceEvidence> {
        self.sources
            .iter()
            .find(|source| source.stage_key == stage_key)
    }

    /// A completed run left no work when every source is finite and ended
    /// naturally.
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
    /// Each sink's declared redelivery safety, by stage key.
    pub sinks: Vec<(String, Option<SinkRedeliverySafety>)>,
}

/// The operator's overrides for one command.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchiveOverrides {
    pub allow_incomplete_archive: bool,
    pub allow_duplicate_sink_delivery: bool,
}

/// An admitted verb, with the sinks admitted only under
/// `allow_duplicate_sink_delivery`, for the caller to warn about.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Admission {
    pub duplicate_sinks: Vec<(String, Option<SinkRedeliverySafety>)>,
}

/// Why a verb is refused. Each prints verbatim as the verb's refusal.
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
    /// FLOWIP-120n F13: cyclic resume is a v1 non-goal.
    Cycle {
        members: Vec<String>,
    },
    /// FLOWIP-134i B8, B22: sources whose recorded input cannot be continued.
    Sources {
        run: PathBuf,
        refusals: Vec<(String, ResumeRefusal)>,
    },
    /// FLOWIP-120n F16, FLOWIP-120v: a sink without safe redelivery.
    Sink {
        verb: ReplayVerb,
        stage: String,
        undeclared: bool,
    },
}

impl ArchiveRefusal {
    pub fn verb(&self) -> ReplayVerb {
        match self {
            Self::StillRunning { verb, .. }
            | Self::Failed { verb, .. }
            | Self::Sink { verb, .. } => *verb,
            Self::NoWorkLeft { .. } | Self::Cycle { .. } | Self::Sources { .. } => {
                ReplayVerb::Resume
            }
            Self::Interrupted { .. } => ReplayVerb::Replay,
        }
    }
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
            Self::Cycle { members } => write!(
                f,
                "--resume-from does not support cyclic topologies (FLOWIP-120n F13); cycle \
                 members: {}",
                members.join(", ")
            ),
            Self::Sources { run, refusals } => f.write_str(&sources_refusal(run, refusals)),
            Self::Sink {
                verb,
                stage,
                undeclared,
            } => f.write_str(&sink_refusal(*verb, stage, *undeclared)),
        }
    }
}

impl std::error::Error for ArchiveRefusal {}

/// Decide whether `verb` over `run` is admitted (FLOWIP-134k §1, B4).
pub fn decide(
    verb: ReplayVerb,
    run: &Path,
    archive: &ArchiveEvidence,
    flow: &FlowEvidence,
    overrides: ArchiveOverrides,
) -> Result<Admission, ArchiveRefusal> {
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
        (ArchiveStatus::Failed, _) if !overrides.allow_incomplete_archive => {
            return Err(ArchiveRefusal::Failed { verb, run });
        }
        (ArchiveStatus::Unknown | ArchiveStatus::Failed, _) => {}
    }

    if verb == ReplayVerb::Resume {
        if !flow.cycle_members.is_empty() {
            return Err(ArchiveRefusal::Cycle {
                members: flow.cycle_members.clone(),
            });
        }
        let mut refusals: Vec<(String, ResumeRefusal)> = flow
            .sources
            .iter()
            .filter_map(|(stage_key, continuation)| {
                let recorded = archive.source(stage_key);
                let frontier = recorded.map(|source| &source.frontier);
                continuation
                    .resume_refusal(
                        recorded.and_then(|source| source.recorded_eof),
                        frontier.unwrap_or(&RecordedSourceFrontier::default()),
                    )
                    .map(|refusal| (stage_key.clone(), refusal))
            })
            .collect();
        refusals.sort_by(|(left, _), (right, _)| left.cmp(right));
        if !refusals.is_empty() {
            return Err(ArchiveRefusal::Sources { run, refusals });
        }
    }

    let mut sinks = flow.sinks.clone();
    sinks.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut admission = Admission::default();
    for (stage, safety) in sinks {
        if safety == Some(SinkRedeliverySafety::SafeToRepeat) {
            continue;
        }
        if !overrides.allow_duplicate_sink_delivery {
            return Err(ArchiveRefusal::Sink {
                verb,
                stage,
                undeclared: safety.is_none(),
            });
        }
        admission.duplicate_sinks.push((stage, safety));
    }
    Ok(admission)
}

// One diagnostic names every source whose connector, or direct handler, cannot
// continue its interrupted input (FLOWIP-134i B8, FLOWIP-122i).
fn unfinished_sources_refusal(archive: &Path, sources: &[String], by: RefusedBy) -> String {
    let quoted: Vec<String> = sources.iter().map(|source| format!("'{source}'")).collect();
    let named = match quoted.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    };
    let refuser = match by {
        RefusedBy::Connector => "connector",
        RefusedBy::Handler => "handler",
    };
    let (subject, their, refusers) = if sources.len() == 1 {
        ("source", "its", format!("its {refuser} cannot"))
    } else {
        ("sources", "their", format!("their {refuser}s cannot"))
    };
    format!(
        "cannot resume {}: {subject} {named} stopped with {their} input unfinished, and \
         {refusers} continue interrupted input.\nUse a connector that continues \
         interrupted input, or implement one.",
        archive.display()
    )
}

// Every source a resume refuses at archive admission, before any claim
// (FLOWIP-134i B8, B22).
fn sources_refusal(archive: &Path, refusals: &[(String, ResumeRefusal)]) -> String {
    let mut paragraphs = Vec::new();
    for refuser in [RefusedBy::Connector, RefusedBy::Handler] {
        let unfinished: Vec<String> = refusals
            .iter()
            .filter(|(_, refusal)| *refusal == ResumeRefusal::UnfinishedInput { by: refuser })
            .map(|(source, _)| source.clone())
            .collect();
        if !unfinished.is_empty() {
            paragraphs.push(unfinished_sources_refusal(archive, &unfinished, refuser));
        }
    }
    for (source, refusal) in refusals {
        match refusal {
            ResumeRefusal::UnfinishedInput { .. } => {}
            ResumeRefusal::RecordedWithoutPositions => paragraphs.push(format!(
                "cannot resume {}: source '{source}' recorded its input without positions, so \
                 its connector cannot continue it from where it stopped.\nStart a new run.",
                archive.display()
            )),
            ResumeRefusal::PositionVersion { recorded, expected } => paragraphs.push(format!(
                "cannot resume {}: source '{source}' recorded positions in version {recorded} \
                 of its connector's format, and the connector now reads version {expected}.\n\
                 Resume with the version that recorded them, or start a new run.",
                archive.display()
            )),
        }
    }
    paragraphs.join("\n")
}

// Verb-aware refusal copy for the archive sink delivery-safety row
// (FLOWIP-120n F16, extended to both archive verbs by FLOWIP-120v).
fn sink_refusal(verb: ReplayVerb, stage: &str, undeclared: bool) -> String {
    let (flag, re_execution) = match verb {
        ReplayVerb::Replay => (
            "--replay-from",
            "deterministic replay re-consumes the recorded stream, so this sink would \
             re-perform its external writes",
        ),
        ReplayVerb::Resume => (
            "--resume-from",
            "resume re-delivers the recorded prefix during catch-up, which would \
             duplicate those writes",
        ),
    };
    if undeclared {
        format!(
            "{flag} refused: sink '{stage}' has no redelivery-safety classification; \
             {re_execution}, and the gate fails closed on undeclared sinks \
             (FLOWIP-120n F16, FLOWIP-120v). Bind the sink before `flow!`, then use \
             `delivery: idempotent` or `delivery: non_idempotent` on its `sink!` row, \
             or return a \
             classified `SinkDescription` from a custom `SinkConnector` or \
             `InlineSink`. A duplicate-sensitive external write belongs behind \
             the effect boundary (effectful transform plus plain sink). Or pass \
             `allow_duplicate_sink_delivery`."
        )
    } else {
        format!(
            "{flag} refused: sink '{stage}' declares a duplicate-sensitive external \
             delivery path; {re_execution} (FLOWIP-120n F16, FLOWIP-120v). Declare \
             `delivery: idempotent` if the destination absorbs duplicates, route the \
             write through the effect boundary (effectful transform plus plain sink), \
             or pass `allow_duplicate_sink_delivery` to accept the duplication."
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    const RUN: &str = "/runs/r1";

    fn source(stage_key: &str, finite: bool, recorded_eof: Option<EofKind>) -> SourceEvidence {
        SourceEvidence {
            stage_key: stage_key.to_string(),
            finite,
            recorded_eof,
            frontier: RecordedSourceFrontier::default(),
        }
    }

    fn archive(status: ArchiveStatus, sources: Vec<SourceEvidence>) -> ArchiveEvidence {
        ArchiveEvidence {
            status,
            live_writer: false,
            sources,
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
                (
                    "store_orders".to_string(),
                    SourceContinuation::Refuse {
                        by: RefusedBy::Connector,
                    },
                ),
                (
                    "web_orders".to_string(),
                    SourceContinuation::Refuse {
                        by: RefusedBy::Connector,
                    },
                ),
            ],
            sinks: vec![(
                "console".to_string(),
                Some(SinkRedeliverySafety::SafeToRepeat),
            )],
        }
    }

    fn decided(
        verb: ReplayVerb,
        archive: &ArchiveEvidence,
        flow: &FlowEvidence,
        overrides: ArchiveOverrides,
    ) -> Result<Admission, ArchiveRefusal> {
        decide(verb, Path::new(RUN), archive, flow, overrides)
    }

    fn plain() -> ArchiveOverrides {
        ArchiveOverrides::default()
    }

    fn incomplete() -> ArchiveOverrides {
        ArchiveOverrides {
            allow_incomplete_archive: true,
            allow_duplicate_sink_delivery: false,
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
    fn a_cycle_refuses_resume_only() {
        let stopped = archive(ArchiveStatus::Cancelled, finished());
        let cyclic = FlowEvidence {
            cycle_members: vec!["review".to_string(), "triage".to_string()],
            ..safe_flow()
        };
        assert!(decided(ReplayVerb::Replay, &stopped, &cyclic, plain()).is_ok());
        assert_eq!(
            decided(ReplayVerb::Resume, &stopped, &cyclic, plain())
                .unwrap_err()
                .to_string(),
            "--resume-from does not support cyclic topologies (FLOWIP-120n F13); cycle \
             members: review, triage"
        );
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
             input unfinished, and their connectors cannot continue interrupted input.\nUse a \
             connector that continues interrupted input, or implement one."
        );
    }

    #[test]
    fn a_seek_source_refuses_without_positions_or_at_another_version() {
        let seek = |version: u32| FlowEvidence {
            sources: vec![(
                "orders".to_string(),
                SourceContinuation::Seek {
                    position_version: NonZeroU32::new(version).unwrap(),
                },
            )],
            ..safe_flow()
        };
        // A never-pinned, never-admitted Seek source starts afresh.
        let empty = archive(
            ArchiveStatus::Cancelled,
            vec![source("orders", true, Some(EofKind::Truncated))],
        );
        assert!(decided(ReplayVerb::Resume, &empty, &seek(1), plain()).is_ok());

        let mut unpositioned = RecordedSourceFrontier::default();
        unpositioned.observe(
            &crate::event::ChainPayload::Fact(serde_json::json!({})),
            None,
        );
        let mut recorded = source("orders", true, Some(EofKind::Truncated));
        recorded.frontier = unpositioned;
        let unpositioned = archive(ArchiveStatus::Cancelled, vec![recorded]);
        assert_eq!(
            decided(ReplayVerb::Resume, &unpositioned, &seek(1), plain()).unwrap_err(),
            ArchiveRefusal::Sources {
                run: PathBuf::from(RUN),
                refusals: vec![(
                    "orders".to_string(),
                    ResumeRefusal::RecordedWithoutPositions
                )],
            }
        );
        // Natural EOF or a reattaching source pass whatever the frontier.
        let natural = archive(
            ArchiveStatus::Cancelled,
            vec![source("orders", true, Some(EofKind::Natural))],
        );
        assert!(decided(ReplayVerb::Resume, &natural, &seek(2), plain()).is_ok());
    }

    #[test]
    fn each_sink_safety_with_and_without_the_override() {
        let stopped = archive(ArchiveStatus::Cancelled, finished());
        for (safety, undeclared) in [
            (Some(SinkRedeliverySafety::DuplicateSensitive), false),
            (None, true),
        ] {
            let flow = FlowEvidence {
                sinks: vec![("ledger".to_string(), safety)],
                ..safe_flow()
            };
            for verb in [ReplayVerb::Replay, ReplayVerb::Resume] {
                assert_eq!(
                    decided(verb, &stopped, &flow, plain()).unwrap_err(),
                    ArchiveRefusal::Sink {
                        verb,
                        stage: "ledger".to_string(),
                        undeclared,
                    }
                );
                let admitted = decided(
                    verb,
                    &stopped,
                    &flow,
                    ArchiveOverrides {
                        allow_incomplete_archive: false,
                        allow_duplicate_sink_delivery: true,
                    },
                )
                .unwrap();
                assert_eq!(
                    admitted.duplicate_sinks,
                    vec![("ledger".to_string(), safety)]
                );
            }
        }
    }

    #[test]
    fn unfinished_source_refusal_names_every_source() {
        let archive = Path::new("/runs/interrupted");
        assert_eq!(
            unfinished_sources_refusal(archive, &["web_orders".to_string()], RefusedBy::Connector),
            "cannot resume /runs/interrupted: source 'web_orders' stopped with its input \
             unfinished, and its connector cannot continue interrupted input.\nUse a connector \
             that continues interrupted input, or implement one."
        );
        assert!(unfinished_sources_refusal(
            archive,
            &["a".to_string(), "b".to_string(), "c".to_string()],
            RefusedBy::Connector
        )
        .starts_with(
            "cannot resume /runs/interrupted: sources 'a', 'b' and 'c' stopped with their \
             input unfinished, and their connectors cannot continue interrupted input."
        ));
    }

    /// FLOWIP-122i: a direct handler declares no continuation, and the
    /// refusal says so.
    #[test]
    fn an_unfinished_direct_handler_is_named_as_a_handler() {
        let archive = Path::new("/runs/interrupted");
        assert_eq!(
            unfinished_sources_refusal(archive, &["local_orders".to_string()], RefusedBy::Handler),
            "cannot resume /runs/interrupted: source 'local_orders' stopped with its input \
             unfinished, and its handler cannot continue interrupted input.\nUse a connector \
             that continues interrupted input, or implement one."
        );
        let refusals = vec![
            (
                "local_orders".to_string(),
                ResumeRefusal::UnfinishedInput {
                    by: RefusedBy::Handler,
                },
            ),
            (
                "cards".to_string(),
                ResumeRefusal::UnfinishedInput {
                    by: RefusedBy::Connector,
                },
            ),
        ];
        assert_eq!(
            sources_refusal(archive, &refusals),
            [
                unfinished_sources_refusal(archive, &["cards".to_string()], RefusedBy::Connector),
                unfinished_sources_refusal(
                    archive,
                    &["local_orders".to_string()],
                    RefusedBy::Handler
                ),
            ]
            .join("\n")
        );
    }

    /// FLOWIP-134i B22: one refusal names every source, the unfinished ones
    /// together and each positioned one with its own reason and remedy.
    #[test]
    fn sources_refusal_names_each_source_with_its_remedy() {
        let archive = Path::new("/runs/interrupted");
        let refusals = vec![
            (
                "cards".to_string(),
                ResumeRefusal::UnfinishedInput {
                    by: RefusedBy::Connector,
                },
            ),
            (
                "ledger".to_string(),
                ResumeRefusal::RecordedWithoutPositions,
            ),
            (
                "orders".to_string(),
                ResumeRefusal::PositionVersion {
                    recorded: NonZeroU32::MIN,
                    expected: NonZeroU32::new(2).unwrap(),
                },
            ),
        ];
        assert_eq!(
            sources_refusal(archive, &refusals),
            [
                unfinished_sources_refusal(archive, &["cards".to_string()], RefusedBy::Connector),
                "cannot resume /runs/interrupted: source 'ledger' recorded its input without \
                 positions, so its connector cannot continue it from where it stopped.\nStart a \
                 new run."
                    .to_string(),
                "cannot resume /runs/interrupted: source 'orders' recorded positions in version 1 \
                 of its connector's format, and the connector now reads version 2.\nResume with \
                 the version that recorded them, or start a new run."
                    .to_string(),
            ]
            .join("\n")
        );
    }

    #[test]
    fn archive_rows_are_checked_before_the_flows() {
        let failed = archive(ArchiveStatus::Failed, finished());
        let everything_wrong = FlowEvidence {
            cycle_members: vec!["triage".to_string()],
            sources: safe_flow().sources,
            sinks: vec![("ledger".to_string(), None)],
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
}
