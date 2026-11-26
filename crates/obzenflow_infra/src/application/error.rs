// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::presentation::shell_path;
use crate::journal::disk::replay_archive::ResumeEvidence;
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::archive_actions::{
    decide, ArchiveOverrides, ArchiveRefusal, FlowEvidence, ReplayVerb,
};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

/// `ResumeAdvice` holds the admission decision for a plain resume of the
/// observed head of a history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeAdvice {
    pub run: PathBuf,
    pub continued_from: Option<PathBuf>,
    pub admission: Result<(), ArchiveRefusal>,
}

impl ResumeAdvice {
    pub(crate) fn judge(target: &ResumeEvidence, flow: &FlowEvidence) -> Self {
        Self {
            run: target.run.clone(),
            continued_from: target.continued_from.clone(),
            admission: decide(
                ReplayVerb::Resume,
                &target.run,
                &target.archive,
                flow,
                ArchiveOverrides::default(),
            ),
        }
    }
}

/// Errors that can occur in the FlowApplication framework
#[derive(Debug)]
pub enum ApplicationError {
    /// Flow failed to build
    FlowBuildFailed(String),

    /// Flow execution failed
    FlowExecutionFailed(String),

    /// Server failed to start
    ServerStartFailed(String),

    /// Runtime creation failed
    RuntimeCreationFailed(String),

    /// Feature not enabled
    FeatureNotEnabled(String),

    /// Invalid runtime configuration
    InvalidConfiguration(String),

    /// IO error
    IoError(std::io::Error),

    /// Replay verification finished with a non-zero verdict (FLOWIP-095j):
    /// 1 divergence in the certified region, 2 certified region matched with
    /// uncertified stages, 3 refused or skipped.
    Verification { exit_code: u8, summary: String },

    /// A replay reached a cursor its recording holds no outcome for, as its
    /// stage-fatal fact records (FLOWIP-134i B13).
    ReplayRecordingEnded {
        replayed: PathBuf,
        stage_key: String,
        input_position: Option<u64>,
        resume: Option<Box<Result<ResumeAdvice, ReplayError>>>,
    },

    /// A resume refused before any work, naming the run to act on
    /// (FLOWIP-134i B6, B7, B8, B12, B13).
    ResumeRefused(String),

    /// A replay refused before any work, printed verbatim.
    ReplayRefused(String),

    /// Other error
    Other(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FlowBuildFailed(msg) => write!(f, "Flow build failed: {msg}"),
            Self::FlowExecutionFailed(msg) => write!(f, "Flow execution failed: {msg}"),
            Self::ServerStartFailed(msg) => write!(f, "Server start failed: {msg}"),
            Self::RuntimeCreationFailed(msg) => write!(f, "Runtime creation failed: {msg}"),
            Self::FeatureNotEnabled(feature) => write!(f, "Feature not enabled: {feature}"),
            Self::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {msg}"),
            Self::IoError(err) => write!(f, "IO error: {err}"),
            Self::Verification { summary, .. } => write!(f, "replay verification: {summary}"),
            Self::ReplayRecordingEnded {
                replayed,
                stage_key,
                input_position,
                resume,
            } => {
                write!(
                    f,
                    "replay of {} stopped: stage '{stage_key}' reached the end of its recording",
                    replayed.display()
                )?;
                if let Some(input) = input_position {
                    write!(f, " at input {input}")?;
                }
                f.write_str(", and a replay never executes an effect.\n")?;
                match resume.as_deref() {
                    Some(Ok(advice)) => match &advice.admission {
                        Ok(()) => write!(
                            f,
                            "Resume {} to finish this run's work.",
                            advice.run.display()
                        ),
                        Err(refusal) => f.write_str(&refusal_text(refusal)),
                    },
                    Some(Err(error)) => f.write_str(&resume_read_error(replayed, error)),
                    None => f.write_str(
                        "Cannot check whether this history can resume without the flow definition.",
                    ),
                }
            }
            Self::ResumeRefused(refusal) | Self::ReplayRefused(refusal) => f.write_str(refusal),
            Self::Other(err) => write!(f, "Application error: {err}"),
        }
    }
}

impl Error for ApplicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IoError(error) => Some(error),
            Self::Other(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl ApplicationError {
    /// Map to a process exit code. Verification verdicts carry their
    /// FLOWIP-095j contract code (1 divergence, 2 uncertified remainder,
    /// 3 refused/skipped); every other error is 1.
    pub fn process_exit_code(&self) -> u8 {
        match self {
            Self::Verification { exit_code, .. } => *exit_code,
            _ => 1,
        }
    }
}

impl From<std::io::Error> for ApplicationError {
    fn from(err: std::io::Error) -> Self {
        ApplicationError::IoError(err)
    }
}

impl From<&ArchiveRefusal> for ApplicationError {
    fn from(refusal: &ArchiveRefusal) -> Self {
        Self::archive_refusal(refusal, None)
    }
}

impl ApplicationError {
    pub(super) fn archive_refusal(
        refusal: &ArchiveRefusal,
        resume: Option<&Result<ResumeAdvice, ReplayError>>,
    ) -> Self {
        let message = match (refusal, resume) {
            (ArchiveRefusal::Interrupted { run }, Some(resume)) => {
                interrupted_text(refusal, run, resume)
            }
            _ => refusal_text(refusal),
        };
        match refusal.verb() {
            ReplayVerb::Replay => Self::ReplayRefused(message),
            ReplayVerb::Resume => Self::ResumeRefused(message),
        }
    }
}

fn refusal_text(refusal: &ArchiveRefusal) -> String {
    match refusal {
        ArchiveRefusal::Cycle {
            verb: ReplayVerb::Replay,
            members,
            run,
        } => format!(
            "--replay-from does not support cyclic topologies; cycle members: {}\nTo read \
             this run without replaying it, run: obzenflow show {}",
            members.join(", "),
            shell_path(run)
        ),
        ArchiveRefusal::Cycle {
            verb: ReplayVerb::Resume,
            members,
            ..
        } => format!(
            "--resume-from does not support cyclic topologies (FLOWIP-120n F13); cycle \
             members: {}",
            members.join(", ")
        ),
        ArchiveRefusal::StillRunning { verb, .. } => {
            format!("{refusal}\nStop it, then {} it again.", verb.word())
        }
        ArchiveRefusal::NoWorkLeft { run } => {
            format!(
                "{refusal}\nReplay it with --replay-from {}.",
                shell_path(run)
            )
        }
        ArchiveRefusal::Failed { verb, .. } => format!(
            "{refusal}\nPass --allow-incomplete-archive to {} it anyway.",
            verb.word()
        ),
        ArchiveRefusal::Interrupted { .. } => {
            format!("{refusal}\nPass --allow-incomplete-archive to replay its recorded prefix.")
        }
        ArchiveRefusal::Sources { .. } => format!("{refusal}\nStart a new run."),
    }
}

fn interrupted_text(
    refusal: &ArchiveRefusal,
    run: &Path,
    resume: &Result<ResumeAdvice, ReplayError>,
) -> String {
    let advice = match resume {
        Ok(advice) => advice,
        Err(error) => {
            return format!(
                "{}\n{}",
                refusal_text(refusal),
                resume_read_error(run, error)
            )
        }
    };
    if advice.admission.is_ok() && advice.run == run {
        return format!(
            "{refusal}\nResume it with --resume-from {}, or pass --allow-incomplete-archive to \
             replay its recorded prefix.",
            shell_path(run)
        );
    }
    let prefix = match &advice.continued_from {
        Some(parent) => format!(
            "Pass --allow-incomplete-archive to replay the recorded prefix of {}.\n{} continued \
             {}.",
            run.display(),
            advice.run.display(),
            parent.display()
        ),
        None => "Pass --allow-incomplete-archive to replay its recorded prefix.".to_string(),
    };
    let next = match &advice.admission {
        Ok(()) => format!("Resume it with --resume-from {}.", shell_path(&advice.run)),
        Err(resume_refusal) => refusal_text(resume_refusal),
    };
    format!("{refusal}\n{prefix}\n{next}")
}

pub(super) fn replay_error_text(error: &ReplayError) -> String {
    match error {
        ReplayError::UnsupportedJournalSchemaVersion { .. }
        | ReplayError::UnsupportedArchiveCapability { .. } => {
            format!("{error}; re-record the run with this build of ObzenFlow")
        }
        ReplayError::MissingSystemLog { .. } => format!(
            "{error}.\nRestore it from a copy of the run directory, or pass \
             --allow-incomplete-archive to open the run as interrupted from its stage journals."
        ),
        ReplayError::AncestorMissing { archive, ancestor }
        | ReplayError::ReplayedRunMissing {
            replay: archive,
            replayed: ancestor,
        } => format!(
            "{error}\nRestore {}, then resume {} again.",
            ancestor.display(),
            archive.display()
        ),
        ReplayError::AncestorUnreadable {
            archive,
            ancestor,
            source,
        } => format!(
            "cannot resume {}: its reconstruction did not complete, and the archive it resumed \
             from, {}, cannot be read: {}.\nRestore {}, then resume {} again.",
            archive.display(),
            ancestor.display(),
            replay_error_text(source),
            ancestor.display(),
            archive.display()
        ),
        ReplayError::AncestorMissingSystemLog {
            archive,
            ancestor,
            system_log,
        } => format!(
            "{error}\nRestore {} from a copy, then resume {} again, or resume it with \
             --allow-incomplete-archive to open {} as interrupted from its stage journals.",
            system_log.display(),
            archive.display(),
            ancestor.display()
        ),
        ReplayError::AncestorDisagrees {
            archive, ancestor, ..
        }
        | ReplayError::NamespaceDisagrees { archive, ancestor } => format!(
            "{error}\nRestore {} as it was recorded, then resume {} again.",
            ancestor.display(),
            archive.display()
        ),
        ReplayError::AlreadyContinued { head, .. }
        | ReplayError::ContinuedByAnother { head, .. }
        | ReplayError::StoppedBeforeTakeover { parent: head, .. } => {
            format!("{error}\nResume {} instead.", head.display())
        }
        ReplayError::ContinuationMissing { successor, .. } => format!(
            "{error}\nRestore {} to check whether this history can resume.",
            successor.display()
        ),
        ReplayError::LocationNotWritable { .. } => {
            format!("{error}\nMake it writable, or copy it elsewhere to start a separate history.")
        }
        other => other.to_string(),
    }
}

fn resume_read_error(run: &Path, error: &ReplayError) -> String {
    match error {
        ReplayError::ContinuationMissing { .. } => replay_error_text(error),
        ReplayError::ReplayedRunMissing { replay, replayed } => format!(
            "{} replays {}, but that directory is missing.\nRestore {} to check whether this history can resume.",
            replay.display(), replayed.display(), replayed.display()
        ),
        ReplayError::MissingSystemLog { path }
        | ReplayError::AncestorMissingSystemLog { system_log: path, .. } => format!(
            "The system journal {} is missing.\nRestore it to check whether this history can resume.",
            path.display()
        ),
        ReplayError::AncestorMissing { ancestor, .. } => format!(
            "The ancestor {} is missing.\nRestore it to check whether this history can resume.", ancestor.display()
        ),
        ReplayError::AncestorUnreadable { ancestor, source, .. } => format!(
            "Cannot read the ancestor {}.\n{}", ancestor.display(), resume_read_error(ancestor, source)
        ),
        ReplayError::AncestorDisagrees { archive, ancestor, stage_key } => format!(
            "Source '{stage_key}' in {} disagrees with {}.\nRestore the ancestor to check whether this history can resume.",
            archive.display(), ancestor.display()
        ),
        ReplayError::NamespaceDisagrees { archive, ancestor } => format!(
            "{archive} and its ancestor {ancestor} record different identity namespaces.\n\
             Restore {ancestor} to check whether this history can resume.",
            archive = archive.display(), ancestor = ancestor.display()
        ),
        ReplayError::Io { message, source } => format!("Cannot check the resume target for {}. {message}: {source}", run.display()),
        other => format!(
            "Cannot check the resume target for {}. {}",
            run.display(),
            replay_error_text(other)
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::flow_control_payload::EofKind;
    use obzenflow_core::journal::archive_actions::{
        ArchiveEvidence, FailureEvidence, SourceEvidence,
    };
    use obzenflow_core::journal::ArchiveStatus;
    use obzenflow_core::source::{ResumeRefusal, SourceContinuation};

    fn source(stage_key: &str, recorded_eof: Option<EofKind>) -> SourceEvidence {
        SourceEvidence {
            stage_key: stage_key.to_string(),
            finite: true,
            recorded_eof,
        }
    }

    fn evidence(status: ArchiveStatus, sources: Vec<SourceEvidence>) -> ArchiveEvidence {
        ArchiveEvidence {
            status,
            live_writer: false,
            sources,
            failure: FailureEvidence::Other,
        }
    }

    fn finished() -> Vec<SourceEvidence> {
        vec![
            source("store_orders", Some(EofKind::Natural)),
            source("web_orders", Some(EofKind::Natural)),
        ]
    }

    fn unfinished() -> Vec<SourceEvidence> {
        vec![
            source("store_orders", Some(EofKind::Truncated)),
            source("web_orders", None),
        ]
    }

    fn advice(
        run: &str,
        continued_from: Option<&str>,
        archive: ArchiveEvidence,
    ) -> Result<ResumeAdvice, ReplayError> {
        let flow = FlowEvidence {
            cycle_members: Vec::new(),
            sources: vec![
                ("store_orders".to_string(), SourceContinuation::Refuse),
                ("web_orders".to_string(), SourceContinuation::Refuse),
            ],
        };
        Ok(ResumeAdvice::judge(
            &ResumeEvidence {
                run: PathBuf::from(run),
                continued_from: continued_from.map(PathBuf::from),
                archive,
            },
            &flow,
        ))
    }

    fn interrupted_refusal(resume: Result<ResumeAdvice, ReplayError>) -> String {
        let refusal = ArchiveRefusal::Interrupted {
            run: PathBuf::from("/runs/r1"),
        };
        ApplicationError::archive_refusal(&refusal, Some(&resume)).to_string()
    }

    #[test]
    fn archive_refusal_text_appends_remedy() {
        let run = || PathBuf::from("/runs/r1");
        for (refusal, expected) in [
            (
                ArchiveRefusal::StillRunning {
                    verb: ReplayVerb::Resume,
                    run: run(),
                },
                "cannot resume /runs/r1: it is still running.\nStop it, then resume it again.",
            ),
            (
                ArchiveRefusal::StillRunning {
                    verb: ReplayVerb::Replay,
                    run: run(),
                },
                "cannot replay /runs/r1: it is still running.\nStop it, then replay it again.",
            ),
            (
                ArchiveRefusal::NoWorkLeft { run: run() },
                "cannot resume /runs/r1: it completed naturally and every source is finite, so \
                 there is no work left to continue.\nReplay it with --replay-from /runs/r1.",
            ),
            (
                ArchiveRefusal::Failed {
                    verb: ReplayVerb::Resume,
                    run: run(),
                },
                "cannot resume /runs/r1: it failed.\nPass --allow-incomplete-archive to resume \
                 it anyway.",
            ),
            (
                ArchiveRefusal::Failed {
                    verb: ReplayVerb::Replay,
                    run: run(),
                },
                "cannot replay /runs/r1: it failed.\nPass --allow-incomplete-archive to replay \
                 it anyway.",
            ),
            (
                ArchiveRefusal::Interrupted { run: run() },
                "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nPass \
                 --allow-incomplete-archive to replay its recorded prefix.",
            ),
            (
                ArchiveRefusal::Sources {
                    run: run(),
                    refusals: vec![
                        ("store_orders".to_string(), ResumeRefusal::UnfinishedInput),
                        ("web_orders".to_string(), ResumeRefusal::UnfinishedInput),
                    ],
                },
                "cannot resume /runs/r1: sources 'store_orders' and 'web_orders' stopped with \
                 their input unfinished, and only the stopped process held that input.\nStart a \
                 new run.",
            ),
        ] {
            assert_eq!(ApplicationError::from(&refusal).to_string(), expected);
        }
    }

    #[test]
    fn interrupted_replay_refused_resume_prints_refusal() {
        assert_eq!(
            interrupted_refusal(advice(
                "/runs/r1",
                None,
                evidence(ArchiveStatus::Unknown, unfinished())
            )),
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nPass \
             --allow-incomplete-archive to replay its recorded prefix.\ncannot resume /runs/r1: \
             sources 'store_orders' and 'web_orders' stopped with their input unfinished, and \
             only the stopped process held that input.\nStart a new run."
        );
        assert_eq!(
            interrupted_refusal(advice(
                "/runs/r0",
                None,
                evidence(ArchiveStatus::Completed, finished())
            )),
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nPass \
             --allow-incomplete-archive to replay its recorded prefix.\ncannot resume /runs/r0: \
             it completed naturally and every source is finite, so there is no work left to \
             continue.\nReplay it with --replay-from /runs/r0."
        );
    }

    #[test]
    fn interrupted_replay_admitted_resume_named() {
        assert_eq!(
            interrupted_refusal(advice(
                "/runs/r1",
                None,
                evidence(ArchiveStatus::Unknown, finished())
            )),
            "cannot replay /runs/r1: it has no terminal record, so it was interrupted.\nResume \
             it with --resume-from /runs/r1, or pass --allow-incomplete-archive to replay its \
             recorded prefix."
        );
    }

    #[test]
    fn replay_claimed_head_names_resume() {
        let refusal = interrupted_refusal(advice(
            "/runs/Pat's Orders/head",
            Some("/runs/r1"),
            evidence(ArchiveStatus::Unknown, finished()),
        ));
        assert!(
            refusal.ends_with(
                "/runs/Pat's Orders/head continued /runs/r1.\nResume it with --resume-from \
                 '/runs/Pat'\\''s Orders/head'."
            ),
            "{refusal}"
        );
    }

    #[test]
    fn replay_error_text_appends_remedy() {
        let path = |run: &str| PathBuf::from(run);
        let schema = || ReplayError::UnsupportedJournalSchemaVersion {
            journal_schema_version: "1".to_string(),
            supported: "2",
        };
        for (error, expected) in [
            (
                schema(),
                "unsupported journal schema version: 1 (supported: 2); re-record the run with \
                 this build of ObzenFlow",
            ),
            (
                ReplayError::UnsupportedArchiveCapability {
                    capability: "effect_binding_descriptor",
                    found: Some(2),
                    supported: 1,
                },
                "Replay archive capability 'effect_binding_descriptor' has unsupported version \
                 Some(2) (supported: 1); re-record the run with this build of ObzenFlow",
            ),
            (
                ReplayError::MissingSystemLog {
                    path: path("/runs/r1/system.log"),
                },
                "Replay archive system.log missing at /runs/r1/system.log.\nRestore it from a \
                 copy of the run directory, or pass --allow-incomplete-archive to open the run as \
                 interrupted from its stage journals.",
            ),
            (
                ReplayError::AncestorMissing {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                },
                "cannot resume /runs/r1: its reconstruction did not complete, and the archive it \
                 resumed from, /runs/r0, is missing.\nRestore /runs/r0, then resume /runs/r1 again.",
            ),
            (
                ReplayError::AncestorUnreadable {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                    source: Box::new(schema()),
                },
                "cannot resume /runs/r1: its reconstruction did not complete, and the archive it \
                 resumed from, /runs/r0, cannot be read: unsupported journal schema version: 1 \
                 (supported: 2); re-record the run with this build of ObzenFlow.\nRestore \
                 /runs/r0, then resume /runs/r1 again.",
            ),
            (
                ReplayError::AncestorMissingSystemLog {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                    system_log: path("/runs/r0/system.log"),
                },
                "cannot resume /runs/r1: its reconstruction did not complete, and the archive it \
                 resumed from, /runs/r0, has no system.log.\nRestore /runs/r0/system.log from a \
                 copy, then resume /runs/r1 again, or resume it with --allow-incomplete-archive \
                 to open /runs/r0 as interrupted from its stage journals.",
            ),
            (
                ReplayError::AncestorDisagrees {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                    stage_key: "orders".to_string(),
                },
                "cannot resume /runs/r1: its reconstruction did not complete, and the archive it \
                 resumed from, /runs/r0, disagrees with it at source 'orders'.\nRestore /runs/r0 \
                 as it was recorded, then resume /runs/r1 again.",
            ),
            (
                ReplayError::NamespaceDisagrees {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                },
                "cannot resume /runs/r1: its reconstruction did not complete, and the archive it \
                 resumed from, /runs/r0, records a different identity namespace.\nRestore \
                 /runs/r0 as it was recorded, then resume /runs/r1 again.",
            ),
            (
                ReplayError::AlreadyContinued {
                    archive: path("/runs/r0"),
                    head: path("/runs/r2"),
                },
                "cannot resume /runs/r0: it was already resumed by /runs/r2, the head of this \
                 history.\nResume /runs/r2 instead.",
            ),
            (
                ReplayError::ContinuationMissing {
                    parent: path("/runs/r0"),
                    successor: path("/runs/r1"),
                },
                "The continuation claim in /runs/r0 names /runs/r1, but the directory /runs/r1 \
                 is missing.\nRestore /runs/r1 to check whether this history can resume.",
            ),
            (
                ReplayError::StoppedBeforeTakeover {
                    archive: path("/runs/r1"),
                    parent: path("/runs/r0"),
                },
                "cannot resume /runs/r1: it stopped before taking over /runs/r0.\nResume \
                 /runs/r0 instead.",
            ),
            (
                ReplayError::ContinuedByAnother {
                    archive: path("/runs/r1"),
                    parent: path("/runs/r0"),
                    head: path("/runs/r2"),
                },
                "cannot resume /runs/r1: /runs/r0 was continued by /runs/r2, not by this \
                 run.\nResume /runs/r2 instead.",
            ),
            (
                ReplayError::LocationNotWritable {
                    archive: path("/runs/r0"),
                },
                "cannot resume /runs/r0: its run location is not writable, so this resume cannot \
                 record that it continues it.\nMake it writable, or copy it elsewhere to start a \
                 separate history.",
            ),
            (
                ReplayError::ReplayedRunMissing {
                    replay: path("/runs/r1"),
                    replayed: path("/runs/r0"),
                },
                "cannot resume /runs/r1: the run it replays, /runs/r0, is missing.\nRestore \
                 /runs/r0, then resume /runs/r1 again.",
            ),
        ] {
            let fact = error.to_string();
            assert!(
                !fact.contains('\n') && !fact.contains("--"),
                "Core states the fact without a remedy: {fact}"
            );
            assert_eq!(replay_error_text(&error), expected);
        }
    }

    #[test]
    fn cycle_refusal_renders_requested_verb() {
        for (verb, expected) in [
            (
                ReplayVerb::Replay,
                "--replay-from does not support cyclic topologies; cycle members: review, triage\n\
             To read this run without replaying it, run: obzenflow show /runs/r1",
            ),
            (
                ReplayVerb::Resume,
                "--resume-from does not support cyclic topologies (FLOWIP-120n F13); cycle \
             members: review, triage",
            ),
        ] {
            let refusal = ArchiveRefusal::Cycle {
                verb,
                run: PathBuf::from("/runs/r1"),
                members: vec!["review".into(), "triage".into()],
            };
            assert_eq!(ApplicationError::from(&refusal).to_string(), expected);
        }
    }

    #[test]
    fn verification_display_omits_exit_code() {
        let error = ApplicationError::Verification {
            exit_code: 3,
            summary: "verification refused: candidate replayed with --allow-incomplete-archive"
                .to_string(),
        };
        assert_eq!(
            error.to_string(),
            "replay verification: verification refused: candidate replayed with \
             --allow-incomplete-archive"
        );
        assert_eq!(error.process_exit_code(), 3);
    }

    #[test]
    fn replay_missing_original_names_directory() {
        let run = PathBuf::from("runs/replay");
        let error = ReplayError::ReplayedRunMissing {
            replay: run.clone(),
            replayed: PathBuf::from("runs/original"),
        };
        let refusal = ArchiveRefusal::Interrupted { run };
        assert_eq!(
            ApplicationError::archive_refusal(&refusal, Some(&Err(error))).to_string(),
            "cannot replay runs/replay: it has no terminal record, so it was interrupted.\n\
             Pass --allow-incomplete-archive to replay its recorded prefix.\n\
             runs/replay replays runs/original, but that directory is missing.\n\
             Restore runs/original to check whether this history can resume."
        );
    }

    #[test]
    fn replay_stop_refused_resume_prints_refusal() {
        let error = ApplicationError::ReplayRecordingEnded {
            replayed: PathBuf::from("runs/r1"),
            stage_key: "authorize_payment".to_string(),
            input_position: Some(3),
            resume: Some(Box::new(Ok(ResumeAdvice {
                run: PathBuf::from("/runs/r1"),
                continued_from: None,
                admission: Err(ArchiveRefusal::Sources {
                    run: PathBuf::from("/runs/r1"),
                    refusals: vec![
                        ("store_orders".to_string(), ResumeRefusal::UnfinishedInput),
                        ("web_orders".to_string(), ResumeRefusal::UnfinishedInput),
                    ],
                }),
            }))),
        };
        assert_eq!(
            error.to_string(),
            "replay of runs/r1 stopped: stage 'authorize_payment' reached the end of its \
             recording at input 3, and a replay never executes an effect.\ncannot resume \
             /runs/r1: sources 'store_orders' and 'web_orders' stopped with their input \
             unfinished, and only the stopped process held that input.\nStart a new run."
        );
    }
}
