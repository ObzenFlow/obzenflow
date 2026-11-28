// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::presentation::shell_path;
use crate::journal::disk::replay_archive::{DiskReplayArchive, ResumeEvidence};
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::archive_actions::{
    decide, ArchiveOverrides, ArchiveRefusal, ArchiveWarning, FlowEvidence, ReplayVerb,
};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

/// `ResumeAdvice` contains the admission decision for a plain resume of the
/// observed head of a history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeAdvice {
    pub run: PathBuf,
    pub continued_from: Option<PathBuf>,
    pub admission: Result<Option<ArchiveWarning>, ArchiveRefusal>,
}

impl ResumeAdvice {
    pub(crate) async fn for_replay_refusal(
        requested: &Path,
        opened: &Path,
        flow: &FlowEvidence,
    ) -> Result<Self, ReplayError> {
        let mut target = DiskReplayArchive::resume_evidence(opened).await?;
        if target.run == opened {
            target.run = requested.to_path_buf();
        }
        if target.continued_from.as_deref() == Some(opened) {
            target.continued_from = Some(requested.to_path_buf());
        }
        Ok(Self::judge(&target, flow))
    }

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
                        Ok(_) => write!(
                            f,
                            "Resume {} to finish this run's work.",
                            advice.run.display()
                        ),
                        Err(refusal) => f.write_str(&refusal_text(refusal)),
                    },
                    Some(Err(error)) => f.write_str(&resume_read_error(replayed, error)),
                    None => f.write_str(
                        "ObzenFlow cannot check whether this history can resume without the flow \
                         definition.",
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

pub(crate) fn archive_open_error(error: ReplayError) -> ApplicationError {
    match error {
        ReplayError::AncestorMissing { .. }
        | ReplayError::AncestorUnreadable { .. }
        | ReplayError::AncestorMissingSystemLog { .. }
        | ReplayError::AncestorDisagrees { .. }
        | ReplayError::NamespaceDisagrees { .. }
        | ReplayError::AlreadyContinued { .. }
        | ReplayError::StoppedBeforeTakeover { .. }
        | ReplayError::ContinuedByAnother { .. }
        | ReplayError::LocationNotWritable { .. }
        | ReplayError::ReplayedRunMissing { .. } => {
            ApplicationError::ResumeRefused(replay_error_text(&error))
        }
        other => ApplicationError::InvalidConfiguration(format!(
            "Failed to open replay archive: {}",
            replay_error_text(&other)
        )),
    }
}

impl ApplicationError {
    pub(crate) fn archive_refusal(
        refusal: &ArchiveRefusal,
        resume: Option<&Result<ResumeAdvice, ReplayError>>,
    ) -> Self {
        let message = match (refusal, resume) {
            (
                ArchiveRefusal::Failed { run }
                | ArchiveRefusal::Interrupted { run }
                | ArchiveRefusal::NotStarted { run },
                Some(resume),
            ) => unsealed_replay_text(refusal, run, resume),
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
            format!("{refusal}\nStop the run. Then {} it again.", verb.word())
        }
        ArchiveRefusal::NoWorkLeft { run } => {
            format!(
                "{refusal}\nReplay it with --replay-from {}.",
                shell_path(run)
            )
        }
        ArchiveRefusal::Failed { .. }
        | ArchiveRefusal::Interrupted { .. }
        | ArchiveRefusal::NotStarted { .. } => {
            format!("{refusal}\nTo replay its recorded prefix, add --allow-incomplete-archive.")
        }
        ArchiveRefusal::Sources { .. } => format!("{refusal}\nStart a new run."),
    }
}

fn unsealed_replay_text(
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
            "{refusal}\nResume it with --resume-from {}. To replay its recorded prefix instead, \
             add --allow-incomplete-archive.",
            shell_path(run)
        );
    }
    let prefix = match &advice.continued_from {
        Some(parent) => format!(
            "To replay the recorded prefix of {}, add --allow-incomplete-archive.\n{} continued \
             {}.",
            run.display(),
            advice.run.display(),
            parent.display()
        ),
        None => "To replay its recorded prefix, add --allow-incomplete-archive.".to_string(),
    };
    let next = match &advice.admission {
        Ok(_) => format!("Resume it with --resume-from {}.", shell_path(&advice.run)),
        Err(resume_refusal) => refusal_text(resume_refusal),
    };
    format!("{refusal}\n{prefix}\n{next}")
}

pub(crate) fn replay_error_text(error: &ReplayError) -> String {
    match error {
        ReplayError::UnsupportedJournalSchemaVersion { .. }
        | ReplayError::UnsupportedArchiveCapability { .. } => {
            format!("{error}\nRecord the run again with this build.")
        }
        ReplayError::MissingSystemLog { .. } => format!(
            "{error}\nRestore system.log from a copy of the run directory. To open the run as \
             interrupted from its stage journals instead, add --allow-incomplete-archive."
        ),
        ReplayError::AncestorMissing { archive, ancestor }
        | ReplayError::ReplayedRunMissing {
            replay: archive,
            replayed: ancestor,
        } => format!(
            "{error}\nRestore {}. Then resume {} again.",
            ancestor.display(),
            archive.display()
        ),
        ReplayError::AncestorUnreadable {
            archive,
            ancestor,
            source,
        } => format!(
            "ObzenFlow cannot resume {}. Its reconstruction did not complete, and ObzenFlow \
             cannot read the archive that it resumed from, {}. {}\nRestore {}. Then resume {} \
             again.",
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
            "{error}\nRestore {} from a copy. Then resume {} again. To open {} as interrupted \
             from its stage journals instead, resume {} with --allow-incomplete-archive.",
            system_log.display(),
            archive.display(),
            ancestor.display(),
            archive.display()
        ),
        ReplayError::AncestorDisagrees {
            archive, ancestor, ..
        }
        | ReplayError::NamespaceDisagrees { archive, ancestor } => format!(
            "{error}\nRestore the original {}. Then resume {} again.",
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
            format!(
                "{error}\nMake the run location writable. To start a separate history instead, \
                 copy the run to a different location."
            )
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
            "ObzenFlow cannot read the ancestor {}.\n{}", ancestor.display(), resume_read_error(ancestor, source)
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
        ReplayError::Io { message, source } => format!("ObzenFlow cannot check the resume target for {}. {message}: {source}", run.display()),
        other => format!(
            "ObzenFlow cannot check the resume target for {}. {}",
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
                "ObzenFlow cannot resume /runs/r1, because the run still has a live writer.\nStop \
                 the run. Then resume it again.",
            ),
            (
                ArchiveRefusal::StillRunning {
                    verb: ReplayVerb::Replay,
                    run: run(),
                },
                "ObzenFlow cannot replay /runs/r1, because the run still has a live writer.\nStop \
                 the run. Then replay it again.",
            ),
            (
                ArchiveRefusal::NoWorkLeft { run: run() },
                "ObzenFlow cannot resume /runs/r1, because the run completed naturally and every \
                 source is finite. No work remains.\nReplay it with --replay-from /runs/r1.",
            ),
            (
                ArchiveRefusal::Failed { run: run() },
                "ObzenFlow cannot replay /runs/r1, because the run failed.\nTo replay its \
                 recorded prefix, add --allow-incomplete-archive.",
            ),
            (
                ArchiveRefusal::Interrupted { run: run() },
                "ObzenFlow cannot replay /runs/r1, because the run is interrupted and has no \
                 terminal record.\nTo replay its recorded prefix, add --allow-incomplete-archive.",
            ),
            (
                ArchiveRefusal::NotStarted { run: run() },
                "ObzenFlow cannot replay /runs/r1, because the run did not start.\nTo replay its \
                 recorded prefix, add --allow-incomplete-archive.",
            ),
            (
                ArchiveRefusal::Sources {
                    run: run(),
                    refusals: vec![
                        ("store_orders".to_string(), ResumeRefusal::UnfinishedInput),
                        ("web_orders".to_string(), ResumeRefusal::UnfinishedInput),
                    ],
                },
                "ObzenFlow cannot resume /runs/r1, because sources 'store_orders' and \
                 'web_orders' stopped before their input finished, and only the stopped process \
                 held that input.\nStart a new run.",
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
            "ObzenFlow cannot replay /runs/r1, because the run is interrupted and has no \
             terminal record.\nTo replay its recorded prefix, add --allow-incomplete-archive.\n\
             ObzenFlow cannot resume /runs/r1, because sources 'store_orders' and 'web_orders' \
             stopped before their input finished, and only the stopped process held that input.\n\
             Start a new run."
        );
        assert_eq!(
            interrupted_refusal(advice(
                "/runs/r0",
                None,
                evidence(ArchiveStatus::Completed, finished())
            )),
            "ObzenFlow cannot replay /runs/r1, because the run is interrupted and has no \
             terminal record.\nTo replay its recorded prefix, add --allow-incomplete-archive.\n\
             ObzenFlow cannot resume /runs/r0, because the run completed naturally and every \
             source is finite. No work remains.\nReplay it with --replay-from /runs/r0."
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
            "ObzenFlow cannot replay /runs/r1, because the run is interrupted and has no \
             terminal record.\nResume it with --resume-from /runs/r1. To replay its recorded \
             prefix instead, add --allow-incomplete-archive."
        );
    }

    #[test]
    fn failed_replay_plain_names_resume() {
        let refusal = ArchiveRefusal::Failed {
            run: PathBuf::from("/runs/r1"),
        };
        let resume = advice(
            "/runs/r1",
            None,
            evidence(ArchiveStatus::Failed, finished()),
        );
        assert!(matches!(
            resume.as_ref().map(|advice| &advice.admission),
            Ok(Ok(Some(ArchiveWarning::Failed { .. })))
        ));
        assert_eq!(
            ApplicationError::archive_refusal(&refusal, Some(&resume)).to_string(),
            "ObzenFlow cannot replay /runs/r1, because the run failed.\nResume it with \
             --resume-from /runs/r1. To replay its recorded prefix instead, add \
             --allow-incomplete-archive."
        );
        let refused = advice(
            "/runs/r1",
            None,
            evidence(ArchiveStatus::Failed, unfinished()),
        );
        assert_eq!(
            ApplicationError::archive_refusal(&refusal, Some(&refused)).to_string(),
            "ObzenFlow cannot replay /runs/r1, because the run failed.\nTo replay its recorded \
             prefix, add --allow-incomplete-archive.\nObzenFlow cannot resume /runs/r1, because \
             sources 'store_orders' and 'web_orders' stopped before their input finished, and \
             only the stopped process held that input.\nStart a new run."
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
                "This build of ObzenFlow does not support journal schema version 1. It supports \
                 version 2.\nRecord the run again with this build.",
            ),
            (
                ReplayError::UnsupportedArchiveCapability {
                    capability: "effect_binding_descriptor",
                    found: Some(2),
                    supported: 1,
                },
                "This build of ObzenFlow does not support version Some(2) of the archive \
                 capability 'effect_binding_descriptor'. It supports version 1.\nRecord the run \
                 again with this build.",
            ),
            (
                ReplayError::MissingSystemLog {
                    path: path("/runs/r1/system.log"),
                },
                "The replay archive has no system.log at /runs/r1/system.log.\nRestore \
                 system.log from a copy of the run directory. To open the run as interrupted \
                 from its stage journals instead, add --allow-incomplete-archive.",
            ),
            (
                ReplayError::AncestorMissing {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                },
                "ObzenFlow cannot resume /runs/r1. Its reconstruction did not complete, and the \
                 archive that it resumed from, /runs/r0, is missing.\nRestore /runs/r0. Then \
                 resume /runs/r1 again.",
            ),
            (
                ReplayError::AncestorUnreadable {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                    source: Box::new(schema()),
                },
                "ObzenFlow cannot resume /runs/r1. Its reconstruction did not complete, and \
                 ObzenFlow cannot read the archive that it resumed from, /runs/r0. This build of \
                 ObzenFlow does not support journal schema version 1. It supports version 2.\n\
                 Record the run again with this build.\nRestore /runs/r0. Then resume /runs/r1 \
                 again.",
            ),
            (
                ReplayError::AncestorMissingSystemLog {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                    system_log: path("/runs/r0/system.log"),
                },
                "ObzenFlow cannot resume /runs/r1. Its reconstruction did not complete, and the \
                 archive that it resumed from, /runs/r0, has no system.log.\nRestore \
                 /runs/r0/system.log from a copy. Then resume /runs/r1 again. To open /runs/r0 \
                 as interrupted from its stage journals instead, resume /runs/r1 with \
                 --allow-incomplete-archive.",
            ),
            (
                ReplayError::AncestorDisagrees {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                    stage_key: "orders".to_string(),
                },
                "ObzenFlow cannot resume /runs/r1. Its reconstruction did not complete, and the \
                 archive that it resumed from, /runs/r0, disagrees with it at source 'orders'.\n\
                 Restore the original /runs/r0. Then resume /runs/r1 again.",
            ),
            (
                ReplayError::NamespaceDisagrees {
                    archive: path("/runs/r1"),
                    ancestor: path("/runs/r0"),
                },
                "ObzenFlow cannot resume /runs/r1. Its reconstruction did not complete, and the \
                 archive that it resumed from, /runs/r0, records a different identity \
                 namespace.\nRestore the original /runs/r0. Then resume /runs/r1 again.",
            ),
            (
                ReplayError::AlreadyContinued {
                    archive: path("/runs/r0"),
                    head: path("/runs/r2"),
                },
                "ObzenFlow cannot resume /runs/r0, because /runs/r2 already resumed it. /runs/r2 \
                 is the head of this history.\nResume /runs/r2 instead.",
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
                "ObzenFlow cannot resume /runs/r1, because it stopped before it took over \
                 /runs/r0.\nResume /runs/r0 instead.",
            ),
            (
                ReplayError::ContinuedByAnother {
                    archive: path("/runs/r1"),
                    parent: path("/runs/r0"),
                    head: path("/runs/r2"),
                },
                "ObzenFlow cannot resume /runs/r1, because /runs/r2 continued /runs/r0 and this \
                 run did not.\nResume /runs/r2 instead.",
            ),
            (
                ReplayError::LocationNotWritable {
                    archive: path("/runs/r0"),
                },
                "ObzenFlow cannot resume /runs/r0, because its run location is not writable. The \
                 resume cannot record that it continues the run.\nMake the run location \
                 writable. To start a separate history instead, copy the run to a different \
                 location.",
            ),
            (
                ReplayError::ReplayedRunMissing {
                    replay: path("/runs/r1"),
                    replayed: path("/runs/r0"),
                },
                "ObzenFlow cannot resume /runs/r1, because the run that it replays, /runs/r0, is \
                 missing.\nRestore /runs/r0. Then resume /runs/r1 again.",
            ),
        ] {
            let fact = error.to_string();
            assert!(
                !fact.contains('\n') && !fact.contains("--"),
                "{fact} contains a second line or a flag"
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
            "ObzenFlow cannot replay runs/replay, because the run is interrupted and has no \
             terminal record.\nTo replay its recorded prefix, add --allow-incomplete-archive.\n\
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
             recording at input 3, and a replay never executes an effect.\nObzenFlow cannot \
             resume /runs/r1, because sources 'store_orders' and 'web_orders' stopped before \
             their input finished, and only the stopped process held that input.\nStart a new \
             run."
        );
    }
}
