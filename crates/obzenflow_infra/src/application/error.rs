// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::archive_actions::{
    shell_path, ArchiveRefusal, ReplayVerb, ResumeAdvice,
};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

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
                        Err(refusal) => write!(f, "{refusal}"),
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
        read_error: Option<&ReplayError>,
    ) -> Self {
        if let (ArchiveRefusal::Interrupted { run, resume: None }, Some(error)) =
            (refusal, read_error)
        {
            return Self::ReplayRefused(format!("{refusal}\n{}", resume_read_error(run, error)));
        }
        let message = match refusal {
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
            other => other.to_string(),
        };
        match refusal.verb() {
            ReplayVerb::Replay => Self::ReplayRefused(message),
            ReplayVerb::Resume => Self::ResumeRefused(message),
        }
    }
}

fn resume_read_error(run: &Path, error: &ReplayError) -> String {
    match error {
        ReplayError::ContinuationMissing { .. } => error.to_string(),
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
        other => format!("Cannot check the resume target for {}. {other}", run.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let refusal = ArchiveRefusal::Interrupted { run, resume: None };
        assert_eq!(
            ApplicationError::archive_refusal(&refusal, Some(&error)).to_string(),
            "cannot replay runs/replay: it has no terminal record, so it was interrupted.\n\
             Pass --allow-incomplete-archive to replay its recorded prefix.\n\
             runs/replay replays runs/original, but that directory is missing.\n\
             Restore runs/original to check whether this history can resume."
        );
    }

    #[test]
    fn replay_stop_refused_resume_prints_refusal() {
        use obzenflow_core::source::ResumeRefusal;
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
