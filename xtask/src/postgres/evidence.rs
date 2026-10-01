// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Target-level evidence from the PostgreSQL service owner. Cargo's exit status
//! does not identify individual failed assertions; raw output remains diagnostic.

use crate::{error, validation::source::SourceIdentity, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path, process::ExitStatus};

const VERSION: u32 = 1;
pub(crate) const REPORT_FILE: &str = "coordinator.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum State {
    Pending,
    Running,
    Passed,
    Rejected,
    Incomplete,
    NotRequired,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Phase {
    pub(crate) state: State,
    pub(crate) detail: Option<String>,
    pub(crate) exit_code: Option<i32>,
    pub(crate) signal: Option<i32>,
}

impl Phase {
    pub(super) fn new(state: State) -> Self {
        Self {
            state,
            detail: None,
            exit_code: None,
            signal: None,
        }
    }

    pub(super) fn incomplete(failure: impl ToString) -> Self {
        Self {
            detail: Some(failure.to_string()),
            ..Self::new(State::Incomplete)
        }
    }

    pub(super) fn exited(status: ExitStatus, interrupted: bool) -> Self {
        #[cfg(unix)]
        let signal = {
            use std::os::unix::process::ExitStatusExt;
            status.signal()
        };
        #[cfg(not(unix))]
        let signal = None;
        Self {
            state: if signal.is_none() && status.code().is_some_and(|code| code != 0) {
                // A concurrently observed interruption cannot erase an already
                // completed rejected command. Other obligations retain it.
                State::Rejected
            } else if interrupted || signal.is_some() {
                State::Incomplete
            } else {
                State::Passed
            },
            detail: Some(status.to_string()),
            exit_code: status.code(),
            signal,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Target {
    pub(crate) id: String,
    pub(crate) cargo_args: Vec<String>,
    pub(crate) result: Phase,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Report {
    pub(crate) version: u32,
    pub(crate) invocation_id: String,
    pub(crate) source: SourceIdentity,
    pub(crate) final_source: Option<SourceIdentity>,
    pub(crate) setup: Phase,
    pub(crate) preparation: Phase,
    pub(crate) targets: Vec<Target>,
    pub(crate) cleanup: Phase,
    pub(crate) session_run_id: Option<String>,
    pub(crate) finished: bool,
}

impl Report {
    pub(super) fn new(invocation_id: &str, source: SourceIdentity) -> Self {
        Self {
            version: VERSION,
            invocation_id: invocation_id.into(),
            source,
            final_source: None,
            setup: Phase::new(State::Pending),
            preparation: Phase::new(State::Pending),
            targets: super::acceptance::COMMANDS
                .iter()
                .map(|spec| Target {
                    id: spec.id.into(),
                    cargo_args: spec.cargo_args.iter().map(|arg| (*arg).into()).collect(),
                    result: Phase::new(State::Pending),
                })
                .collect(),
            cleanup: Phase::new(State::NotRequired),
            session_run_id: None,
            finished: false,
        }
    }

    pub(super) fn save(&self, directory: &Path) -> Result<()> {
        let pending = directory.join("coordinator.pending.json");
        fs::write(&pending, serde_json::to_vec_pretty(self)?)?;
        fs::rename(pending, directory.join(REPORT_FILE))?;
        Ok(())
    }

    pub(crate) fn known_failures(&self) -> Vec<String> {
        let mut failures = self
            .targets
            .iter()
            .filter(|target| target.result.state == State::Rejected)
            .map(|target| {
                format!(
                    "target command {} failed: {}",
                    target.id,
                    target
                        .result
                        .detail
                        .as_deref()
                        .unwrap_or("unsuccessful exit")
                )
            })
            .collect::<Vec<_>>();
        if self.cleanup.state == State::Rejected {
            failures.push(format!(
                "cleanup failed: {}",
                self.cleanup.detail.as_deref().unwrap_or("unknown error")
            ));
        }
        failures
    }

    pub(crate) fn unfinished_obligations(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for (name, phase) in [("setup", &self.setup), ("preparation", &self.preparation)] {
            if phase.state != State::Passed {
                missing.push(format!(
                    "{name}: {:?}{}",
                    phase.state,
                    phase
                        .detail
                        .as_ref()
                        .map(|s| format!(" ({s})"))
                        .unwrap_or_default()
                ));
            }
        }
        missing.extend(
            self.targets
                .iter()
                .filter(|target| !matches!(target.result.state, State::Passed | State::Rejected))
                .map(|target| format!("target {}: {:?}", target.id, target.result.state)),
        );
        if !matches!(self.cleanup.state, State::Passed | State::NotRequired) {
            missing.push(format!("cleanup: {:?}", self.cleanup.state));
        }
        if !self.finished {
            missing.push("coordinator did not finish".into());
        }
        match &self.final_source {
            Some(source) if self.source.same_contents_as(source) => {}
            Some(_) => {
                missing.push("source changed; results do not certify the final checkout".into())
            }
            None => missing.push("final source identity unavailable".into()),
        }
        missing
    }

    pub(crate) fn passed(&self) -> bool {
        self.known_failures().is_empty() && self.unfinished_obligations().is_empty()
    }
}

pub(crate) fn read_validated(
    directory: &Path,
    invocation_id: &str,
    source_sha256: &str,
) -> Result<Report> {
    let report: Report = serde_json::from_slice(&fs::read(directory.join(REPORT_FILE))?)?;
    if report.version != VERSION
        || report.invocation_id != invocation_id
        || report.source.content_sha256 != source_sha256
    {
        return Err(error(
            "PostgreSQL coordinator evidence has the wrong version, invocation or source",
        ));
    }
    let expected = super::acceptance::COMMANDS;
    if report.targets.len() != expected.len()
        || report.targets.iter().zip(expected).any(|(actual, spec)| {
            actual.id != spec.id
                || actual
                    .cargo_args
                    .iter()
                    .map(String::as_str)
                    .ne(spec.cargo_args.iter().copied())
        })
    {
        return Err(error(
            "PostgreSQL coordinator required-target inventory differs from the declared commands",
        ));
    }
    if report.setup.state == State::NotRequired
        || report.preparation.state == State::NotRequired
        || report.setup.exit_code.is_some()
        || report.setup.signal.is_some()
        || report.cleanup.exit_code.is_some()
        || report.cleanup.signal.is_some()
        || (report.session_run_id.is_some() == (report.cleanup.state == State::NotRequired))
        || (report.setup.state == State::Passed && report.session_run_id.is_none())
        || (report.preparation.state == State::Passed
            && (report.preparation.exit_code != Some(0) || report.preparation.signal.is_some()))
        || (report
            .targets
            .iter()
            .any(|target| target.result.state != State::Pending)
            && report.setup.state != State::Passed)
    {
        return Err(error(
            "PostgreSQL coordinator has contradictory setup, preparation or cleanup evidence",
        ));
    }
    for target in &report.targets {
        let result = &target.result;
        let valid = match result.state {
            State::Passed => result.exit_code == Some(0) && result.signal.is_none(),
            State::Rejected => {
                result.exit_code.is_some_and(|code| code != 0) && result.signal.is_none()
            }
            State::Pending | State::Running => {
                result.exit_code.is_none() && result.signal.is_none()
            }
            State::Incomplete => true,
            State::NotRequired => false,
        };
        if !valid {
            return Err(error(format!(
                "PostgreSQL target {} has contradictory evidence",
                target.id
            )));
        }
    }
    Ok(report)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    fn source() -> SourceIdentity {
        SourceIdentity {
            commit: "commit".into(),
            content_sha256: "source".into(),
            files: 1,
        }
    }

    fn complete() -> Report {
        let mut report = Report::new("invocation", source());
        report.setup = Phase::new(State::Passed);
        report.preparation = Phase::exited(ExitStatus::from_raw(0), false);
        report.session_run_id = Some("owned-session".into());
        report.cleanup = Phase::new(State::Passed);
        for target in &mut report.targets {
            target.result = Phase::exited(ExitStatus::from_raw(0), false);
        }
        report.final_source = Some(source());
        report.finished = true;
        report
    }

    #[test]
    fn delegated_evidence_preserves_failure_scope_and_rejects_wrong_identity() {
        let directory = tempfile::tempdir().unwrap();
        let read = || read_validated(directory.path(), "invocation", "source");
        let consume = |execution| crate::validation::postgres_acceptance(execution, read());
        assert!(
            consume(Ok(ExitStatus::from_raw(0))).is_err(),
            "missing report"
        );
        let mut report = complete();
        report.save(directory.path()).unwrap();
        assert!(consume(Ok(ExitStatus::from_raw(0))).is_ok());
        assert!(
            consume(Ok(ExitStatus::from_raw(256))).is_err(),
            "nonzero process wins over passing report"
        );
        assert!(read_validated(directory.path(), "other", "source").is_err());
        assert!(read_validated(directory.path(), "invocation", "other").is_err());

        // A late interruption cannot erase a completed rejected command.
        report.targets[0].result = Phase::exited(ExitStatus::from_raw(101 << 8), true);
        report.targets[1].result = Phase::new(State::Pending);
        report.finished = false;
        report.save(directory.path()).unwrap();
        let loaded = read().unwrap();
        assert_eq!(loaded.known_failures().len(), 1);
        assert!(loaded
            .unfinished_obligations()
            .iter()
            .any(|item| item.contains("postgres-lifecycle")));
        let failure = consume(Err(error("interrupted"))).unwrap_err().to_string();
        assert!(failure.contains("xtask-unit-conformance"), "{failure}");
        assert!(failure.contains("postgres-lifecycle"), "{failure}");
        assert!(
            consume(Ok(ExitStatus::from_raw(0))).is_err(),
            "zero exit cannot erase retained failure"
        );

        report = complete();
        report.preparation = Phase::incomplete("fetch unavailable");
        report.save(directory.path()).unwrap();
        assert!(
            consume(Ok(ExitStatus::from_raw(0))).is_err(),
            "later tests cannot certify failed preparation"
        );
        report = complete();
        report.cleanup = Phase::new(State::Rejected);
        report.save(directory.path()).unwrap();
        assert_eq!(read().unwrap().known_failures().len(), 1);
        assert!(consume(Ok(ExitStatus::from_raw(0))).is_err());
        report.cleanup = Phase::new(State::NotRequired);
        report.save(directory.path()).unwrap();
        assert!(read().is_err(), "owned session requires cleanup evidence");

        report = complete();
        report.final_source.as_mut().unwrap().content_sha256 = "changed".into();
        report.save(directory.path()).unwrap();
        assert!(consume(Ok(ExitStatus::from_raw(0))).is_err());
        report = complete();
        report.targets.pop();
        report.save(directory.path()).unwrap();
        assert!(read().is_err(), "required inventory cannot shrink");
        fs::write(directory.path().join(REPORT_FILE), b"{truncated").unwrap();
        assert!(consume(Ok(ExitStatus::from_raw(0))).is_err());
    }
}
