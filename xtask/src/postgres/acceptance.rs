// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    compose::{Compose, ServiceEvidence},
    config::{SESSION_ROOT, STATE_FILE},
    credentials, environment,
    evidence::{Phase, Report, State},
    fixtures,
    state::{self, SessionState, TestIdentity},
    tls,
};
use crate::{error, Result};
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

pub(super) struct CommandSpec {
    pub(super) id: &'static str,
    label: &'static str,
    pub(super) cargo_args: &'static [&'static str],
}

// Test-target granularity is intentional. Individual test functions remain
// owned by their Rust test binaries and can be added or renamed independently.
pub(super) const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        id: "xtask-unit-conformance",
        label: "xtask unit conformance",
        cargo_args: &["test", "--locked", "-p", "xtask", "--bin", "xtask"],
    },
    CommandSpec {
        id: "postgres-lifecycle",
        label: "xtask PostgreSQL lifecycle test",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "xtask",
            "--test",
            "postgres_lifecycle",
            "--",
            "--ignored",
            "--nocapture",
        ],
    },
    CommandSpec {
        id: "dsl-sink-composition",
        label: "DSL sink composition tests",
        cargo_args: &["test", "--locked", "-p", "obzenflow_dsl", "--lib"],
    },
    CommandSpec {
        id: "postgres-adapter-unit",
        label: "PostgreSQL adapter unit tests",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--lib",
        ],
    },
    CommandSpec {
        id: "postgres-real-driver",
        label: "PostgreSQL real-driver tests",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_driver_test",
        ],
    },
    CommandSpec {
        id: "postgres-writer-conformance",
        label: "PostgreSQL writer conformance",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_conformance_test",
        ],
    },
    CommandSpec {
        id: "postgres-application-conformance",
        label: "PostgreSQL application conformance",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_application_conformance_test",
        ],
    },
    CommandSpec {
        id: "payments-application",
        label: "payments application black-box test",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres,e2e",
            "--test",
            "postgres_payments_e2e_test",
        ],
    },
    CommandSpec {
        id: "hn-digest-example-build",
        label: "production-feature HN digest example build",
        cargo_args: &[
            "build",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "http-pull,ai,postgres",
            "--example",
            "hn_ai_digest_demo",
        ],
    },
    CommandSpec {
        id: "hn-digest-postgres-treatment",
        label: "HN digest PostgreSQL treatment",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "http-pull,ai,postgres,test-support,e2e",
            "--test",
            "hn_ai_digest_effect_replay_journal_test",
            "postgres_output_inserts_one_deterministic_hn_digest_with_stable_receipt",
            "--",
            "--exact",
        ],
    },
    CommandSpec {
        id: "inventory-consumer",
        label: "independent inventory consumer test",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres,e2e",
            "--test",
            "postgres_sink_inventory_consumer_test",
        ],
    },
    CommandSpec {
        id: "postgres-public-consumer",
        label: "PostgreSQL public consumer test",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres",
            "--test",
            "postgres_public_consumer_test",
        ],
    },
    CommandSpec {
        id: "postgres-package-boundary",
        label: "PostgreSQL package boundary test",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--test",
            "postgres_connector_package_boundary_test",
        ],
    },
];

pub(super) fn run(root: &Path, directory: &Path, report: &mut Report) -> Result<()> {
    let _signal_guard = SignalGuard::install()?;
    report.setup = Phase::new(State::Running);
    report.save(directory)?;
    let setup = (|| {
        let compose = Compose::preflight()?;
        super::report_existing_test_sessions(root, &compose)?;
        TestSession::start(root.to_path_buf(), compose, directory, report)
    })();
    let mut session = match setup {
        Ok(session) => session,
        Err(failure) => {
            report.setup = Phase::incomplete(&failure);
            if let Err(save) = report.save(directory) {
                return Err(error(format!(
                    "{failure}; retaining coordinator evidence failed: {save}"
                )));
            }
            return Err(failure);
        }
    };
    report.setup = Phase::new(State::Passed);
    // Once a session exists, every reporting/execution error still flows through
    // finish so persistence cannot bypass the existing service cleanup owner.
    let proof = (|| {
        report.save(directory)?;
        session.prepare(directory, report)?;
        session.run_targets(directory, report)
    })();
    session.finish(proof, directory, report)
}

struct TestSession {
    root: PathBuf,
    compose: Compose,
    identity: TestIdentity,
    state: SessionState,
    service: ServiceEvidence,
    cleaned: bool,
}

impl TestSession {
    fn start(
        root: PathBuf,
        compose: Compose,
        directory: &Path,
        report: &mut Report,
    ) -> Result<Self> {
        let run_id = state::unique_run_id();
        let identity = state::test_identity(&root, &run_id)?;
        if identity.directory.exists() {
            return Err(error(
                "PostgreSQL test generated an already-owned session identity",
            ));
        }
        state::create_session_directory(&identity.directory)?;
        let mut session_state = state::new_test(&identity);
        report.session_run_id = Some(run_id);
        report.cleanup = Phase::new(State::Pending);
        let mut service_attempted = false;
        let setup = (|| {
            report.save(directory)?;
            state::write(&identity.directory.join(STATE_FILE), &session_state)?;
            credentials::create_acceptance_raw(&identity.directory)?;
            tls::create_test(&identity.directory)?;
            service_attempted = true;
            compose.start(&root, &identity.directory, &mut session_state)?;
            let service = compose.service_evidence(&root, &identity.directory, &session_state)?;
            state::record_or_verify_volume(&mut session_state, &service.volume)?;
            credentials::create_acceptance_pgpass(&identity.directory, session_state.port)?;
            credentials::validate_acceptance(&identity.directory, session_state.port)?;
            state::write(&identity.directory.join(STATE_FILE), &session_state)?;
            fixtures::provision_tests(&root, &compose, &identity.directory, &session_state)?;
            Ok(service)
        })();
        let service = match setup {
            Ok(service) => service,
            Err(failure) => {
                return Err(cleanup_setup_failure(
                    failure,
                    &root,
                    &compose,
                    &identity,
                    &session_state,
                    (directory, report),
                    service_attempted,
                ))
            }
        };
        Ok(Self {
            root,
            compose,
            identity,
            state: session_state,
            service,
            cleaned: false,
        })
    }

    fn prepare(&self, directory: &Path, report: &mut Report) -> Result<()> {
        check_signal()?;
        report.preparation = Phase::new(State::Running);
        report.save(directory)?;
        let mut command = Command::new("cargo");
        command
            .current_dir(&self.root)
            .args(crate::validation::dependencies::FETCH_ARGS);
        let execution = command_status(&mut command, directory, "dependencies");
        report.preparation = match execution {
            Ok(status) if status.success() && check_signal().is_ok() => {
                Phase::exited(status, false)
            }
            Ok(status) => Phase::incomplete(format!(
                "locked dependency preparation did not complete: {status}"
            )),
            Err(failure) => Phase::incomplete(failure),
        };
        report.save(directory)?;
        // Unrelated unit and database targets still supply useful evidence.
        // Preparation remains independently incomplete even if they all pass.
        check_signal()
    }

    fn run_targets(&mut self, directory: &Path, report: &mut Report) -> Result<()> {
        run_selected_targets(|index, spec| {
            check_signal()?;
            println!("\n==> {}", spec.label);
            report.targets[index].result = Phase::new(State::Running);
            report.save(directory)?;
            let execution = (|| {
                let mut child = Command::new("cargo");
                child.current_dir(&self.root).args(spec.cargo_args);
                environment::configure_test(
                    &mut child,
                    &self.identity.directory,
                    &self.state,
                    &self.service,
                )?;
                if spec.id == "hn-digest-postgres-treatment" {
                    child
                        .env(
                            "OBZENFLOW_POSTGRES_SCHEMA",
                            super::config::hn_digest_test_schema(&self.state.run_id),
                        )
                        .env(
                            "OBZENFLOW_SINKS_STAGES_DIGEST_SUMMARY_HANDLER",
                            "postgres_sink",
                        );
                }
                command_status(&mut child, directory, spec.id)
            })();
            report.targets[index].result = match &execution {
                Ok(status) => Phase::exited(*status, check_signal().is_err()),
                Err(failure) => Phase::incomplete(failure),
            };
            if let Err(save) = report.save(directory) {
                return Err(error(format!(
                    "{} result={execution:?}; retaining evidence failed: {save}",
                    spec.id
                )));
            }
            let status = execution?;
            check_signal()?;
            Ok(status)
        })
    }

    fn finish(&mut self, proof: Result<()>, directory: &Path, report: &mut Report) -> Result<()> {
        let captured_log = proof
            .as_ref()
            .err()
            .and_then(|_| self.capture_failure_logs().ok());
        report.cleanup = Phase::new(State::Running);
        let before_save = report.save(directory);
        let cleanup = self.cleanup();
        report.cleanup = match &cleanup {
            Ok(()) => Phase::new(State::Passed),
            Err(failure) => Phase {
                state: State::Rejected,
                detail: Some(failure.to_string()),
                ..Phase::new(State::Rejected)
            },
        };
        let after_save = report.save(directory);
        let completion = match (proof, cleanup) {
            (Ok(()), Ok(())) => {
                Ok(())
            }
            (Err(proof), Ok(())) => Err(error(match captured_log {
                Some(path) => format!("{proof}; captured logs: {}", path.display()),
                None => proof.to_string(),
            })),
            (Ok(()), Err(cleanup)) => Err(error(format!(
                "PostgreSQL tests passed but cleanup failed: {cleanup}; recover with `cargo xtask postgres cleanup {}`",
                self.state.run_id
            ))),
            (Err(proof), Err(cleanup)) => Err(error(format!(
                "{proof}; cleanup={cleanup}; recover with `cargo xtask postgres cleanup {}`",
                self.state.run_id
            ))),
        };
        combine_results(completion, combine_results(before_save, after_save))
    }

    fn capture_failure_logs(&self) -> Result<PathBuf> {
        let directory = self.root.join(SESSION_ROOT).join("failures");
        fs::create_dir_all(&directory)?;
        let destination = directory.join(format!("{}.log", self.state.run_id));
        self.compose.capture_logs(
            &self.root,
            &self.identity.directory,
            &self.state,
            &destination,
        )?;
        Ok(destination)
    }

    fn cleanup(&mut self) -> Result<()> {
        if self.cleaned {
            return Ok(());
        }
        cleanup_started_session(&self.root, &self.compose, &self.identity, &self.state)?;
        self.cleaned = true;
        Ok(())
    }
}

/// The shared xtask target is independent of the following service proofs.
/// Preserve its rejected command while retaining the established stop boundary
/// among the resource-mutating PostgreSQL targets themselves.
fn run_selected_targets(
    mut execute: impl FnMut(usize, &CommandSpec) -> Result<ExitStatus>,
) -> Result<()> {
    let unit_status = execute(0, &COMMANDS[0])?;
    let unit = require_success(&COMMANDS[0], unit_status);
    let products = (|| {
        for (index, command) in COMMANDS.iter().enumerate().skip(1) {
            require_success(command, execute(index, command)?)?;
        }
        Ok(())
    })();
    combine_results(unit, products)
}

fn require_success(spec: &CommandSpec, status: ExitStatus) -> Result<()> {
    if status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "{} target command failed with status {status}",
            spec.id
        )))
    }
}

fn combine_results(first: Result<()>, second: Result<()>) -> Result<()> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(failure), Ok(())) | (Ok(()), Err(failure)) => Err(failure),
        (Err(first), Err(second)) => Err(error(format!("{first}; {second}"))),
    }
}

/// Keep the PostgreSQL owner's inherited process group and signal handling.
/// The validator's process::execute creates a new group and has a separate
/// signal flag, so importing it here would change service cleanup semantics.
fn command_status(command: &mut Command, directory: &Path, label: &str) -> Result<ExitStatus> {
    fs::write(
        directory.join(format!("{label}.command.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "program": command.get_program().to_string_lossy(),
            "args": command.get_args().map(|arg| arg.to_string_lossy()).collect::<Vec<_>>(),
        }))?,
    )?;
    // coordinator.json owns the captured status. A second diagnostic write
    // after wait must not turn a known rejected command into an I/O-only error.
    command.status().map_err(|failure| {
        error(format!(
            "{label}: command execution failed (os_code={:?}): {failure}",
            failure.raw_os_error()
        ))
    })
}

fn cleanup_started_session(
    root: &Path,
    compose: &Compose,
    identity: &TestIdentity,
    session: &SessionState,
) -> Result<()> {
    state::require_owned_directory(root, &identity.directory)?;
    state::require_test_authority(session, identity)?;
    let expected_volume = state::expected_volume(&session.project);
    if let Some(container_id) = compose.container_id(root, &identity.directory, session)? {
        compose.verify_container_authority(&container_id, &identity.project)?;
        let actual_volume = compose.container_volume(&container_id)?;
        if actual_volume != expected_volume {
            return Err(error(
                "refusing cleanup because the disposable PostgreSQL volume changed",
            ));
        }
        compose.verify_volume_authority(&actual_volume, &session.project)?;
    }
    compose.verify_volume_authority_if_present(&expected_volume, &session.project)?;
    compose.stop(root, &identity.directory, session, true)?;
    if compose.volume_exists(&expected_volume)? {
        return Err(error(format!(
            "Docker retained disposable PostgreSQL volume {expected_volume}"
        )));
    }
    state::remove_owned_directory(root, &identity.directory)
}

fn cleanup_setup_failure(
    failure: Box<dyn std::error::Error>,
    root: &Path,
    compose: &Compose,
    identity: &TestIdentity,
    session: &SessionState,
    evidence: (&Path, &mut Report),
    service_attempted: bool,
) -> Box<dyn std::error::Error> {
    let (directory, report) = evidence;
    report.cleanup = Phase::new(State::Running);
    // Persistence is best effort here: cleanup still owns the disposable state.
    let before = report.save(directory);
    let cleanup = if service_attempted {
        cleanup_started_session(root, compose, identity, session)
    } else {
        state::remove_owned_directory(root, &identity.directory)
    };
    report.cleanup = match &cleanup {
        Ok(()) => Phase::new(State::Passed),
        Err(cleanup) => Phase {
            detail: Some(cleanup.to_string()),
            ..Phase::new(State::Rejected)
        },
    };
    let saved = combine_results(before, report.save(directory));
    let failure = match cleanup {
        Ok(()) => failure,
        Err(cleanup) => error(format!(
            "{failure}; setup cleanup failed: {cleanup}; recover with `cargo xtask postgres cleanup {}`",
            session.run_id
        )),
    };
    match saved {
        Ok(()) => failure,
        Err(save) => error(format!(
            "{failure}; retaining coordinator evidence failed: {save}"
        )),
    }
}

impl Drop for TestSession {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = self.cleanup();
        }
    }
}

#[cfg(unix)]
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);

#[cfg(unix)]
extern "C" fn record_signal(signal: libc::c_int) {
    RECEIVED_SIGNAL.store(signal, Ordering::Relaxed);
}

#[cfg(unix)]
struct SignalGuard {
    previous_interrupt: libc::sighandler_t,
    previous_terminate: libc::sighandler_t,
}

#[cfg(unix)]
impl SignalGuard {
    fn install() -> Result<Self> {
        RECEIVED_SIGNAL.store(0, Ordering::Relaxed);
        // SAFETY: the handler performs only a lock-free atomic store. The
        // process-global handlers are restored when the test session ends.
        let previous_interrupt =
            unsafe { libc::signal(libc::SIGINT, record_signal as *const () as _) };
        if previous_interrupt == libc::SIG_ERR {
            return Err(error("failed to install PostgreSQL test SIGINT handler"));
        }
        // SAFETY: see the SIGINT installation above.
        let previous_terminate =
            unsafe { libc::signal(libc::SIGTERM, record_signal as *const () as _) };
        if previous_terminate == libc::SIG_ERR {
            // SAFETY: restoring the handler returned by `signal`.
            unsafe { libc::signal(libc::SIGINT, previous_interrupt) };
            return Err(error("failed to install PostgreSQL test SIGTERM handler"));
        }
        Ok(Self {
            previous_interrupt,
            previous_terminate,
        })
    }
}

#[cfg(unix)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        // SAFETY: restoring the handlers returned by the matching calls above.
        unsafe {
            libc::signal(libc::SIGINT, self.previous_interrupt);
            libc::signal(libc::SIGTERM, self.previous_terminate);
        }
    }
}

#[cfg(not(unix))]
struct SignalGuard;

#[cfg(not(unix))]
impl SignalGuard {
    fn install() -> Result<Self> {
        Ok(Self)
    }
}

fn check_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let signal = RECEIVED_SIGNAL.load(Ordering::Relaxed);
        if signal != 0 {
            return Err(error(format!(
                "PostgreSQL tests interrupted by signal {signal}"
            )));
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Stdio;

    #[test]
    fn unit_rejection_preserves_independent_targets_and_product_stop_boundary() {
        // Reuse the existing real libtest controls. There is no Cargo rebuild,
        // database, simulated ExitStatus or alternate command scheduler here.
        for product_failure in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut reached = Vec::new();
            let result = run_selected_targets(|index, spec| {
                reached.push(spec.id);
                let fails = index == 0 || (product_failure && index == 1);
                let fixture = if fails {
                    "validation::tests::leaks::assertion_fixture"
                } else {
                    "validation::tests::leaks::passing_fixture"
                };
                let mut command = Command::new(std::env::current_exe()?);
                command
                    .args(["--ignored", "--exact", fixture, "--nocapture"])
                    .env("OBZENFLOW_LEAK_CONTROL_MODE", "mixed")
                    .stdout(Stdio::null())
                    .stderr(fs::File::create(
                        directory.path().join(format!("{index}.stderr")),
                    )?);
                command_status(&mut command, directory.path(), spec.id)
            });
            let failure = result.unwrap_err().to_string();
            assert!(failure.contains("xtask-unit-conformance"), "{failure}");
            if product_failure {
                assert_eq!(
                    reached.len(),
                    2,
                    "resource-mutating targets retain their stop boundary"
                );
                assert!(failure.contains("postgres-lifecycle"), "{failure}");
            } else {
                assert_eq!(reached.len(), COMMANDS.len());
                let independent = fs::read_to_string(directory.path().join("1.stderr")).unwrap();
                assert!(independent.contains("independent work completed"));
            }
        }
        let mut attempted = 0;
        let result = run_selected_targets(|_, _| {
            attempted += 1;
            Err(error("service authority unavailable"))
        });
        assert!(result.is_err());
        assert_eq!(
            attempted, 1,
            "setup failure cannot authorise later service work"
        );
    }
}
