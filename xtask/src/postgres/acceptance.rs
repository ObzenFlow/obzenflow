// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    compose::{Compose, ServiceEvidence},
    config::{SESSION_ROOT, STATE_FILE},
    environment, fixtures,
    state::{self, SessionMode, SessionState, TestIdentity},
    tls,
};
use crate::{error, Result};
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

struct CommandSpec {
    label: &'static str,
    cargo_args: &'static [&'static str],
}

// Test-target granularity is intentional. Individual test functions remain
// owned by their Rust test binaries and can be added or renamed independently.
const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        label: "xtask PostgreSQL unit tests",
        cargo_args: &["test", "--locked", "-p", "xtask", "--bin", "xtask"],
    },
    CommandSpec {
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
        label: "DSL sink composition tests",
        cargo_args: &["test", "--locked", "-p", "obzenflow_dsl", "--lib"],
    },
    CommandSpec {
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
        label: "production-feature payments example build",
        cargo_args: &[
            "build",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres",
            "--example",
            "postgres_sink_payments",
        ],
    },
    CommandSpec {
        label: "payments example black-box test",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres,e2e",
            "--test",
            "postgres_sink_example_e2e_test",
        ],
    },
    CommandSpec {
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

pub(super) fn run(root: PathBuf, compose: Compose) -> Result<()> {
    let _signal_guard = SignalGuard::install()?;
    let mut session = TestSession::start(root, compose)?;
    let proof = session.run_targets();
    session.finish(proof)
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
    fn start(root: PathBuf, compose: Compose) -> Result<Self> {
        let run_id = state::unique_run_id();
        let identity = state::test_identity(&root, &run_id)?;
        if identity.directory.exists() {
            return Err(error(
                "PostgreSQL test generated an already-owned session identity",
            ));
        }
        fs::create_dir_all(&identity.directory)?;
        let mut session_state = SessionState {
            project: identity.project.clone(),
            run_id,
            port: 0,
            mode: SessionMode::Test,
        };
        state::write(&identity.directory.join(STATE_FILE), &session_state)?;
        if let Err(failure) = tls::ensure(&identity.directory) {
            let _ = state::remove_owned_directory(&root, &identity.directory);
            return Err(failure);
        }
        if let Err(failure) = compose.start(&root, &identity.directory, &mut session_state) {
            return Err(cleanup_setup_failure(
                failure,
                &root,
                &compose,
                &identity,
                &session_state,
            ));
        }
        let setup = (|| {
            state::write(&identity.directory.join(STATE_FILE), &session_state)?;
            let service = compose.service_evidence(&root, &identity.directory, &session_state)?;
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

    fn run_targets(&mut self) -> Result<()> {
        for command in COMMANDS {
            check_signal()?;
            println!("\n==> {}", command.label);
            let mut child = Command::new("cargo");
            child.current_dir(&self.root).args(command.cargo_args);
            environment::configure(
                &mut child,
                &self.identity.directory,
                &self.state,
                &self.service,
            )?;
            let status = child.status()?;
            if !status.success() {
                return Err(error(format!(
                    "{} failed with status {status}",
                    command.label
                )));
            }
            check_signal()?;
        }
        Ok(())
    }

    fn finish(&mut self, proof: Result<()>) -> Result<()> {
        let captured_log = proof
            .as_ref()
            .err()
            .and_then(|_| self.capture_failure_logs().ok());
        let cleanup = self.cleanup();
        match (proof, cleanup) {
            (Ok(()), Ok(())) => {
                println!(
                    "PostgreSQL test targets passed; project={} cleaned",
                    self.state.project
                );
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
        }
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

fn cleanup_started_session(
    root: &Path,
    compose: &Compose,
    identity: &TestIdentity,
    session: &SessionState,
) -> Result<()> {
    state::require_owned_directory(root, &identity.directory)?;
    state::require_test_authority(session, identity)?;
    if let Some(container_id) = compose.container_id(root, &identity.directory, session)? {
        compose.verify_container_authority(&container_id, &identity.project)?;
    }
    compose.stop(root, &identity.directory, session, true)?;
    state::remove_owned_directory(root, &identity.directory)
}

fn cleanup_setup_failure(
    failure: Box<dyn std::error::Error>,
    root: &Path,
    compose: &Compose,
    identity: &TestIdentity,
    session: &SessionState,
) -> Box<dyn std::error::Error> {
    match cleanup_started_session(root, compose, identity, session) {
        Ok(()) => failure,
        Err(cleanup) => error(format!(
            "{failure}; setup cleanup failed: {cleanup}; recover with `cargo xtask postgres cleanup {}`",
            session.run_id
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
