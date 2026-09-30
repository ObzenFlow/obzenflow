// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::plan::Policy;
use crate::{error, Result};
use serde::Serialize;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicI32, Ordering},
    thread,
    time::{Duration, Instant},
};

static SIGNAL: AtomicI32 = AtomicI32::new(0);
extern "C" fn interrupted(signal: libc::c_int) {
    SIGNAL.store(signal, Ordering::Relaxed);
}

pub(super) struct SignalGuard {
    previous: [libc::sighandler_t; 2],
}
impl SignalGuard {
    pub(super) fn install() -> Result<Self> {
        SIGNAL.store(0, Ordering::Relaxed);
        // Only a lock-free atomic store runs in the signal handler.
        let interrupt = unsafe { libc::signal(libc::SIGINT, interrupted as *const () as _) };
        if interrupt == libc::SIG_ERR {
            return Err(std::io::Error::last_os_error().into());
        }
        let terminate = unsafe { libc::signal(libc::SIGTERM, interrupted as *const () as _) };
        if terminate == libc::SIG_ERR {
            let failure = std::io::Error::last_os_error();
            unsafe {
                libc::signal(libc::SIGINT, interrupt);
            }
            return Err(failure.into());
        }
        Ok(Self {
            previous: [interrupt, terminate],
        })
    }
}
impl Drop for SignalGuard {
    fn drop(&mut self) {
        unsafe {
            libc::signal(libc::SIGINT, self.previous[0]);
            libc::signal(libc::SIGTERM, self.previous[1]);
        }
    }
}
pub(super) fn was_interrupted() -> bool {
    SIGNAL.load(Ordering::Relaxed) != 0
}

pub(super) fn lock(root: &Path) -> Result<File> {
    fs::create_dir_all(root.join("target/test-runs"))?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("target/test-runs/acceptance.lock"))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // The open file owns this advisory lock until dropped; inherited child
        // commands do not receive the close-on-exec descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(error("another local acceptance command owns this checkout's build/results; wait for it to finish"));
        }
    }
    Ok(file)
}

pub(super) fn command(
    root: &Path,
    policy: &Policy,
    program: impl AsRef<std::ffi::OsStr>,
) -> Command {
    let mut command = Command::new(program);
    command.current_dir(root);
    // Ambient Nextest options must not narrow or relax the declared plan.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("NEXTEST_")
            || name.to_string_lossy().starts_with("CARGO_PROFILE_")
        {
            command.env_remove(name);
        }
    }
    command
        .env_remove("CARGO_BUILD_TARGET")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_PROFILE_DEV_DEBUG", "0")
        .env("CARGO_PROFILE_TEST_DEBUG", "0")
        .env("CARGO_TERM_COLOR", "never")
        .env("CARGO_BUILD_JOBS", policy.build_jobs.to_string())
        .env("TOKIO_WORKER_THREADS", policy.tokio_workers.to_string())
        .env("NEXTEST_USER_CONFIG_FILE", "none");
    command
}

#[derive(Serialize)]
struct Invocation {
    program: String,
    args: Vec<String>,
    watchdog_seconds: u64,
    started_at_unix_ms: u128,
}

struct OwnedChild {
    process: Child,
    reaped: bool,
}

impl OwnedChild {
    fn signal(&mut self, signal: libc::c_int) {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.process.id() as i32), signal);
        }
        #[cfg(not(unix))]
        let _ = self.process.kill();
    }

    fn reap(&mut self) {
        let _ = self.process.wait();
        self.reaped = true;
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        // An I/O error or panic must not leave a Cargo/Nextest child running.
        if !self.reaped {
            self.signal(libc::SIGKILL);
            self.reap();
        }
    }
}

pub(super) fn execute(
    command: &mut Command,
    directory: &Path,
    label: &str,
    timeout: Duration,
) -> Result<ExitStatus> {
    execute_observed(command, directory, label, timeout, None)
}

pub(super) fn execute_nextest(
    command: &mut Command,
    directory: &Path,
    timeout: Duration,
) -> Result<ExitStatus> {
    execute_observed(
        command,
        directory,
        "tests",
        timeout,
        Some(&mut |bytes| eprint!("{}", String::from_utf8_lossy(bytes))),
    )
}

type OutputCallback<'a> = &'a mut dyn FnMut(&[u8]);

struct Feedback<'a> {
    file: File,
    remaining: usize,
    publish: OutputCallback<'a>,
}

impl Feedback<'_> {
    fn poll(&mut self) -> std::io::Result<()> {
        let mut buffer = [0; 4096];
        while self.remaining > 0 {
            let limit = self.remaining.min(buffer.len());
            let read = self.file.read(&mut buffer[..limit])?;
            if read == 0 {
                break;
            }
            (self.publish)(&buffer[..read]);
            self.remaining -= read;
            if self.remaining == 0 {
                (self.publish)(b"\nvalidation: live diagnostic limit reached; complete output remains in tests.stderr.log\n");
            }
        }
        Ok(())
    }
}

pub(super) fn execute_observed(
    command: &mut Command,
    directory: &Path,
    label: &str,
    timeout: Duration,
    publish: Option<OutputCallback<'_>>,
) -> Result<ExitStatus> {
    fs::create_dir_all(directory)?;
    let out = directory.join(format!("{label}.stdout.log"));
    let err = directory.join(format!("{label}.stderr.log"));
    let invocation = Invocation {
        program: command.get_program().to_string_lossy().into_owned(),
        args: command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect(),
        watchdog_seconds: timeout.as_secs(),
        started_at_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    };
    fs::write(
        directory.join(format!("{label}.command.json")),
        serde_json::to_vec_pretty(&invocation)?,
    )?;
    eprintln!("validation: {label}; logs={}", directory.display());
    command
        .stdout(Stdio::from(File::create(&out)?))
        .stderr(Stdio::from(File::create(&err)?));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = OwnedChild {
        process: command.spawn()?,
        reaped: false,
    };
    // Reading the ordinary log file cannot block the child on a pipe. Forward
    // its early failure evidence while independent tests continue, with a
    // fixed display budget; the original output and JUnit remain complete.
    let mut feedback = match publish {
        Some(publish) => Some(Feedback {
            file: File::open(&err)?,
            remaining: 16 * 1024,
            publish,
        }),
        None => None,
    };
    let start = Instant::now();
    let mut heartbeat = start;
    let mut cancellation = None;
    loop {
        if let Some(feedback) = &mut feedback {
            feedback.poll()?;
        }
        if let Some(status) = child.process.try_wait()? {
            child.reaped = true;
            if let Some(feedback) = &mut feedback {
                feedback.poll()?;
            }
            fs::write(
                directory.join(format!("{label}.result.json")),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "exit_code": status.code(), "success": status.success(), "elapsed_seconds": start.elapsed().as_secs_f64(), "incomplete": cancellation,
                }))?,
            )?;
            eprintln!(
                "validation: {label} finished in {:.2}s ({status})",
                start.elapsed().as_secs_f64()
            );
            if !status.success() {
                show_tail(&err);
                show_tail(&out);
            }
            if let Some(reason) = cancellation {
                return Err(error(format!(
                    "{label} incomplete: {reason}; artifacts={}",
                    directory.display()
                )));
            }
            return Ok(status);
        }
        if cancellation.is_none() && (was_interrupted() || start.elapsed() >= timeout) {
            let reason = if was_interrupted() {
                "interrupted"
            } else {
                "infrastructure watchdog expired"
            };
            eprintln!("validation: {label}: {reason}; stopping owned child group");
            cancellation = Some(reason);
            child.signal(libc::SIGTERM);
            heartbeat = Instant::now();
        }
        // PostgreSQL's service owner must be able to reap its Cargo child and
        // complete Compose cleanup. This is teardown grace, not extra test time.
        if let Some(reason) =
            cancellation.filter(|_| heartbeat.elapsed() >= Duration::from_secs(60))
        {
            child.signal(libc::SIGKILL);
            child.reap();
            fs::write(
                directory.join(format!("{label}.result.json")),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "exit_code": null, "success": false, "elapsed_seconds": start.elapsed().as_secs_f64(),
                    "incomplete": cancellation, "forced_cleanup": true,
                }))?,
            )?;
            show_tail(&err);
            show_tail(&out);
            return Err(error(format!(
                "{label} incomplete: {}; artifacts={}",
                reason,
                directory.display()
            )));
        }
        if cancellation.is_none() && heartbeat.elapsed() >= Duration::from_secs(15) {
            eprintln!(
                "validation: {label} still running ({:.0}s); logs={}",
                start.elapsed().as_secs_f64(),
                directory.display()
            );
            heartbeat = Instant::now();
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn show_tail(path: &Path) {
    let Ok(mut file) = File::open(path) else {
        return;
    };
    let length = file.metadata().map(|m| m.len()).unwrap_or(0);
    let _ = file.seek(SeekFrom::Start(length.saturating_sub(16 * 1024)));
    let mut bytes = Vec::new();
    if file.take(16 * 1024).read_to_end(&mut bytes).is_ok() && !bytes.is_empty() {
        eprintln!("{}:\n{}", path.display(), String::from_utf8_lossy(&bytes));
    }
}

pub(super) fn capture(
    root: &Path,
    policy: &Policy,
    program: &str,
    args: &[&str],
    directory: &Path,
    label: &str,
) -> Result<String> {
    let status = execute(
        command(root, policy, program).args(args),
        directory,
        label,
        Duration::from_secs(policy.command_watchdog_seconds),
    )?;
    if !status.success() {
        return Err(error(format!("{label} failed ({status})")));
    }
    Ok(fs::read_to_string(
        directory.join(format!("{label}.stdout.log")),
    )?)
}
