// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::TERMINATE_WAIT;
use std::{
    collections::BTreeSet,
    io,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

pub(super) fn wait_until_stopped(pid: u32) -> bool {
    let checks = TERMINATE_WAIT.as_millis() / 100;
    for _ in 0..checks {
        if !process_exists(pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    !process_exists(pid)
}

pub(super) fn wait_until_all_stopped(pids: &BTreeSet<u32>, timeout: Duration) -> Vec<u32> {
    let deadline = Instant::now() + timeout;
    loop {
        let alive = pids
            .iter()
            .copied()
            .filter(|pid| process_exists(*pid))
            .collect::<Vec<_>>();
        if alive.is_empty() || Instant::now() >= deadline {
            return alive;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

pub(super) fn join_pids(pids: &[u32]) -> String {
    pids.iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(unix)]
pub(super) fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
pub(super) fn process_exists(pid: u32) -> bool {
    let filter = format!("PID eq {pid}");
    Command::new("tasklist")
        .args(["/FI", &filter])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .is_ok_and(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
        })
}

#[cfg(unix)]
pub(super) fn terminate_process(pid: u32) -> io::Result<bool> {
    Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
}

#[cfg(windows)]
pub(super) fn terminate_process(pid: u32) -> io::Result<bool> {
    Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T"])
        .status()
        .map(|status| status.success())
}

#[cfg(unix)]
pub(super) fn force_kill_process(pid: u32) -> io::Result<bool> {
    validate_kill_pid(pid)?;
    Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
}

#[cfg(windows)]
pub(super) fn force_kill_process(pid: u32) -> io::Result<bool> {
    validate_kill_pid(pid)?;
    Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
}

fn validate_kill_pid(pid: u32) -> io::Result<()> {
    if pid <= 1 || pid == std::process::id() {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("refusing to signal protected pid {pid}"),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_to_signal_protected_pids() {
        assert_eq!(
            validate_kill_pid(0)
                .expect_err("process group must be protected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_kill_pid(1)
                .expect_err("init must be protected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_kill_pid(std::process::id())
                .expect_err("xtask itself must be protected")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(unix)]
    #[test]
    fn force_kill_process_sends_an_uncatchable_signal() {
        let mut child = Command::new("sleep")
            .arg("60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn disposable child");
        let pid = child.id();

        assert!(force_kill_process(pid).expect("send SIGKILL"));
        let status = child.wait().expect("reap killed child");
        assert!(!status.success());
        assert!(!process_exists(pid));
    }
}
