// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::process::{force_kill_process, process_exists};
use super::{JobSpec, FORCE_KILL_WAIT, JOBS, PHONEBOOK_PORT};
use crate::{error, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

pub(super) fn ensure_ports_free() -> Result<()> {
    let occupied = occupied_required_ports()?;

    if occupied.is_empty() {
        Ok(())
    } else {
        Err(error(format!(
            "required runtime ports are already occupied: {}",
            describe_ports(&occupied)
        )))
    }
}

fn occupied_required_ports() -> io::Result<Vec<JobSpec>> {
    JOBS.iter()
        .filter_map(|job| match port_is_available(job.port) {
            Ok(true) => None,
            Ok(false) => Some(Ok(*job)),
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn describe_ports(jobs: &[JobSpec]) -> String {
    jobs.iter()
        .map(|job| format!("{} ({})", job.port, job.job_id))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Repeatedly discover and SIGKILL listeners on Studio's configured runtime
/// ports until every port can be bound or the bounded cleanup window expires.
/// Repeating matters when a detached parent races by respawning its child.
pub(super) fn force_release_required_ports() -> Result<()> {
    let deadline = Instant::now() + FORCE_KILL_WAIT;
    let mut announced = BTreeSet::new();

    loop {
        let occupied = occupied_required_ports()?;
        if occupied.is_empty() {
            return Ok(());
        }

        let mut owners = BTreeMap::<u32, Vec<JobSpec>>::new();
        for job in &occupied {
            for pid in listener_pids_for_port(job.port)? {
                owners.entry(pid).or_default().push(*job);
            }
        }

        if owners.is_empty() {
            if Instant::now() >= deadline {
                return Err(error(format!(
                    "required runtime ports remain occupied but no listener PID could be identified: {}",
                    describe_ports(&occupied)
                )));
            }
            thread::sleep(Duration::from_millis(100));
            continue;
        }

        let mut kill_failures = Vec::new();
        for (pid, jobs) in owners {
            if announced.insert(pid) {
                println!(
                    "force-killing orphan listener pid={} on {}",
                    pid,
                    describe_ports(&jobs)
                );
            }
            match force_kill_process(pid) {
                Ok(true) => {}
                Ok(false) if !process_exists(pid) => {}
                Ok(false) => kill_failures.push(format!("pid {pid} rejected SIGKILL")),
                Err(error) => kill_failures.push(format!("pid {pid}: {error}")),
            }
        }
        if !kill_failures.is_empty() {
            return Err(error(format!(
                "failed to SIGKILL Studio port owners: {}",
                kill_failures.join("; ")
            )));
        }

        if Instant::now() >= deadline {
            let occupied = occupied_required_ports()?;
            if occupied.is_empty() {
                return Ok(());
            }
            return Err(error(format!(
                "required runtime ports remain occupied after SIGKILL: {}",
                describe_ports(&occupied)
            )));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn port_is_available(port: u16) -> io::Result<bool> {
    match TcpListener::bind(("127.0.0.1", port)) {
        Ok(listener) => {
            drop(listener);
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn listener_pids_for_port(port: u16) -> io::Result<Vec<u32>> {
    let selector = format!("-iTCP:{port}");
    let output = Command::new("lsof")
        .args(["-nP", "-t", &selector, "-sTCP:LISTEN"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "`lsof` is required for `studio-jobs up --force` to identify orphan port owners",
                )
            } else {
                error
            }
        })?;

    // `lsof` uses exit 1 for an empty selection. That is a valid race: the
    // bind probe observed the listener just before it exited.
    if !output.status.success() && output.status.code() != Some(1) {
        return Err(io::Error::other(format!(
            "lsof failed while inspecting port {port}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    parse_pid_lines(&String::from_utf8_lossy(&output.stdout))
}

#[cfg(unix)]
fn parse_pid_lines(output: &str) -> io::Result<Vec<u32>> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .try_fold(BTreeSet::new(), |mut pids, line| {
            let pid = line.parse::<u32>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("lsof returned invalid PID '{line}': {error}"),
                )
            })?;
            pids.insert(pid);
            Ok(pids)
        })
        .map(|pids| pids.into_iter().collect())
}

#[cfg(windows)]
fn listener_pids_for_port(port: u16) -> io::Result<Vec<u32>> {
    let output = Command::new("netstat")
        .args(["-ano", "-p", "tcp"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "netstat failed while inspecting port {port}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let pids = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 5
                || !fields[0].eq_ignore_ascii_case("TCP")
                || !fields[3].eq_ignore_ascii_case("LISTENING")
                || fields[1]
                    .rsplit_once(':')
                    .and_then(|(_, value)| value.parse::<u16>().ok())
                    != Some(port)
            {
                return None;
            }
            fields[4].parse::<u32>().ok()
        })
        .collect::<BTreeSet<_>>();
    Ok(pids.into_iter().collect())
}

pub(super) fn phonebook_is_listening() -> bool {
    port_is_reachable(PHONEBOOK_PORT)
}

pub(super) fn port_is_reachable(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(150)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn parses_and_deduplicates_lsof_pid_output() {
        assert_eq!(
            parse_pid_lines("4242\n  17 \n4242\n\n").expect("valid lsof output"),
            vec![17, 4242]
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_malformed_lsof_pid_output() {
        let error = parse_pid_lines("4242\nnot-a-pid\n").expect_err("invalid PID must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not-a-pid"));
    }
}
