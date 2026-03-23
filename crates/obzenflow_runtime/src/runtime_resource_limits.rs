// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::journal::JournalError;
use std::io;

#[derive(Clone, Copy, Debug)]
pub(crate) struct NofileLimit {
    pub(crate) soft: u64,
    pub(crate) hard: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DiskJournalFdEstimate {
    pub(crate) stages: usize,
    pub(crate) edges: usize,
    pub(crate) metrics_enabled: bool,
    pub(crate) estimated_fds: u64,
    pub(crate) breakdown: DiskJournalFdBreakdown,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DiskJournalFdBreakdown {
    pub(crate) writer_fds: u64,
    pub(crate) stage_reader_fds: u64,
    pub(crate) metrics_reader_fds: u64,
    pub(crate) system_reader_fds: u64,
    pub(crate) overhead_fds: u64,
}

pub(crate) fn estimate_disk_journal_fds(
    stages: usize,
    edges: usize,
    metrics_enabled: bool,
) -> DiskJournalFdEstimate {
    let stage_writers = 2u64.saturating_mul(stages as u64); // data + error
    let system_writer = 1u64; // system journal (DiskJournal holds a write FD)

    // One upstream reader per topology edge (subscription reader).
    let stage_readers = edges as u64;

    // Metrics aggregator (enabled by default) tails both data and error journals:
    // - one reader per stage data journal
    // - one reader per stage error journal
    // - plus a system journal reader for lifecycle/coordination
    let metrics_readers = if metrics_enabled {
        (2u64.saturating_mul(stages as u64)).saturating_add(1)
    } else {
        0
    };

    // System journal readers owned by the pipeline runtime itself.
    // - completion subscription is long-lived
    // - readiness/drain readers are short-lived but can overlap during startup/shutdown
    let system_readers = 2u64;

    // Leave headroom for non-journal FDs (stdout/stderr, sockets, resolver, etc).
    let overhead = 32u64;

    let estimated = stage_writers
        .saturating_add(system_writer)
        .saturating_add(stage_readers)
        .saturating_add(metrics_readers)
        .saturating_add(system_readers)
        .saturating_add(overhead);

    DiskJournalFdEstimate {
        stages,
        edges,
        metrics_enabled,
        estimated_fds: estimated,
        breakdown: DiskJournalFdBreakdown {
            writer_fds: stage_writers.saturating_add(system_writer),
            stage_reader_fds: stage_readers,
            metrics_reader_fds: metrics_readers,
            system_reader_fds: system_readers,
            overhead_fds: overhead,
        },
    }
}

pub(crate) fn journal_error_is_too_many_open_files(err: &JournalError) -> bool {
    match err {
        JournalError::Implementation { source, .. } => {
            if let Some(io_error) = source.downcast_ref::<io::Error>() {
                io_error_is_too_many_open_files(io_error)
            } else {
                false
            }
        }
        _ => false,
    }
}

pub(crate) fn io_error_is_too_many_open_files(err: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(err.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
    }

    #[cfg(not(unix))]
    {
        false
    }
}

pub(crate) fn env_try_raise_nofile() -> bool {
    std::env::var("OBZENFLOW_TRY_RAISE_NOFILE")
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false)
}

#[cfg(unix)]
fn get_nofile_limit() -> io::Result<NofileLimit> {
    unsafe {
        let mut current = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(NofileLimit {
            soft: current.rlim_cur,
            hard: current.rlim_max,
        })
    }
}

#[cfg(not(unix))]
fn get_nofile_limit() -> io::Result<NofileLimit> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "RLIMIT_NOFILE is only available on unix",
    ))
}

#[cfg(unix)]
fn try_raise_nofile_soft_limit(desired_soft: u64) -> io::Result<NofileLimit> {
    unsafe {
        let current = get_nofile_limit()?;

        if current.soft >= desired_soft {
            return Ok(current);
        }

        let new_soft = std::cmp::min(desired_soft, current.hard);
        let updated = libc::rlimit {
            rlim_cur: new_soft as libc::rlim_t,
            rlim_max: current.hard as libc::rlim_t,
        };

        if libc::setrlimit(libc::RLIMIT_NOFILE, &updated) != 0 {
            return Err(io::Error::last_os_error());
        }

        get_nofile_limit()
    }
}

#[cfg(not(unix))]
fn try_raise_nofile_soft_limit(_desired_soft: u64) -> io::Result<NofileLimit> {
    get_nofile_limit()
}

pub(crate) fn preflight_nofile_for_disk_journals(
    estimate: DiskJournalFdEstimate,
    try_raise: bool,
) -> Result<Option<NofileLimit>, String> {
    let current = match get_nofile_limit() {
        Ok(limit) => limit,
        Err(_) => return Ok(None),
    };

    if current.soft >= estimate.estimated_fds {
        return Ok(Some(current));
    }

    if try_raise {
        match try_raise_nofile_soft_limit(estimate.estimated_fds) {
            Ok(raised) if raised.soft >= estimate.estimated_fds => return Ok(Some(raised)),
            Ok(raised) => {
                return Err(format!(
                    "Disk-journal pipeline requires ~{} file descriptors (stages={}, edges={}, metrics_enabled={}). RLIMIT_NOFILE soft={} hard={} (raised attempt reached soft={}). Increase your process file-descriptor limit (e.g. `ulimit -n {}`) or reduce pipeline size / disable metrics in obzenflow.toml.",
                    estimate.estimated_fds,
                    estimate.stages,
                    estimate.edges,
                    estimate.metrics_enabled,
                    current.soft,
                    current.hard,
                    raised.soft,
                    estimate.estimated_fds
                ));
            }
            Err(e) => {
                return Err(format!(
                    "Disk-journal pipeline requires ~{} file descriptors (stages={}, edges={}, metrics_enabled={}). RLIMIT_NOFILE soft={} hard={}. Attempt to raise soft limit failed: {}. Increase your process file-descriptor limit (e.g. `ulimit -n {}`) or reduce pipeline size / disable metrics in obzenflow.toml.",
                    estimate.estimated_fds,
                    estimate.stages,
                    estimate.edges,
                    estimate.metrics_enabled,
                    current.soft,
                    current.hard,
                    e,
                    estimate.estimated_fds
                ));
            }
        }
    }

    Err(format!(
        "Disk-journal pipeline requires ~{} file descriptors (stages={}, edges={}, metrics_enabled={}). RLIMIT_NOFILE soft={} hard={}. Increase your process file-descriptor limit (e.g. `ulimit -n {}`) or reduce pipeline size / disable metrics in obzenflow.toml.",
        estimate.estimated_fds,
        estimate.stages,
        estimate.edges,
        estimate.metrics_enabled,
        current.soft,
        current.hard,
        estimate.estimated_fds
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn detects_emfile_and_enfile_from_raw_os_error() {
        let emfile = io::Error::from_raw_os_error(libc::EMFILE);
        assert!(io_error_is_too_many_open_files(&emfile));

        let enfile = io::Error::from_raw_os_error(libc::ENFILE);
        assert!(io_error_is_too_many_open_files(&enfile));

        let eacces = io::Error::from_raw_os_error(libc::EACCES);
        assert!(!io_error_is_too_many_open_files(&eacces));
    }

    #[test]
    #[cfg(unix)]
    fn detects_too_many_open_files_inside_journal_error() {
        let err = JournalError::Implementation {
            message: "open failed".to_string(),
            source: Box::new(io::Error::from_raw_os_error(libc::EMFILE)),
        };
        assert!(journal_error_is_too_many_open_files(&err));
    }

    #[test]
    fn ignores_non_io_journal_errors() {
        let err = JournalError::Implementation {
            message: "open failed".to_string(),
            source: "not an io::Error".into(),
        };
        assert!(!journal_error_is_too_many_open_files(&err));
    }
}
