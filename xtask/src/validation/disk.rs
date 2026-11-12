// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use serde::Serialize;
use std::{
    collections::HashMap,
    ffi::CString,
    fs, io,
    mem::MaybeUninit,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
};

pub(super) const GIGABYTE: u64 = 1_000_000_000;

pub(super) fn gigabytes(bytes: u64) -> String {
    format!("{:.1} GB", bytes as f64 / GIGABYTE as f64)
}

pub(super) fn free_bytes(path: &Path) -> Result<u64> {
    let held = path
        .ancestors()
        .find(|ancestor| ancestor.is_dir())
        .ok_or_else(|| error(format!("no directory holds {}", path.display())))?;
    let name = CString::new(held.as_os_str().as_bytes())?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(name.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let stat = unsafe { stat.assume_init() };
    Ok(product(stat.f_bavail, stat.f_frsize))
}

fn product(blocks: impl Into<u64>, block_size: impl Into<u64>) -> u64 {
    blocks.into().saturating_mul(block_size.into())
}

pub(super) fn floor_refusal(free: u64, minimum_gigabytes: u64) -> Option<String> {
    (free < minimum_gigabytes.saturating_mul(GIGABYTE)).then(|| {
        format!(
            "{} free, under the {minimum_gigabytes} GB floor",
            gigabytes(free)
        )
    })
}

struct Inode {
    bytes: u64,
    links: u64,
    links_inside: u64,
}

fn inodes(path: &Path) -> HashMap<(u64, u64), Inode> {
    let mut pending = vec![path.to_owned()];
    let mut found = HashMap::new();
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            match entry.metadata() {
                Ok(metadata) if metadata.is_dir() => pending.push(entry.path()),
                Ok(metadata) => {
                    found
                        .entry((metadata.dev(), metadata.ino()))
                        .or_insert(Inode {
                            bytes: metadata.blocks() * 512,
                            links: metadata.nlink(),
                            links_inside: 0,
                        })
                        .links_inside += 1;
                }
                Err(_) => {}
            }
        }
    }
    found
}

pub(super) fn directory_bytes(path: &Path) -> u64 {
    inodes(path).values().map(|inode| inode.bytes).sum()
}

fn freed_bytes(path: &Path) -> u64 {
    inodes(path)
        .values()
        .filter(|inode| inode.links_inside >= inode.links)
        .map(|inode| inode.bytes)
        .sum()
}

pub(super) fn remove_directory(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(failure) if failure.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RemovalKind {
    SupersededSessions,
    LaneDirectory,
    IncrementalState,
}

#[derive(Debug, Serialize)]
pub(super) struct Removal {
    pub(super) kind: RemovalKind,
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct BuildDirectoryRecord {
    pub(super) path: PathBuf,
    pub(super) bytes_at_start: u64,
    pub(super) removed: Vec<Removal>,
}

pub(super) fn measure(directory: &Path) -> BuildDirectoryRecord {
    BuildDirectoryRecord {
        path: directory.to_owned(),
        bytes_at_start: directory_bytes(directory),
        removed: Vec::new(),
    }
}

pub(super) fn trim(
    target: &Path,
    unrequested_lanes: &[PathBuf],
    budget: u64,
) -> BuildDirectoryRecord {
    let mut record = measure(target);
    let mut held = record.bytes_at_start;
    let incremental = incremental_directories(target);
    if held > budget {
        remove_superseded_sessions(&incremental, &mut held, &mut record.removed);
    }
    if held > budget {
        remove_all(
            RemovalKind::LaneDirectory,
            unrequested_lanes,
            &mut held,
            &mut record.removed,
        );
    }
    if held > budget {
        remove_all(
            RemovalKind::IncrementalState,
            &incremental,
            &mut held,
            &mut record.removed,
        );
    }
    record
}

fn remove_all(
    kind: RemovalKind,
    directories: &[PathBuf],
    held: &mut u64,
    removed: &mut Vec<Removal>,
) {
    for directory in directories.iter().filter(|directory| directory.is_dir()) {
        let bytes = freed_bytes(directory);
        match fs::remove_dir_all(directory) {
            Ok(()) => {
                *held = held.saturating_sub(bytes);
                removed.push(Removal {
                    kind,
                    path: directory.clone(),
                    bytes,
                });
            }
            Err(failure) => eprintln!(
                "validation: could not remove {}: {failure}",
                directory.display()
            ),
        }
    }
}

fn remove_superseded_sessions(incremental: &[PathBuf], held: &mut u64, removed: &mut Vec<Removal>) {
    for directory in incremental {
        let bytes: u64 = superseded_sessions(directory)
            .iter()
            .map(|(session, lock)| remove_session(session, lock))
            .sum();
        if bytes > 0 {
            *held = held.saturating_sub(bytes);
            removed.push(Removal {
                kind: RemovalKind::SupersededSessions,
                path: directory.clone(),
                bytes,
            });
        }
    }
}

fn superseded_sessions(incremental: &Path) -> Vec<(PathBuf, PathBuf)> {
    let Ok(units) = fs::read_dir(incremental) else {
        return Vec::new();
    };
    let mut superseded = Vec::new();
    for unit in units.flatten() {
        let Ok(entries) = fs::read_dir(unit.path()) else {
            continue;
        };
        let mut sessions: Vec<_> = entries
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| {
                let (timestamp, lock) = finalized_session(entry.file_name().to_str()?)?;
                Some((timestamp, entry.path(), unit.path().join(lock)))
            })
            .collect();
        sessions.sort();
        sessions.pop();
        superseded.extend(
            sessions
                .into_iter()
                .map(|(_, session, lock)| (session, lock)),
        );
    }
    superseded
}

fn finalized_session(name: &str) -> Option<(u64, String)> {
    let mut parts = name.strip_prefix("s-")?.split('-');
    let (timestamp, random, hash) = (parts.next()?, parts.next()?, parts.next()?);
    if hash == "working" || parts.next().is_some() {
        return None;
    }
    Some((
        u64::from_str_radix(timestamp, 36).ok()?,
        format!("s-{timestamp}-{random}.lock"),
    ))
}

fn remove_session(session: &Path, lock: &Path) -> u64 {
    let bytes = freed_bytes(session);
    if let Err(failure) = fs::remove_dir_all(session) {
        eprintln!(
            "validation: could not remove {}: {failure}",
            session.display()
        );
        return 0;
    }
    match fs::remove_file(lock) {
        Err(failure) if failure.kind() != io::ErrorKind::NotFound => {
            eprintln!("validation: could not remove {}: {failure}", lock.display());
        }
        _ => {}
    }
    bytes
}

fn incremental_directories(target: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![(target.to_owned(), 0)];
    while let Some((directory, depth)) = pending.pop() {
        if directory.join("deps").is_dir() && directory.join("incremental").is_dir() {
            found.push(directory.join("incremental"));
        }
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let descend = depth < 3
                && entry.file_type().is_ok_and(|kind| kind.is_dir())
                && !matches!(
                    entry.file_name().to_str(),
                    Some("test-runs" | "incremental" | "deps" | "build" | ".fingerprint")
                );
            if descend {
                pending.push((entry.path(), depth + 1));
            }
        }
    }
    found.sort();
    found
}

pub(super) fn trim_lines(record: &BuildDirectoryRecord, budget_gigabytes: u64) -> Vec<String> {
    if record.bytes_at_start <= budget_gigabytes.saturating_mul(GIGABYTE) {
        return Vec::new();
    }
    let of_kind = |kind: RemovalKind| {
        record
            .removed
            .iter()
            .filter(move |removal| removal.kind == kind)
    };
    let total = |kind: RemovalKind| of_kind(kind).map(|removal| removal.bytes).sum::<u64>();
    let mut parts = Vec::new();
    if of_kind(RemovalKind::SupersededSessions).next().is_some() {
        parts.push(format!(
            "superseded incremental sessions ({})",
            gigabytes(total(RemovalKind::SupersededSessions))
        ));
    }
    parts.extend(of_kind(RemovalKind::LaneDirectory).map(|removal| {
        let path = removal
            .path
            .strip_prefix(&record.path)
            .unwrap_or(&removal.path);
        format!("{} ({})", path.display(), gigabytes(removal.bytes))
    }));
    let dropped_incremental = of_kind(RemovalKind::IncrementalState).next().is_some();
    if dropped_incremental {
        parts.push(format!(
            "incremental state ({})",
            gigabytes(total(RemovalKind::IncrementalState))
        ));
    }
    let held = format!(
        "target holds {}, over its {budget_gigabytes} GB budget.",
        gigabytes(record.bytes_at_start)
    );
    let mut lines = vec![match parts.as_slice() {
        [] => format!("{held} Nothing in it can be removed."),
        [only] => format!("{held} Removed {only}."),
        [rest @ .., last] => format!("{held} Removed {} and {last}.", rest.join(", ")),
    }];
    if dropped_incremental {
        lines.push("this run rebuilds the workspace crates without incremental state.".into());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLDER_SESSION: &str = "s-ho2o94wfmm-0l95arp-au7v9xcnc6ypu0fphcydmwutd";
    const OLDER_LOCK: &str = "s-ho2o94wfmm-0l95arp.lock";
    const NEWER_SESSION: &str = "s-ho2ogdv0pc-01tzy5g-10fyz7wav0hibmh4grkwl6qeb";
    const NEWER_LOCK: &str = "s-ho2ogdv0pc-01tzy5g.lock";
    const WORKING_SESSION: &str = "s-ho2ogqk1a0-0b3xz9d-working";

    fn target_with_incremental_state() -> tempfile::TempDir {
        let target = tempfile::tempdir().unwrap();
        for directory in ["debug/deps", "lanes/postgres/debug/deps"] {
            fs::create_dir_all(target.path().join(directory)).unwrap();
        }
        let unit = target.path().join("debug/incremental/core-1");
        for session in [OLDER_SESSION, NEWER_SESSION, WORKING_SESSION] {
            fs::create_dir_all(unit.join(session)).unwrap();
            fs::write(
                unit.join(session).join("query-cache.bin"),
                vec![0; 64 * 1024],
            )
            .unwrap();
        }
        for lock in [OLDER_LOCK, NEWER_LOCK] {
            fs::write(unit.join(lock), []).unwrap();
        }
        target
    }

    fn kinds(record: &BuildDirectoryRecord) -> Vec<RemovalKind> {
        record.removed.iter().map(|removal| removal.kind).collect()
    }

    #[test]
    fn over_budget_target_drops_superseded_sessions() {
        let target = target_with_incremental_state();
        let unit = target.path().join("debug/incremental/core-1");
        let lane = target.path().join("lanes/postgres");
        let budget = directory_bytes(target.path()) - directory_bytes(&unit.join(OLDER_SESSION));
        let record = trim(target.path(), std::slice::from_ref(&lane), budget);
        assert!(!unit.join(OLDER_SESSION).exists());
        assert!(!unit.join(OLDER_LOCK).exists());
        assert!(unit.join(NEWER_SESSION).is_dir());
        assert!(unit.join(NEWER_LOCK).is_file());
        assert!(unit.join(WORKING_SESSION).is_dir());
        assert!(lane.is_dir());
        assert_eq!(kinds(&record), [RemovalKind::SupersededSessions]);
        let lines = trim_lines(&record, 0);
        assert!(
            lines[0].contains("Removed superseded incremental sessions ("),
            "{lines:?}"
        );
        assert_eq!(lines.len(), 1, "{lines:?}");
    }

    #[test]
    fn over_budget_target_drops_incremental_state() {
        let target = target_with_incremental_state();
        let lane = target.path().join("lanes/postgres");
        let record = trim(target.path(), std::slice::from_ref(&lane), 1);
        assert!(!target.path().join("debug/incremental").exists());
        assert!(!lane.exists());
        assert!(target.path().join("debug/deps").is_dir());
        assert_eq!(
            kinds(&record),
            [
                RemovalKind::SupersededSessions,
                RemovalKind::LaneDirectory,
                RemovalKind::IncrementalState
            ]
        );
        let lines = trim_lines(&record, 0);
        assert!(lines[0].starts_with("target holds "), "{lines:?}");
        assert!(
            lines[0].contains(
                "Removed superseded incremental sessions (0.0 GB), lanes/postgres (0.0 GB) and \
                 incremental state ("
            ),
            "{lines:?}"
        );
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn under_budget_target_keeps_incremental_state() {
        let target = target_with_incremental_state();
        let lane = target.path().join("lanes/postgres");
        let record = trim(target.path(), std::slice::from_ref(&lane), u64::MAX);
        assert!(target.path().join("debug/incremental/core-1").is_dir());
        assert!(lane.is_dir());
        assert!(record.removed.is_empty());
        assert!(trim_lines(&record, u64::MAX / GIGABYTE).is_empty());
    }

    #[test]
    fn hard_linked_file_counted_once() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("artifact"), vec![0; 64 * 1024]).unwrap();
        let single = directory_bytes(directory.path());
        fs::hard_link(
            directory.path().join("artifact"),
            directory.path().join("uplifted"),
        )
        .unwrap();
        assert_eq!(directory_bytes(directory.path()), single);
    }

    #[test]
    fn superseded_removal_reports_unshared_bytes() {
        let target = target_with_incremental_state();
        let unit = target.path().join("debug/incremental/core-1");
        let shared = unit.join(NEWER_SESSION).join("shared.o");
        fs::write(&shared, vec![0; 128 * 1024]).unwrap();
        fs::hard_link(&shared, unit.join(OLDER_SESSION).join("shared.o")).unwrap();
        let unshared = fs::metadata(unit.join(OLDER_SESSION).join("query-cache.bin"))
            .unwrap()
            .blocks()
            * 512;
        let budget = directory_bytes(target.path()) - unshared;
        let record = trim(target.path(), &[], budget);
        assert!(!unit.join(OLDER_SESSION).exists());
        assert!(shared.is_file());
        assert_eq!(kinds(&record), [RemovalKind::SupersededSessions]);
        assert_eq!(record.removed[0].bytes, unshared);
    }
}
