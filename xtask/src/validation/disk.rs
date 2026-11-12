// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use serde::Serialize;
use std::{
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

pub(super) fn directory_bytes(path: &Path) -> u64 {
    let mut pending = vec![path.to_owned()];
    let mut total = 0;
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            match entry.metadata() {
                Ok(metadata) if metadata.is_dir() => pending.push(entry.path()),
                Ok(metadata) => total += metadata.blocks() * 512,
                Err(_) => {}
            }
        }
    }
    total
}

pub(super) fn remove_directory(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Err(failure) if failure.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[derive(Debug, Serialize)]
pub(super) struct Removal {
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
    if held > budget {
        remove_all(unrequested_lanes, &mut held, &mut record.removed);
    }
    if held > budget {
        remove_all(
            &incremental_directories(target),
            &mut held,
            &mut record.removed,
        );
    }
    record
}

fn remove_all(directories: &[PathBuf], held: &mut u64, removed: &mut Vec<Removal>) {
    for directory in directories.iter().filter(|directory| directory.is_dir()) {
        let bytes = directory_bytes(directory);
        match fs::remove_dir_all(directory) {
            Ok(()) => {
                *held = held.saturating_sub(bytes);
                removed.push(Removal {
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
    let (incremental, lanes): (Vec<_>, Vec<_>) = record
        .removed
        .iter()
        .partition(|removal| removal.path.ends_with("incremental"));
    let mut parts: Vec<String> = lanes
        .iter()
        .map(|removal| {
            let path = removal
                .path
                .strip_prefix(&record.path)
                .unwrap_or(&removal.path);
            format!("{} ({})", path.display(), gigabytes(removal.bytes))
        })
        .collect();
    if !incremental.is_empty() {
        let bytes = incremental.iter().map(|removal| removal.bytes).sum();
        parts.push(format!("incremental state ({})", gigabytes(bytes)));
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
    if !incremental.is_empty() {
        lines.push("this run rebuilds the workspace crates without incremental state.".into());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target_with_incremental_state() -> tempfile::TempDir {
        let target = tempfile::tempdir().unwrap();
        for directory in [
            "debug/deps",
            "debug/incremental/core-1",
            "lanes/postgres/debug/deps",
        ] {
            fs::create_dir_all(target.path().join(directory)).unwrap();
        }
        fs::write(
            target
                .path()
                .join("debug/incremental/core-1/query-cache.bin"),
            vec![0; 64 * 1024],
        )
        .unwrap();
        target
    }

    #[test]
    fn over_budget_target_drops_incremental_state() {
        let target = target_with_incremental_state();
        let lane = target.path().join("lanes/postgres");
        let record = trim(target.path(), std::slice::from_ref(&lane), 1);
        assert!(!target.path().join("debug/incremental").exists());
        assert!(!lane.exists());
        assert!(target.path().join("debug/deps").is_dir());
        assert_eq!(record.removed.len(), 2);
        let lines = trim_lines(&record, 0);
        assert!(lines[0].starts_with("target holds "), "{lines:?}");
        assert!(lines[0].contains("Removed lanes/postgres (0.0 GB) and incremental state ("));
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
}
