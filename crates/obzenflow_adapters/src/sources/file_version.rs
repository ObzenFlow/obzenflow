// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Kernel-maintained file versions (FLOWIP-134i B10).
//!
//! A position indexes one version of a file's bytes. File connectors pin the
//! open file's version in their input identity and re-check it on the same
//! open file before publishing anything read from it, so an append, an edit,
//! a truncation, an `mv` replacement or an edit that restores the
//! modification time fails closed instead of misplacing a position.

use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_runtime::stages::source::SourceError;
use serde::Serialize;
use std::fs::File;

/// One `fstat` of an open file. On Unix the change time, which users cannot
/// set, moves on every content or metadata change; other platforms fall back
/// to the modification and creation times and are unsupported for stability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct FileVersion {
    inode: u64,
    size: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl FileVersion {
    pub(crate) fn of(file: &File) -> Result<Self, SourceError> {
        let metadata = file
            .metadata()
            .map_err(|_| SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into()))?;
        Ok(Self::from_metadata(&metadata))
    }

    /// Fail the read when the open file is no longer the pinned version.
    pub(crate) fn verify(&self, file: &File) -> Result<(), SourceError> {
        if Self::of(file)? == *self {
            Ok(())
        } else {
            Err(input_changed())
        }
    }

    #[cfg(unix)]
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            inode: metadata.ino(),
            size: metadata.size(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }

    #[cfg(not(unix))]
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        let instant = |time: std::io::Result<std::time::SystemTime>| {
            time.ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or((0, 0), |since| {
                    (
                        i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
                        i64::from(since.subsec_nanos()),
                    )
                })
        };
        Self {
            inode: 0,
            size: metadata.len(),
            modified: instant(metadata.modified()),
            changed: instant(metadata.created()),
        }
    }
}

/// The source failure for an input that changed while it was read.
pub(crate) fn input_changed() -> SourceError {
    SourceError::Terminal {
        kind: SourcePollErrorKind::Transport,
        diagnostic: SourceDiagnosticReason::InputChanged.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn an_append_or_replacement_changes_the_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.csv");
        std::fs::write(&path, "a\n1\n").unwrap();
        let file = File::open(&path).unwrap();
        let pinned = FileVersion::of(&file).unwrap();
        assert!(pinned.verify(&file).is_ok());

        let mut appender = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(appender, "2").unwrap();
        assert!(matches!(
            pinned.verify(&file),
            Err(SourceError::Terminal { .. })
        ));

        let replacement = dir.path().join("replacement.csv");
        std::fs::write(&replacement, "a\n1\n").unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        let reopened = File::open(&path).unwrap();
        assert_ne!(FileVersion::of(&reopened).unwrap(), pinned);
    }
}
