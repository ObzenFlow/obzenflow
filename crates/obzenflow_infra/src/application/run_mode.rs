// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::journal::archive::manifest::RUN_MANIFEST_FILENAME;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub(crate) enum RunMode {
    Live,
    Replay(ReplayRunContext),
    Resume(ReplayRunContext),
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayRunContext {
    pub archive_path: PathBuf,
    pub archive_flow_id: Option<String>,
    pub verify: bool,
}

impl RunMode {
    pub(crate) fn replay_from_archive(archive_path: PathBuf, verify: bool) -> Self {
        let archive_flow_id = peek_archive_flow_id(&archive_path);
        RunMode::Replay(ReplayRunContext {
            archive_path,
            archive_flow_id,
            verify,
        })
    }

    pub(crate) fn resume_from_archive(archive_path: PathBuf) -> Self {
        let archive_flow_id = peek_archive_flow_id(&archive_path);
        RunMode::Resume(ReplayRunContext {
            archive_path,
            archive_flow_id,
            verify: false,
        })
    }
}

impl ReplayRunContext {
    pub(crate) fn source_label(&self) -> String {
        match &self.archive_flow_id {
            Some(flow_id) => format!("{flow_id} at {}", self.archive_path.display()),
            None => self.archive_path.display().to_string(),
        }
    }
}

fn peek_archive_flow_id(archive_path: &Path) -> Option<String> {
    let manifest = std::fs::read_to_string(archive_path.join(RUN_MANIFEST_FILENAME)).ok()?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest).ok()?;
    crate::journal::disk::manifest_gate::require_current_journal_schema_version(&manifest).ok()?;
    manifest.get("flow_id")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_from_archive_tolerates_missing_manifest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mode = RunMode::replay_from_archive(temp.path().to_path_buf(), true);
        match mode {
            RunMode::Replay(ctx) => {
                assert!(ctx.archive_flow_id.is_none());
                assert_eq!(ctx.archive_path, temp.path());
                assert_eq!(ctx.source_label(), temp.path().display().to_string());
                assert!(ctx.verify);
            }
            other => panic!("expected replay mode, got {other:?}"),
        }
    }

    #[test]
    fn resume_from_archive_carries_the_same_context_shape() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mode = RunMode::resume_from_archive(temp.path().to_path_buf());
        match mode {
            RunMode::Resume(ctx) => {
                assert!(ctx.archive_flow_id.is_none());
                assert_eq!(ctx.archive_path, temp.path());
                assert!(!ctx.verify, "a resume continues live and is never verified");
            }
            other => panic!("expected resume mode, got {other:?}"),
        }
    }

    #[test]
    fn banner_peek_does_not_deserialize_an_old_manifest() {
        let temp = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            temp.path().join(RUN_MANIFEST_FILENAME),
            serde_json::to_vec(&serde_json::json!({
                "journal_schema_version": "2.0",
                "flow_id": "old-flow-must-not-leak-through"
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(peek_archive_flow_id(temp.path()).is_none());
    }
}
