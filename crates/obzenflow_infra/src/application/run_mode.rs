// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120i: run-scoped provenance for banners and completion prompts.
//!
//! `RunMode` is the run-scoped half of the two-layer provenance surface. It
//! feeds startup and completion copy only: banners (`Presentation::for_mode`)
//! and footers (`RunPresentationOutcome`). Per-event labelling never reads it;
//! a sink delivery's provenance derives from `event.replay_context` through
//! the runtime's `DeliveryContext`, which is what keeps labels honest when
//! FLOWIP-120n's resume mixes a replayed prefix with a live tail in one run.
//!
//! The enum is non-exhaustive on purpose: FLOWIP-120n adds a `Resume` variant,
//! and the catch-up-to-live transition is announced on the `ReplayLifecycle`
//! system-event channel rather than by mutating this value mid-run.

use obzenflow_core::journal::run_manifest::{RunManifest, RUN_MANIFEST_FILENAME};
use std::path::{Path, PathBuf};

/// How this process is executing the flow, resolved once at startup.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RunMode {
    /// Ordinary live execution.
    Live,
    /// Strict replay of a recorded run (`--replay-from`).
    Replay(ReplayRunContext),
    /// Resume of a recorded run (`--resume-from`): catch up on the archive,
    /// then continue live from the recorded high-water mark (FLOWIP-120n).
    Resume(ReplayRunContext),
}

/// The replay run's source archive, for user-facing copy.
#[derive(Debug, Clone)]
pub struct ReplayRunContext {
    /// The archive directory `--replay-from` named.
    pub archive_path: PathBuf,
    /// The recorded run's flow id, when the archive manifest is readable.
    pub archive_flow_id: Option<String>,
}

impl RunMode {
    pub fn is_replay(&self) -> bool {
        matches!(self, RunMode::Replay(_))
    }

    pub(crate) fn replay_from_archive(archive_path: PathBuf) -> Self {
        let archive_flow_id = peek_archive_flow_id(&archive_path);
        RunMode::Replay(ReplayRunContext {
            archive_path,
            archive_flow_id,
        })
    }

    pub(crate) fn resume_from_archive(archive_path: PathBuf) -> Self {
        let archive_flow_id = peek_archive_flow_id(&archive_path);
        RunMode::Resume(ReplayRunContext {
            archive_path,
            archive_flow_id,
        })
    }
}

impl ReplayRunContext {
    /// The source archive named for humans: the recorded flow id when the
    /// manifest is readable, always the archive path.
    pub fn source_label(&self) -> String {
        match &self.archive_flow_id {
            Some(flow_id) => format!("{flow_id} ({})", self.archive_path.display()),
            None => self.archive_path.display().to_string(),
        }
    }
}

/// Best-effort manifest peek for banner copy. Archive validation, including
/// `allow_incomplete_archive` semantics, stays with the replay archive open
/// path; an unreadable manifest here only costs the flow id in the copy.
fn peek_archive_flow_id(archive_path: &Path) -> Option<String> {
    let manifest = std::fs::read_to_string(archive_path.join(RUN_MANIFEST_FILENAME)).ok()?;
    let manifest: RunManifest = serde_json::from_str(&manifest).ok()?;
    Some(manifest.flow_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_from_archive_tolerates_missing_manifest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mode = RunMode::replay_from_archive(temp.path().to_path_buf());
        match mode {
            RunMode::Replay(ctx) => {
                assert!(ctx.archive_flow_id.is_none());
                assert_eq!(ctx.archive_path, temp.path());
                assert_eq!(ctx.source_label(), temp.path().display().to_string());
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
            }
            other => panic!("expected resume mode, got {other:?}"),
        }
    }
}
