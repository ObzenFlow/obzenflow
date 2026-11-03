// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_runtime::bootstrap::{BootstrapConfig, ReplayBootstrap, ReplayVerb};
use std::sync::Arc;

/// FLOWIP-120u: the host opens the replay/resume input archive and the
/// bootstrap snapshot carries it. Tests install through this helper so the
/// build actually enters replay/resume instead of silently running live.
pub async fn bootstrap_with_archive(replay: ReplayBootstrap) -> BootstrapConfig {
    let archive = match replay.verb {
        ReplayVerb::Resume => {
            DiskReplayArchive::open_for_resume(
                replay.archive_path.clone(),
                replay.allow_incomplete_archive,
            )
            .await
        }
        ReplayVerb::Replay => {
            DiskReplayArchive::open(replay.archive_path.clone(), replay.allow_incomplete_archive)
                .await
        }
    }
    .expect("test replay archive must open");
    BootstrapConfig {
        replay: Some(replay),
        archive_evidence: Some(archive.evidence()),
        replay_archive: Some(Arc::new(archive)),
        ..Default::default()
    }
}
