// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Start-up and failure-footer lines for a replay or resume whose
//! configuration differs from the runs it rebuilds (FLOWIP-010t B6, B9).
//! Projections over the manifests' config evidence; they change nothing.

use crate::journal::read::{
    compare_with_runs, open_disk_run, ConfigChanges, JournalReadError, RunConfigChanges,
};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainPayload, StageFatalReason};
use obzenflow_core::journal::read::RunRecordData;
use obzenflow_runtime::pipeline::FlowHandle;
use std::path::{Path, PathBuf};

/// This run's configuration against the archive it reads and, for a resume,
/// every ancestor it merges: the runs its manifest links.
pub(super) fn rebuilt_config_changes(flow: &FlowHandle) -> Vec<RunConfigChanges> {
    let (Some(archive), Some(effective)) = (
        obzenflow_runtime::bootstrap::replay_archive(),
        flow.flow_effective_config(),
    ) else {
        return Vec::new();
    };
    let runs: Vec<PathBuf> = std::iter::once(archive.archive_path().to_path_buf())
        .chain(
            archive
                .ancestors()
                .iter()
                .map(|ancestor| ancestor.archive_path().to_path_buf()),
        )
        .collect();
    compare_with_runs(&effective.manifest_evidence(), &runs)
}

/// One line per compared run whose configuration differs or is unknown.
pub(super) fn startup_lines(changes: &[RunConfigChanges]) -> Vec<String> {
    changes
        .iter()
        .filter_map(|compared| {
            let run = compared.run.display();
            match &compared.changes {
                ConfigChanges::Recorded(differences) if differences.is_empty() => None,
                ConfigChanges::Recorded(_) => Some(format!(
                    "Configuration differs from {run}: {}",
                    compared.changes.keys().join(", ")
                )),
                ConfigChanges::NotRecorded => {
                    Some(format!("Configuration of {run} was not recorded"))
                }
                ConfigChanges::Unreadable(error) => {
                    Some(format!("Configuration of {run} could not be read: {error}"))
                }
            }
        })
        .collect()
}

/// Whether a stage of the run refused to rebuild its recorded history, as a
/// changed effect identity does.
pub(super) async fn refused_by_divergence(run_dir: &Path) -> bool {
    let divergence = async {
        let mut snapshot = open_disk_run(run_dir).await?;
        while let Some(record) = snapshot.next().await? {
            if let RunRecordData::Chain(row) = &record.record {
                if let ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) =
                    &row.payload
                {
                    if fatal.reason == StageFatalReason::ReplayDivergence {
                        return Ok(true);
                    }
                }
            }
        }
        Ok::<_, JournalReadError>(false)
    };
    divergence.await.unwrap_or_else(|error| {
        tracing::warn!(
            run = %run_dir.display(),
            %error,
            "could not read the failed run's stage-fatal facts"
        );
        false
    })
}

/// The keys to revert after a stage refused a changed effect identity.
pub(super) fn descriptor_mismatch_line(changes: &[RunConfigChanges]) -> Option<String> {
    let mut keys: Vec<&str> = changes
        .iter()
        .flat_map(|compared| compared.changes.keys())
        .collect();
    keys.sort_unstable();
    keys.dedup();
    (!keys.is_empty()).then(|| {
        format!(
            "The recorded run used different configuration for: {}",
            keys.join(", ")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::runtime_config::ConfigDifference;
    use serde_json::json;

    fn changed(run: &str, keys: &[&str]) -> RunConfigChanges {
        RunConfigChanges {
            run: PathBuf::from(run),
            changes: ConfigChanges::Recorded(
                keys.iter()
                    .map(|key| ConfigDifference {
                        key: key.to_string(),
                        point: None,
                        recorded: Some(json!(1)),
                        current: Some(json!(2)),
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn startup_names_each_differing_run_once_and_skips_identical_ones() {
        let changes = [
            changed("/runs/torn", &["app.affinity.threshold", "app.mode"]),
            changed("/runs/root", &[]),
            RunConfigChanges {
                run: PathBuf::from("/runs/old"),
                changes: ConfigChanges::NotRecorded,
            },
        ];
        assert_eq!(
            startup_lines(&changes),
            [
                "Configuration differs from /runs/torn: app.affinity.threshold, app.mode",
                "Configuration of /runs/old was not recorded",
            ]
        );
        assert_eq!(
            descriptor_mismatch_line(&changes).unwrap(),
            "The recorded run used different configuration for: app.affinity.threshold, app.mode"
        );
        assert_eq!(descriptor_mismatch_line(&[changed("/runs/r", &[])]), None);
    }
}
