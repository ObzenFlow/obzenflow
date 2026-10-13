// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Start-up and failure-footer lines for a replay or resume whose
//! configuration differs from the runs it rebuilds (FLOWIP-010t B6, B9, B12).
//! Projections over the manifests and journalled refusals; they change nothing.

use crate::journal::read::{
    compare_with_runs, read_configuration_refusals, read_logic_version_changes, ConfigChanges,
    LogicVersionChange, RunConfigChanges,
};
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

/// The failure footer after a refusal a changed configuration value can cause
/// (FLOWIP-010t B12); empty after any other failure.
pub(super) async fn refusal_footer(run_dir: &Path, changes: &[RunConfigChanges]) -> Vec<String> {
    let refused = match read_configuration_refusals(run_dir).await {
        Ok(refused) => refused,
        Err(error) => {
            tracing::warn!(
                run = %run_dir.display(),
                %error,
                "could not read the failed run's refusals"
            );
            return Vec::new();
        }
    };
    if refused.is_empty() {
        return Vec::new();
    }
    let runs: Vec<PathBuf> = changes
        .iter()
        .map(|compared| compared.run.clone())
        .collect();
    footer_lines(
        changes,
        &read_logic_version_changes(run_dir, &runs, &refused),
    )
}

/// The changed keys, then each refused stage whose logic version changed.
fn footer_lines(changes: &[RunConfigChanges], logic: &[LogicVersionChange]) -> Vec<String> {
    let mut keys: Vec<&str> = changes
        .iter()
        .flat_map(|compared| compared.changes.keys())
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let changed_keys = (!keys.is_empty()).then(|| {
        format!(
            "The recorded run used different configuration for: {}",
            keys.join(", ")
        )
    });
    changed_keys
        .into_iter()
        .chain(logic.iter().map(|change| {
            format!(
                "Stage '{}' changed its logic version since {}: {} -> {}",
                change.stage,
                change.run.display(),
                change.recorded,
                change.current
            )
        }))
        .collect()
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
    }

    #[test]
    fn the_footer_names_changed_keys_then_changed_logic_versions() {
        let changes = [
            changed("/runs/torn", &["app.mode", "app.affinity.threshold"]),
            changed("/runs/root", &["app.mode"]),
        ];
        let logic = [LogicVersionChange {
            run: PathBuf::from("/runs/root"),
            stage: "score".to_string(),
            recorded: "scorer-v1".to_string(),
            current: "scorer-v2".to_string(),
        }];
        assert_eq!(
            footer_lines(&changes, &logic),
            [
                "The recorded run used different configuration for: app.affinity.threshold, app.mode",
                "Stage 'score' changed its logic version since /runs/root: scorer-v1 -> scorer-v2",
            ]
        );
        assert_eq!(
            footer_lines(&[changed("/runs/r", &[])], &logic),
            ["Stage 'score' changed its logic version since /runs/root: scorer-v1 -> scorer-v2"],
            "a code change alone names no configuration"
        );
        assert!(footer_lines(&[changed("/runs/r", &[])], &[]).is_empty());
    }
}
