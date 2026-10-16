// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::journal::disk::inspect::load_manifest;
use crate::journal::disk::replay_archive::DiskReplayArchive;
use crate::journal::read::{
    compare_with_runs, read_configuration_refusals, read_logic_version_changes, ConfigChanges,
    LogicVersionChange, RunConfigChanges,
};
use obzenflow_core::journal::archive_actions::{
    decide, ArchiveEvidence, ArchiveOverrides, FailureEvidence, FlowEvidence, ReplayVerb,
};
use obzenflow_core::journal::factory::CurrentRunLocator;
use obzenflow_runtime::pipeline::FlowHandle;
use obzenflow_runtime::runtime_config::app_leaves;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub(super) fn settings_lines(flow: &FlowHandle, origin: Option<&Path>) -> Vec<String> {
    flow.flow_effective_config()
        .map(|effective| render_settings(effective.app_settings(), origin))
        .unwrap_or_default()
}

fn render_settings(app: Option<&Value>, origin: Option<&Path>) -> Vec<String> {
    let Some(app) = app.filter(|app| !app.as_object().is_some_and(|table| table.is_empty())) else {
        return Vec::new();
    };
    let heading = match origin {
        Some(origin) => format!("Application settings from {}:", origin.display()),
        None => "Application settings (defaults; no [app] table):".to_string(),
    };
    std::iter::once(heading)
        .chain(
            app_leaves(Some(app))
                .into_iter()
                .map(|(key, value)| format!("  {key} = {value}")),
        )
        .collect()
}

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

pub(super) async fn resume_remedy(
    flow: &FlowHandle,
    locator: &CurrentRunLocator,
) -> Option<String> {
    let run = locator.path();
    let resumed_from = load_manifest(run).ok()?.resume?.resumed_from;
    let evidence = DiskReplayArchive::written_run_evidence(run.to_path_buf(), ReplayVerb::Resume)
        .await
        .ok()?;
    remedy_line(
        &evidence,
        flow.archive_flow_evidence(),
        &resumed_from,
        run,
        &locator.to_string(),
    )
}

fn remedy_line(
    evidence: &ArchiveEvidence,
    flow: &FlowEvidence,
    resumed_from: &Path,
    run: &Path,
    shown: &str,
) -> Option<String> {
    let admitted = evidence.failure == FailureEvidence::DepartedFromArchive
        && decide(
            ReplayVerb::Resume,
            run,
            evidence,
            flow,
            ArchiveOverrides::default(),
        )
        .is_ok();
    admitted.then(|| {
        format!(
            "To continue this run, restore what {} recorded, then add: --resume-from {shown}",
            resumed_from.display()
        )
    })
}

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
    fn settings_print_each_recorded_leaf_under_their_file() {
        let recorded = json!({
            "accounts_file": "accounts.yaml",
            "partner": { "api_key_env": "PARTNER_API_KEY", "retries": 3 },
            "regions": ["ca", "us"],
        });
        assert_eq!(
            render_settings(Some(&recorded), Some(Path::new("deploy/staging.toml"))),
            [
                "Application settings from deploy/staging.toml:",
                "  app.accounts_file = \"accounts.yaml\"",
                "  app.partner.api_key_env = \"PARTNER_API_KEY\"",
                "  app.partner.retries = 3",
                "  app.regions = [\"ca\",\"us\"]",
            ]
        );
        assert_eq!(
            render_settings(Some(&json!({ "mode": "mock" })), None),
            [
                "Application settings (defaults; no [app] table):",
                "  app.mode = \"mock\"",
            ]
        );
        assert!(render_settings(None, None).is_empty(), "nothing extracted");
        assert!(
            render_settings(Some(&json!({})), Some(Path::new("shared.toml"))).is_empty(),
            "a type with no settings prints nothing"
        );
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

    #[test]
    fn the_remedy_names_the_plain_resume_only_after_a_departure_core_admits() {
        use obzenflow_core::journal::ArchiveStatus;
        let failed = |failure| ArchiveEvidence {
            status: ArchiveStatus::Failed,
            live_writer: false,
            sources: Vec::new(),
            failure,
        };
        let line = |evidence: &ArchiveEvidence, flow: &FlowEvidence| {
            remedy_line(
                evidence,
                flow,
                Path::new("/runs/killed"),
                Path::new("/runs/refused"),
                "runs/refused",
            )
        };
        let flow = FlowEvidence::default();
        assert_eq!(
            line(&failed(FailureEvidence::DepartedFromArchive), &flow).as_deref(),
            Some(
                "To continue this run, restore what /runs/killed recorded, then add: \
                 --resume-from runs/refused"
            )
        );
        assert_eq!(line(&failed(FailureEvidence::Other), &flow), None);
        let cyclic = FlowEvidence {
            cycle_members: vec!["triage".to_string()],
            ..FlowEvidence::default()
        };
        assert_eq!(
            line(&failed(FailureEvidence::DepartedFromArchive), &cyclic),
            None,
            "a command Core refuses is never offered"
        );
    }
}
