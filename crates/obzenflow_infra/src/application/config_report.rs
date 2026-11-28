// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::presentation::shell_path;
use crate::journal::disk::inspect::load_manifest;
use crate::journal::disk::replay_archive::DiskReplayArchive;
use crate::journal::read::{
    compare_with_runs, read_configuration_refusals, read_logic_version_changes, read_stage_stops,
    ConfigChanges, LogicVersionChange, RunConfigChanges,
};
use obzenflow_core::event::StageFatalReason;
use obzenflow_core::journal::archive_actions::{
    decide, ArchiveEvidence, ArchiveOverrides, ArchiveWarning, FailureEvidence, FlowEvidence,
    ReplayVerb,
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
    let manifest = load_manifest(run).ok()?;
    if crate::journal::disk::replay_archive::run_link(&manifest).is_some() {
        return None;
    }
    let resumed_from = manifest.resume.map(|resume| resume.resumed_from);
    let evidence = DiskReplayArchive::written_run_evidence(run.to_path_buf(), ReplayVerb::Resume)
        .await
        .ok()?;
    let stops = read_stage_stops(run).await.unwrap_or_default();
    remedy_line(
        &evidence,
        flow.archive_flow_evidence(),
        resumed_from.as_deref(),
        run,
        &stops,
    )
}

fn remedy_line(
    evidence: &ArchiveEvidence,
    flow: &FlowEvidence,
    resumed_from: Option<&Path>,
    run: &Path,
    stops: &[(String, StageFatalReason)],
) -> Option<String> {
    let command = shell_path(run);
    match decide(
        ReplayVerb::Resume,
        run,
        evidence,
        flow,
        ArchiveOverrides::default(),
        &[],
    ) {
        Ok(Some(ArchiveWarning::Failed { .. })) => Some(format!(
            "To continue this run, remove the cause of the failure. Then add --resume-from \
             {command}."
        )),
        Ok(None) if evidence.failure == FailureEvidence::DepartedFromArchive => {
            resumed_from.map(|parent| {
                format!(
                    "To continue this run, restore what {} recorded{}. Then add --resume-from \
                     {command}.",
                    parent.display(),
                    rebuild_clause(evidence, flow, run, stops)
                )
            })
        }
        Ok(None) | Err(_) => None,
    }
}

fn rebuild_clause(
    evidence: &ArchiveEvidence,
    flow: &FlowEvidence,
    run: &Path,
    stops: &[(String, StageFatalReason)],
) -> String {
    let mut stages: Vec<String> = stops
        .iter()
        .filter(|(_, reason)| *reason == StageFatalReason::DestinationChanged)
        .map(|(stage, _)| stage.clone())
        .collect();
    stages.sort();
    stages.dedup();
    if stages.is_empty()
        || decide(
            ReplayVerb::Rebuild,
            run,
            evidence,
            flow,
            ArchiveOverrides::default(),
            &stages,
        )
        .is_err()
    {
        return String::new();
    }
    let flags: Vec<String> = stages
        .iter()
        .map(|stage| format!("--stage {stage}"))
        .collect();
    format!(
        ", or rebuild {} with --rebuild-from {} {}",
        and_list(&stages),
        shell_path(run),
        flags.join(" ")
    )
}

fn and_list(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

pub(super) fn rebuild_stop_lines(
    stops: &[(String, StageFatalReason)],
    named: &[String],
) -> Vec<String> {
    let mut lines = Vec::new();
    for (stage, reason) in stops.iter().filter(|(stage, _)| named.contains(stage)) {
        if reason.departs_from_archive() {
            lines.push(
                "The destination holds only what this rebuild wrote before it stopped.".to_string(),
            );
            lines.push(format!(
                "Rebuild {stage} with the build and configuration that recorded this call."
            ));
        } else if *reason == StageFatalReason::DestinationUnwritable {
            lines.push(format!(
                "Fix what made it fail. Then rebuild {stage} again."
            ));
        }
    }
    lines
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
    fn remedy_line_admitted_resume_names_command() {
        use obzenflow_core::journal::ArchiveStatus;
        let failed = |failure| ArchiveEvidence {
            status: ArchiveStatus::Failed,
            live_writer: false,
            sources: Vec::new(),
            failure,
            sinks: Vec::new(),
            guarantees: Vec::new(),
        };
        let line = |evidence: &ArchiveEvidence, flow: &FlowEvidence, parent: Option<&str>| {
            remedy_line(
                evidence,
                flow,
                parent.map(Path::new),
                Path::new("runs/refused"),
                &[],
            )
        };
        let flow = FlowEvidence::default();
        assert_eq!(
            line(
                &failed(FailureEvidence::DepartedFromArchive),
                &flow,
                Some("/runs/killed")
            )
            .as_deref(),
            Some(
                "To continue this run, restore what /runs/killed recorded. Then add \
                 --resume-from runs/refused."
            )
        );
        for parent in [Some("/runs/killed"), None] {
            assert_eq!(
                line(&failed(FailureEvidence::Other), &flow, parent).as_deref(),
                Some(
                    "To continue this run, remove the cause of the failure. Then add \
                     --resume-from runs/refused."
                )
            );
        }
        let cyclic = FlowEvidence {
            cycle_members: vec!["triage".to_string()],
            ..FlowEvidence::default()
        };
        for failure in [FailureEvidence::DepartedFromArchive, FailureEvidence::Other] {
            assert_eq!(
                line(&failed(failure), &cyclic, Some("/runs/killed")),
                None,
                "a command Core refuses is never offered"
            );
        }
    }

    fn departed_register_run() -> (ArchiveEvidence, FlowEvidence) {
        use obzenflow_core::journal::archive::EffectGuarantee;
        use obzenflow_core::journal::ArchiveStatus;
        let guarantees = vec![
            (
                "case_register".to_string(),
                vec![EffectGuarantee::ExactlyOnce],
            ),
            (
                "totals_report".to_string(),
                vec![EffectGuarantee::AtLeastOnce],
            ),
            (
                "unopened_alerts".to_string(),
                vec![EffectGuarantee::ExactlyOnce],
            ),
        ];
        let evidence = ArchiveEvidence {
            status: ArchiveStatus::Failed,
            live_writer: false,
            sources: Vec::new(),
            failure: FailureEvidence::DepartedFromArchive,
            sinks: vec![
                "case_register".to_string(),
                "totals_report".to_string(),
                "unopened_alerts".to_string(),
            ],
            guarantees: guarantees.clone(),
        };
        let flow = FlowEvidence {
            guarantees,
            ..FlowEvidence::default()
        };
        (evidence, flow)
    }

    fn departed_line(stops: &[(&str, StageFatalReason)]) -> Option<String> {
        let (evidence, flow) = departed_register_run();
        let stops: Vec<(String, StageFatalReason)> = stops
            .iter()
            .map(|(stage, reason)| (stage.to_string(), *reason))
            .collect();
        remedy_line(
            &evidence,
            &flow,
            Some(Path::new("/runs/killed")),
            Path::new("runs/refused"),
            &stops,
        )
    }

    #[test]
    fn rebuild_remedy_two_sinks_lists_stages() {
        assert_eq!(
            departed_line(&[
                ("unopened_alerts", StageFatalReason::DestinationChanged),
                ("case_register", StageFatalReason::DestinationChanged),
            ])
            .as_deref(),
            Some(
                "To continue this run, restore what /runs/killed recorded, or rebuild \
                 case_register and unopened_alerts with --rebuild-from runs/refused --stage \
                 case_register --stage unopened_alerts. Then add --resume-from runs/refused."
            )
        );
    }

    #[test]
    fn remedy_at_least_once_stage_omits_rebuild() {
        assert_eq!(
            departed_line(&[("totals_report", StageFatalReason::DestinationChanged)]).as_deref(),
            Some(
                "To continue this run, restore what /runs/killed recorded. Then add \
                 --resume-from runs/refused."
            ),
            "a rebuild that decide refuses is never offered"
        );
    }

    #[test]
    fn rebuild_clause_requires_destination_changed() {
        for reason in [
            StageFatalReason::DescriptorMismatch,
            StageFatalReason::MissingRecordedEffect,
            StageFatalReason::UnusedEffectHistory,
            StageFatalReason::MissingRecordedFact,
            StageFatalReason::UnusedFactHistory,
            StageFatalReason::DestinationChanged,
        ] {
            assert!(reason.departs_from_archive());
            let line = departed_line(&[("case_register", reason)]).unwrap();
            assert_eq!(
                line.contains("--rebuild-from"),
                reason == StageFatalReason::DestinationChanged,
                "{reason:?}: {line}"
            );
        }
    }

    #[test]
    fn rebuild_unused_history_prints_stop_lines() {
        let named = ["case_register".to_string()];
        assert_eq!(
            rebuild_stop_lines(
                &[(
                    "case_register".to_string(),
                    StageFatalReason::UnusedEffectHistory
                )],
                &named
            ),
            [
                "The destination holds only what this rebuild wrote before it stopped.",
                "Rebuild case_register with the build and configuration that recorded this call.",
            ]
        );
        assert!(
            rebuild_stop_lines(
                &[("score".to_string(), StageFatalReason::UnusedEffectHistory)],
                &named
            )
            .is_empty(),
            "a stage outside the named sinks prints no rebuild line"
        );
    }

    #[test]
    fn unwritable_rebuild_stop_omits_departure_lines() {
        assert_eq!(
            rebuild_stop_lines(
                &[(
                    "case_register".to_string(),
                    StageFatalReason::DestinationUnwritable
                )],
                &["case_register".to_string()]
            ),
            ["Fix what made it fail. Then rebuild case_register again."]
        );
    }
}
