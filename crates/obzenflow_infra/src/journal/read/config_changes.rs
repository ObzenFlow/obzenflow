// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{open_disk_run, JournalReadError};
use crate::journal::disk::inspect::load_manifest;
use obzenflow_core::config::EffectiveConfigEvidence;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::archive::manifest::RunManifestStage;
use obzenflow_core::journal::archive_actions::departs_from_archive;
use obzenflow_core::journal::read::{RunRecordData, RunStage};
use obzenflow_core::journal::RunManifest;
use obzenflow_runtime::runtime_config::{config_differences, ConfigDifference};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// One compared run and how this run's configuration differs from it.
#[derive(Debug, Clone, PartialEq)]
pub struct RunConfigChanges {
    pub run: PathBuf,
    pub changes: ConfigChanges,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigChanges {
    /// Every recorded configuration value that differs; empty when none does.
    Recorded(Vec<ConfigDifference>),
    /// The compared run's manifest holds no configuration evidence.
    NotRecorded,
    /// The compared run's manifest could not be read.
    Unreadable(String),
}

impl ConfigChanges {
    /// The distinct changed keys, in key order.
    pub fn keys(&self) -> Vec<&str> {
        let mut keys: Vec<&str> = match self {
            Self::Recorded(differences) => differences
                .iter()
                .map(|difference| difference.key.as_str())
                .collect(),
            Self::NotRecorded | Self::Unreadable(_) => Vec::new(),
        };
        keys.dedup();
        keys
    }
}

/// Configuration changes against the replayed run, or `resumed_from` and every
/// `merged_from` ancestor; empty when the run rebuilt nothing or recorded no configuration.
pub fn read_config_changes(run_dir: &Path) -> Result<Vec<RunConfigChanges>, JournalReadError> {
    let manifest = load_manifest(run_dir)?;
    Ok(match &manifest.effective_config {
        Some(current) => compare_with_runs(current, &rebuilt_runs(&manifest)),
        None => Vec::new(),
    })
}

/// Compare this run's evidence with each run's recorded evidence.
pub fn compare_with_runs(
    current: &EffectiveConfigEvidence,
    runs: &[PathBuf],
) -> Vec<RunConfigChanges> {
    runs.iter()
        .map(|run| RunConfigChanges {
            run: run.clone(),
            changes: match load_manifest(run) {
                Ok(RunManifest {
                    effective_config: Some(recorded),
                    ..
                }) => ConfigChanges::Recorded(config_differences(&recorded, current)),
                Ok(_) => ConfigChanges::NotRecorded,
                Err(error) => ConfigChanges::Unreadable(error.to_string()),
            },
        })
        .collect()
}

/// The stages of a run that refused in a way a changed configuration value can
/// cause, in key order.
pub async fn read_configuration_refusals(
    run_dir: &Path,
) -> Result<BTreeSet<String>, JournalReadError> {
    let mut snapshot = open_disk_run(run_dir).await?;
    let mut refused = BTreeSet::new();
    while let Some(record) = snapshot.next().await? {
        let RunRecordData::Chain(row) = &record.record else {
            continue;
        };
        let ChainPayload::Execution(execution) = &row.payload else {
            continue;
        };
        if let Some(stage) = configuration_refusal(record.journal.stage.as_ref(), execution) {
            refused.insert(stage.to_string());
        }
    }
    Ok(refused)
}

fn configuration_refusal<'a>(
    stage: Option<&'a RunStage>,
    execution: &'a ExecutionPayload,
) -> Option<&'a str> {
    if !departs_from_archive(execution) {
        return None;
    }
    match execution {
        ExecutionPayload::StageFatalRecorded(fatal) => Some(&fatal.stage_key),
        _ => stage.map(|stage| stage.key.as_str()),
    }
}

/// A refused stage whose recorded logic version differs from this run's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicVersionChange {
    pub run: PathBuf,
    pub stage: String,
    pub recorded: String,
    pub current: String,
}

/// Each of `stages` whose logic version this run records differently from a
/// compared run; unreadable manifests compare nothing.
pub fn read_logic_version_changes(
    run_dir: &Path,
    runs: &[PathBuf],
    stages: &BTreeSet<String>,
) -> Vec<LogicVersionChange> {
    let Ok(current) = load_manifest(run_dir) else {
        return Vec::new();
    };
    runs.iter()
        .filter_map(|run| Some((run, load_manifest(run).ok()?)))
        .flat_map(|(run, recorded)| {
            logic_version_changes(&recorded.stages, &current.stages, stages)
                .into_iter()
                .map(|(stage, recorded, current)| LogicVersionChange {
                    run: run.clone(),
                    stage,
                    recorded,
                    current,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn logic_version_changes(
    recorded: &HashMap<String, RunManifestStage>,
    current: &HashMap<String, RunManifestStage>,
    stages: &BTreeSet<String>,
) -> Vec<(String, String, String)> {
    stages
        .iter()
        .filter_map(|stage| {
            let was = &recorded.get(stage)?.stage_logic_version;
            let now = &current.get(stage)?.stage_logic_version;
            (was != now).then(|| (stage.clone(), was.clone(), now.clone()))
        })
        .collect()
}

fn rebuilt_runs(manifest: &RunManifest) -> Vec<PathBuf> {
    match (&manifest.replay, &manifest.resume) {
        (_, Some(resume)) => std::iter::once(resume.resumed_from.clone())
            .chain(resume.merged_from.iter().cloned())
            .collect(),
        (Some(replay), None) => vec![PathBuf::from(&replay.replay_from)],
        (None, None) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::execution_payload::{
        SourceContinuationRefusal, SourceContinuationRefusedFact,
    };
    use obzenflow_core::event::{
        StageFatalCode, StageFatalReason, StageFatalRecorded, StageFatalSeverity,
    };
    use obzenflow_core::StageId;

    fn fatal(stage: &str, reason: StageFatalReason) -> ExecutionPayload {
        ExecutionPayload::StageFatalRecorded(StageFatalRecorded {
            severity: StageFatalSeverity::Primary,
            stage_id: StageId::new(),
            stage_key: stage.to_string(),
            causal_event_id: None,
            input_position: None,
            primary_cause_event_id: None,
            code: StageFatalCode::Replay,
            reason,
            detail: String::new(),
            departure: None,
        })
    }

    fn source_refused(refusal: SourceContinuationRefusal) -> ExecutionPayload {
        ExecutionPayload::SourceContinuationRefused(SourceContinuationRefusedFact { refusal })
    }

    fn stage(key: &str, logic: &str) -> (String, RunManifestStage) {
        (
            key.to_string(),
            RunManifestStage {
                dsl_var: key.to_string(),
                stage_type: StageType::Transform,
                is_effectful: true,
                stage_id: StageId::new().to_string(),
                stage_logic_version: logic.to_string(),
                data_journal_file: String::new(),
                error_journal_file: String::new(),
                inbound: Vec::new(),
                delivery_order: obzenflow_core::journal::archive::DeliveryOrder::SingleInput,
                ai_models: Vec::new(),
                max_open_keys: None,
            },
        )
    }

    #[test]
    fn only_refusals_a_changed_value_can_cause_name_their_stage() {
        let accounts = RunStage {
            key: "accounts".to_string(),
            id: StageId::new(),
            stage_type: StageType::FiniteSource,
            is_effectful: false,
        };
        for reason in [
            StageFatalReason::DescriptorMismatch,
            StageFatalReason::MissingRecordedEffect,
            StageFatalReason::UnusedEffectHistory,
            StageFatalReason::MissingRecordedFact,
            StageFatalReason::UnusedFactHistory,
        ] {
            assert_eq!(
                configuration_refusal(None, &fatal("score", reason)),
                Some("score")
            );
        }
        assert_eq!(
            configuration_refusal(
                Some(&accounts),
                &source_refused(SourceContinuationRefusal::InputChanged {
                    part: "file".to_string()
                })
            ),
            Some("accounts")
        );
        for code_only in [
            fatal("score", StageFatalReason::ReplayDivergence),
            fatal("score", StageFatalReason::RecordingEnded),
            source_refused(SourceContinuationRefusal::PositionUnreadable),
        ] {
            assert_eq!(
                configuration_refusal(Some(&accounts), &code_only),
                None,
                "{code_only:?}"
            );
        }
    }

    #[test]
    fn logic_versions_compare_only_refused_stages_both_runs_record() {
        let recorded = HashMap::from([stage("score", "v1"), stage("snk", "v1")]);
        let current = HashMap::from([stage("score", "v2"), stage("snk", "v2"), stage("new", "v1")]);
        let refused = BTreeSet::from(["new".to_string(), "score".to_string()]);
        assert_eq!(
            logic_version_changes(&recorded, &current, &refused),
            [("score".to_string(), "v1".to_string(), "v2".to_string())]
        );
    }
}
