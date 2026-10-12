// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! What a replayed or resumed run's configuration changed from the runs whose
//! history it rebuilds (FLOWIP-010t B6, B9). Read from run manifests linked
//! by `replay_from`, `resumed_from` and `merged_from`; it adds no event and
//! never refuses or alters a run.

use super::JournalReadError;
use crate::journal::disk::inspect::load_manifest;
use obzenflow_core::config::EffectiveConfigEvidence;
use obzenflow_core::journal::RunManifest;
use obzenflow_runtime::runtime_config::{config_differences, ConfigDifference};
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

/// A run's configuration changes against each run whose history it rebuilt:
/// the replayed run, or `resumed_from` and every `merged_from` ancestor. A
/// run that rebuilt nothing, or recorded no configuration, compares nothing.
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

fn rebuilt_runs(manifest: &RunManifest) -> Vec<PathBuf> {
    match (&manifest.replay, &manifest.resume) {
        (_, Some(resume)) => std::iter::once(resume.resumed_from.clone())
            .chain(resume.merged_from.iter().cloned())
            .collect(),
        (Some(replay), None) => vec![PathBuf::from(&replay.replay_from)],
        (None, None) => Vec::new(),
    }
}
