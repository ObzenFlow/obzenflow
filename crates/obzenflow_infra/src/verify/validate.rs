// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pre-comparison validation (FLOWIP-095j): manifest pairing gates, mode
//! selection, and the per-stage `stage_logic_version` deltas that classify
//! divergences as `changed-code` versus `unexpected`.

use std::collections::BTreeMap;

use obzenflow_core::journal::ArchiveStatus;

use super::certification::{certify, StageCertification};
use super::error::RefusalReason;
use super::lineage::{identity_mode, IdentityMode};
use super::source::RunSource;
use super::walker::WalkMode;

pub(crate) struct ValidationPlan {
    pub mode: WalkMode,
    pub identity: IdentityMode,
    pub certification: BTreeMap<String, StageCertification>,
    pub logic_version_changed: BTreeMap<String, bool>,
}

pub(crate) fn validate(
    baseline: &dyn RunSource,
    candidate: &dyn RunSource,
) -> Result<ValidationPlan, Box<RefusalReason>> {
    let b_manifest = baseline.manifest();
    let c_manifest = candidate.manifest();

    // A resume-mode candidate legitimately extends history past the baseline.
    if c_manifest
        .replay
        .as_ref()
        .is_some_and(|replay| replay.allow_incomplete_archive)
    {
        return Err(Box::new(RefusalReason::ResumeIncompleteCandidate));
    }

    if b_manifest.flow_name != c_manifest.flow_name {
        return Err(Box::new(RefusalReason::FlowNameMismatch {
            baseline: b_manifest.flow_name.clone(),
            candidate: c_manifest.flow_name.clone(),
        }));
    }

    let mut added: Vec<String> = c_manifest
        .stages
        .keys()
        .filter(|key| !b_manifest.stages.contains_key(*key))
        .cloned()
        .collect();
    let mut removed: Vec<String> = b_manifest
        .stages
        .keys()
        .filter(|key| !c_manifest.stages.contains_key(*key))
        .cloned()
        .collect();
    if !added.is_empty() || !removed.is_empty() {
        added.sort();
        removed.sort();
        return Err(Box::new(RefusalReason::StageSetMismatch { added, removed }));
    }

    for (key, c_stage) in &c_manifest.stages {
        let b_stage = &b_manifest.stages[key];
        if b_stage.inbound != c_stage.inbound {
            return Err(Box::new(RefusalReason::InboundMismatch {
                stage: key.clone(),
            }));
        }
    }

    // The same terminal gate replay itself applies.
    for (source, label) in [(baseline, "baseline"), (candidate, "candidate")] {
        let status = source.status();
        if !matches!(status, ArchiveStatus::Completed | ArchiveStatus::Cancelled) {
            let _ = label;
            return Err(Box::new(RefusalReason::StatusGate {
                run: source.run_dir().to_path_buf(),
                status: format!("{status:?}"),
            }));
        }
    }

    // A killed baseline may not have processed every recorded source row
    // downstream and never wrote completion evidence: prefix semantics.
    let mode = if baseline.status() == ArchiveStatus::Cancelled {
        WalkMode::BaselinePrefixOfCandidate
    } else {
        WalkMode::WholeRun
    };

    let logic_version_changed = c_manifest
        .stages
        .iter()
        .map(|(key, c_stage)| {
            let changed = b_manifest.stages[key].stage_logic_version != c_stage.stage_logic_version;
            (key.clone(), changed)
        })
        .collect();

    Ok(ValidationPlan {
        mode,
        identity: identity_mode(c_manifest),
        certification: certify(b_manifest, c_manifest),
        logic_version_changed,
    })
}
