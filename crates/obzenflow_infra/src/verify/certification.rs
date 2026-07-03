// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Order-certification (FLOWIP-095j): which stages may be compared
//! positionally.
//!
//! A stage is order-certified when every fan-in on every path from the
//! sources to it delivers deterministically. The inputs are the manifest's
//! recorded `inbound` edges and `ordered_delivery` flags (written at flow
//! build, where the FLOWIP-095d marked set and structural orderers are
//! known), so certification is a pure graph walk over manifest data and both
//! verify surfaces certify identically. A stage's own `ordered_delivery` must
//! be true in BOTH manifests: the flag is the ordering-regime record, and a
//! mismatch means the runs executed under different regimes.

use std::collections::{BTreeMap, HashMap, HashSet};

use obzenflow_core::journal::run_manifest::RunManifest;

#[derive(Debug, Clone, PartialEq)]
pub enum StageCertification {
    Certified,
    /// Not positionally comparable. `blocking` names every ancestor
    /// (possibly including the stage itself) whose delivery order is
    /// unconstrained in at least one of the two runs.
    NotOrderCertified {
        blocking: Vec<String>,
    },
}

impl StageCertification {
    pub fn is_certified(&self) -> bool {
        matches!(self, Self::Certified)
    }
}

/// Compute certification for every stage key present in both manifests.
/// Callers validate stage-set and inbound equality first (`validate`), so the
/// walk reads edges from the candidate manifest.
pub fn certify(
    baseline: &RunManifest,
    candidate: &RunManifest,
) -> BTreeMap<String, StageCertification> {
    let ordered_both: HashMap<&str, bool> = candidate
        .stages
        .iter()
        .map(|(key, stage)| {
            let baseline_ordered = baseline
                .stages
                .get(key)
                .map(|s| s.ordered_delivery)
                .unwrap_or(false);
            (key.as_str(), stage.ordered_delivery && baseline_ordered)
        })
        .collect();

    let mut result = BTreeMap::new();
    for key in candidate.stages.keys() {
        let mut blocking = HashSet::new();
        let mut visited = HashSet::new();
        collect_blocking(key, candidate, &ordered_both, &mut visited, &mut blocking);
        let certification = if blocking.is_empty() {
            StageCertification::Certified
        } else {
            let mut blocking: Vec<String> = blocking.into_iter().collect();
            blocking.sort();
            StageCertification::NotOrderCertified { blocking }
        };
        result.insert(key.clone(), certification);
    }
    result
}

fn collect_blocking(
    key: &str,
    manifest: &RunManifest,
    ordered_both: &HashMap<&str, bool>,
    visited: &mut HashSet<String>,
    blocking: &mut HashSet<String>,
) {
    if !visited.insert(key.to_string()) {
        return;
    }
    if !ordered_both.get(key).copied().unwrap_or(false) {
        blocking.insert(key.to_string());
    }
    if let Some(stage) = manifest.stages.get(key) {
        for upstream in &stage.inbound {
            collect_blocking(upstream, manifest, ordered_both, visited, blocking);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::journal::run_manifest::RunManifestStage;
    use std::collections::HashMap as StdHashMap;

    fn manifest(stages: &[(&str, &[&str], bool)]) -> RunManifest {
        let mut map = StdHashMap::new();
        for (key, inbound, ordered) in stages {
            map.insert(
                key.to_string(),
                RunManifestStage {
                    dsl_var: key.to_string(),
                    stage_type: StageType::Transform,
                    stage_id: format!("stage_{key}"),
                    stage_logic_version: "1".to_string(),
                    data_journal_file: format!("{key}.log"),
                    error_journal_file: format!("{key}_error.log"),
                    inbound: inbound.iter().map(|s| s.to_string()).collect(),
                    ordered_delivery: *ordered,
                },
            );
        }
        RunManifest {
            manifest_version: "2.0".to_string(),
            journal_format_version: 1,
            obzenflow_version: "0.1.2".to_string(),
            flow_id: "flow_test".to_string(),
            flow_name: "test".to_string(),
            created_at: obzenflow_core::chrono::Utc::now(),
            replay: None,
            resume: None,
            stages: map,
            system_journal_file: "system.log".to_string(),
            effective_config: None,
        }
    }

    #[test]
    fn single_input_chain_is_fully_certified() {
        let m = manifest(&[
            ("source", &[], true),
            ("transform", &["source"], true),
            ("sink", &["transform"], true),
        ]);
        let certs = certify(&m, &m);
        assert!(certs.values().all(StageCertification::is_certified));
    }

    #[test]
    fn unordered_fan_in_poisons_descendants_and_names_the_blocker() {
        let m = manifest(&[
            ("a", &[], true),
            ("b", &[], true),
            ("merge", &["a", "b"], false),
            ("downstream", &["merge"], true),
        ]);
        let certs = certify(&m, &m);
        assert!(certs["a"].is_certified());
        assert!(certs["b"].is_certified());
        assert_eq!(
            certs["merge"],
            StageCertification::NotOrderCertified {
                blocking: vec!["merge".to_string()]
            }
        );
        assert_eq!(
            certs["downstream"],
            StageCertification::NotOrderCertified {
                blocking: vec!["merge".to_string()]
            }
        );
    }

    #[test]
    fn ordered_fan_in_certifies_when_both_regimes_agree() {
        let m = manifest(&[
            ("a", &[], true),
            ("b", &[], true),
            ("merge", &["a", "b"], true),
        ]);
        let certs = certify(&m, &m);
        assert!(certs["merge"].is_certified());
    }

    #[test]
    fn regime_mismatch_downgrades_the_stage() {
        let baseline = manifest(&[
            ("a", &[], true),
            ("b", &[], true),
            ("merge", &["a", "b"], false),
        ]);
        let candidate = manifest(&[
            ("a", &[], true),
            ("b", &[], true),
            ("merge", &["a", "b"], true),
        ]);
        let certs = certify(&baseline, &candidate);
        assert_eq!(
            certs["merge"],
            StageCertification::NotOrderCertified {
                blocking: vec!["merge".to_string()]
            }
        );
    }

    #[test]
    fn cycles_terminate_and_stay_uncertified() {
        // Cycle members record ordered_delivery=false at build time.
        let m = manifest(&[
            ("source", &[], true),
            ("loop_a", &["source", "loop_b"], false),
            ("loop_b", &["loop_a"], false),
        ]);
        let certs = certify(&m, &m);
        assert!(!certs["loop_a"].is_certified());
        assert!(!certs["loop_b"].is_certified());
        assert!(certs["source"].is_certified());
    }
}
