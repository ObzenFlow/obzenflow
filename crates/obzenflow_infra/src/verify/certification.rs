// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Order-certification (FLOWIP-095j, FLOWIP-095l): which comparison a stage
//! admits.
//!
//! Each stage's manifest `order_decision` is one of `Ordered`,
//! `OrderInsensitive`, or `CycleNonDeterministic`, written at flow build where
//! the FLOWIP-095d marked set, barriers, and cycles are known. Certification is
//! a pure graph walk over the ancestor cone, reading both manifests so two runs
//! are compared only under a matching regime:
//!
//! - every cone fan-in `Ordered` in both runs is `Positional`: delivery
//!   positions reproduce, so the stage is compared row by row;
//! - a cone fan-in `OrderInsensitive` in both runs (and no hard blocker) is
//!   `Content`: positions may differ between correct runs, but the emitted
//!   multiset is the complete observable (the build certified that no descendant
//!   observes order), so the stage is compared as an order-insensitive multiset;
//! - a `CycleNonDeterministic` member, or a per-stage regime mismatch between
//!   the runs, on any path is `NotCertifiable`: order is neither pinned nor
//!   proven irrelevant, so only a per-type count advisory is reported.

use std::collections::{BTreeMap, HashMap, HashSet};

use obzenflow_core::journal::run_manifest::{OrderDecision, RunManifest};

#[derive(Debug, Clone, PartialEq)]
pub enum StageCertification {
    /// Every fan-in on every path is `Ordered` in both runs: delivery positions
    /// reproduce, so the stage is compared position by position.
    Positional,
    /// A fan-in on a path is `OrderInsensitive` in both runs (and no hard
    /// blocker): delivery order may differ between correct runs, but the emitted
    /// multiset is the complete observable, so the stage is compared as an
    /// order-insensitive multiset.
    Content,
    /// A cycle member, or a per-stage regime mismatch between the runs, sits on
    /// a path. `blocking` names every such ancestor (possibly the stage itself).
    /// Order is neither pinned nor proven irrelevant, so the stage is not
    /// certifiable and only a per-type count advisory is reported.
    NotCertifiable { blocking: Vec<String> },
}

impl StageCertification {
    /// True when the stage carries a certificate, positional or content.
    pub fn is_certified(&self) -> bool {
        matches!(self, Self::Positional | Self::Content)
    }
}

/// A stage's joint ordering regime across the two runs.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Regime {
    /// `Ordered` in both runs.
    Ordered,
    /// `OrderInsensitive` in both runs.
    Insensitive,
    /// `CycleNonDeterministic` in either run, the two runs disagree, or the
    /// stage is missing from a run. Not certifiable.
    Hard,
}

fn joint_regime(baseline: &RunManifest, candidate: &RunManifest, key: &str) -> Regime {
    let baseline = baseline.stages.get(key).map(|s| &s.order_decision);
    let candidate = candidate.stages.get(key).map(|s| &s.order_decision);
    match (baseline, candidate) {
        (Some(OrderDecision::Ordered), Some(OrderDecision::Ordered)) => Regime::Ordered,
        (Some(OrderDecision::OrderInsensitive), Some(OrderDecision::OrderInsensitive)) => {
            Regime::Insensitive
        }
        // CycleNonDeterministic in either, a mismatch, or a missing stage.
        _ => Regime::Hard,
    }
}

/// Compute certification for every stage key present in both manifests.
/// Callers validate stage-set and inbound equality first (`validate`), so the
/// walk reads edges from the candidate manifest.
pub fn certify(
    baseline: &RunManifest,
    candidate: &RunManifest,
) -> BTreeMap<String, StageCertification> {
    let regime: HashMap<&str, Regime> = candidate
        .stages
        .keys()
        .map(|key| (key.as_str(), joint_regime(baseline, candidate, key)))
        .collect();

    let mut result = BTreeMap::new();
    for key in candidate.stages.keys() {
        let mut hard = HashSet::new();
        let mut has_soft = false;
        let mut visited = HashSet::new();
        collect_cone(
            key,
            candidate,
            &regime,
            &mut visited,
            &mut hard,
            &mut has_soft,
        );

        let certification = if !hard.is_empty() {
            let mut blocking: Vec<String> = hard.into_iter().collect();
            blocking.sort();
            StageCertification::NotCertifiable { blocking }
        } else if has_soft {
            StageCertification::Content
        } else {
            StageCertification::Positional
        };
        result.insert(key.clone(), certification);
    }
    result
}

/// Walk the ancestor cone of `key`, recording every hard-blocking stage and
/// whether any cone stage is order-insensitive. Precedence at the call site: a
/// hard blocker beats a soft (order-insensitive) stage beats all-ordered.
fn collect_cone(
    key: &str,
    manifest: &RunManifest,
    regime: &HashMap<&str, Regime>,
    visited: &mut HashSet<String>,
    hard: &mut HashSet<String>,
    has_soft: &mut bool,
) {
    if !visited.insert(key.to_string()) {
        return;
    }
    match regime.get(key).copied().unwrap_or(Regime::Hard) {
        Regime::Ordered => {}
        Regime::Insensitive => *has_soft = true,
        Regime::Hard => {
            hard.insert(key.to_string());
        }
    }
    if let Some(stage) = manifest.stages.get(key) {
        for upstream in &stage.inbound {
            collect_cone(upstream, manifest, regime, visited, hard, has_soft);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::journal::run_manifest::RunManifestStage;
    use std::collections::HashMap as StdHashMap;

    use OrderDecision::{CycleNonDeterministic, OrderInsensitive, Ordered};

    fn manifest(stages: &[(&str, &[&str], OrderDecision)]) -> RunManifest {
        let mut map = StdHashMap::new();
        for (key, inbound, order_decision) in stages {
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
                    order_decision: order_decision.clone(),
                },
            );
        }
        RunManifest {
            manifest_version: "3.0".to_string(),
            journal_format_version: 1,
            obzenflow_version: "0.1.2".to_string(),
            flow_id: "flow_test".to_string(),
            flow_name: "test".to_string(),
            created_at: obzenflow_core::chrono::Utc::now(),
            replay: None,
            stages: map,
            system_journal_file: "system.log".to_string(),
        }
    }

    #[test]
    fn single_input_chain_is_positionally_certified() {
        let m = manifest(&[
            ("source", &[], Ordered),
            ("transform", &["source"], Ordered),
            ("sink", &["transform"], Ordered),
        ]);
        let certs = certify(&m, &m);
        assert!(certs.values().all(|c| *c == StageCertification::Positional));
    }

    #[test]
    fn order_insensitive_fan_in_is_content_certified_and_propagates() {
        // A single-input `Ordered` stage below an `OrderInsensitive` fan-in is
        // content-certified, not positional: its positions are run-dependent
        // (its input arrived in a free order) while its multiset reproduces.
        let m = manifest(&[
            ("a", &[], Ordered),
            ("b", &[], Ordered),
            ("merge", &["a", "b"], OrderInsensitive),
            ("downstream", &["merge"], Ordered),
        ]);
        let certs = certify(&m, &m);
        assert_eq!(certs["a"], StageCertification::Positional);
        assert_eq!(certs["merge"], StageCertification::Content);
        assert_eq!(certs["downstream"], StageCertification::Content);
    }

    #[test]
    fn ordered_fan_in_is_positional_when_both_regimes_agree() {
        let m = manifest(&[
            ("a", &[], Ordered),
            ("b", &[], Ordered),
            ("merge", &["a", "b"], Ordered),
        ]);
        let certs = certify(&m, &m);
        assert_eq!(certs["merge"], StageCertification::Positional);
    }

    #[test]
    fn regime_mismatch_is_not_certifiable() {
        let baseline = manifest(&[
            ("a", &[], Ordered),
            ("b", &[], Ordered),
            ("merge", &["a", "b"], OrderInsensitive),
        ]);
        let candidate = manifest(&[
            ("a", &[], Ordered),
            ("b", &[], Ordered),
            ("merge", &["a", "b"], Ordered),
        ]);
        let certs = certify(&baseline, &candidate);
        assert_eq!(
            certs["merge"],
            StageCertification::NotCertifiable {
                blocking: vec!["merge".to_string()]
            }
        );
    }

    #[test]
    fn cycle_member_is_not_certifiable_and_names_the_blocker() {
        let m = manifest(&[
            ("source", &[], Ordered),
            ("loop_a", &["source", "loop_b"], CycleNonDeterministic),
            ("loop_b", &["loop_a"], CycleNonDeterministic),
        ]);
        let certs = certify(&m, &m);
        assert_eq!(certs["source"], StageCertification::Positional);
        assert!(matches!(
            certs["loop_a"],
            StageCertification::NotCertifiable { .. }
        ));
        assert!(matches!(
            certs["loop_b"],
            StageCertification::NotCertifiable { .. }
        ));
    }

    #[test]
    fn a_cycle_upstream_poisons_a_downstream_stage() {
        let m = manifest(&[
            ("source", &[], Ordered),
            ("loop_a", &["source", "loop_b"], CycleNonDeterministic),
            ("loop_b", &["loop_a"], CycleNonDeterministic),
            ("downstream", &["loop_a"], Ordered),
        ]);
        let certs = certify(&m, &m);
        let StageCertification::NotCertifiable { blocking } = &certs["downstream"] else {
            panic!("a stage below a cycle is not certifiable");
        };
        assert!(blocking.contains(&"loop_a".to_string()));
    }
}
