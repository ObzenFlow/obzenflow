// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Order certification decides which stages may be compared positionally.
//!
//! A pure walk over each manifest's `inbound` edges and `delivery_order`
//! values, which must match in both manifests. A replay reproduces every
//! order except `unordered`. Independent runs agree only where the order is a
//! function of the inputs, so a recorded stage or ancestor is reported instead
//! of compared.

use std::collections::{BTreeMap, HashMap, HashSet};

use obzenflow_core::journal::archive::manifest::{DeliveryOrder, RunManifest};

/// What the two runs are to each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// The candidate replays the baseline's lineage.
    Replay,
    /// Two runs that each read their inputs live.
    IndependentRuns,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StageCertification {
    Certified,
    /// Not positionally comparable. `blocking` names every ancestor, possibly
    /// the stage itself, whose delivery order prevents certification; at
    /// least one is unordered or differs between the runs.
    NotOrderCertified {
        blocking: Vec<String>,
    },
    /// Not compared across independent runs, because the stage or an ordered
    /// ancestor recorded its order at run time. `blocking` names them.
    OrderRecorded {
        blocking: Vec<String>,
    },
}

impl StageCertification {
    pub fn is_certified(&self) -> bool {
        matches!(self, Self::Certified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Blocker {
    Recorded,
    Unconstrained,
}

/// Compute certification for every stage key present in both manifests.
/// Callers validate stage-set and inbound equality first (`validate`), so the
/// walk reads edges from the candidate manifest.
pub fn certify(
    baseline: &RunManifest,
    candidate: &RunManifest,
    comparison: Comparison,
) -> BTreeMap<String, StageCertification> {
    let blockers: HashMap<&str, Blocker> = candidate
        .stages
        .iter()
        .filter_map(|(key, stage)| {
            let baseline_order = baseline.stages.get(key).map(|s| s.delivery_order);
            stage_blocker(baseline_order, stage.delivery_order, comparison)
                .map(|blocker| (key.as_str(), blocker))
        })
        .collect();

    let mut result = BTreeMap::new();
    for key in candidate.stages.keys() {
        let mut blocking = HashSet::new();
        let mut visited = HashSet::new();
        collect_blocking(key, candidate, &blockers, &mut visited, &mut blocking);
        let unconstrained = blocking
            .iter()
            .any(|stage| blockers.get(stage.as_str()) == Some(&Blocker::Unconstrained));
        let mut blocking: Vec<String> = blocking.into_iter().collect();
        blocking.sort();
        let certification = if blocking.is_empty() {
            StageCertification::Certified
        } else if unconstrained {
            StageCertification::NotOrderCertified { blocking }
        } else {
            StageCertification::OrderRecorded { blocking }
        };
        result.insert(key.clone(), certification);
    }
    result
}

fn stage_blocker(
    baseline: Option<DeliveryOrder>,
    candidate: DeliveryOrder,
    comparison: Comparison,
) -> Option<Blocker> {
    if baseline != Some(candidate) || candidate == DeliveryOrder::Unordered {
        return Some(Blocker::Unconstrained);
    }
    match comparison {
        Comparison::Replay => None,
        Comparison::IndependentRuns if candidate.agrees_across_runs() => None,
        Comparison::IndependentRuns => Some(Blocker::Recorded),
    }
}

fn collect_blocking(
    key: &str,
    manifest: &RunManifest,
    blockers: &HashMap<&str, Blocker>,
    visited: &mut HashSet<String>,
    blocking: &mut HashSet<String>,
) {
    if !visited.insert(key.to_string()) {
        return;
    }
    if blockers.contains_key(key) {
        blocking.insert(key.to_string());
    }
    if let Some(stage) = manifest.stages.get(key) {
        for upstream in &stage.inbound {
            collect_blocking(upstream, manifest, blockers, visited, blocking);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::journal::archive::manifest::RunManifestStage;
    use std::collections::HashMap as StdHashMap;
    use DeliveryOrder::{Canonical, Recorded, SingleInput, Structural, Unordered};

    fn manifest(stages: &[(&str, &[&str], DeliveryOrder)]) -> RunManifest {
        let mut map = StdHashMap::new();
        for (key, inbound, delivery_order) in stages {
            map.insert(
                key.to_string(),
                RunManifestStage {
                    dsl_var: key.to_string(),
                    stage_type: StageType::Transform,
                    is_effectful: false,
                    stage_id: format!("stage_{key}"),
                    stage_logic_version: "1".to_string(),
                    data_journal_file: format!("{key}.log"),
                    error_journal_file: format!("{key}_error.log"),
                    inbound: inbound.iter().map(|s| s.to_string()).collect(),
                    delivery_order: *delivery_order,
                },
            );
        }
        RunManifest {
            metrics_journals: None,
            journal_schema_version:
                obzenflow_core::journal::archive::manifest::JOURNAL_SCHEMA_VERSION.to_string(),

            obzenflow_version: "0.1.2".to_string(),
            flow_id: "flow_test".to_string(),
            pipeline_writer_id: obzenflow_core::SystemId::new().into(),
            flow_name: "test".to_string(),
            created_at: obzenflow_core::chrono::Utc::now(),
            replay: None,
            resume: None,
            stages: map,
            system_journal_file: "system.log".to_string(),
            effective_config: None,
            capabilities: Default::default(),
            bounded_direct_fact_admission: Vec::new(),
        }
    }

    fn names(blocking: &[&str]) -> Vec<String> {
        blocking.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn single_input_chain_is_fully_certified() {
        let m = manifest(&[
            ("source", &[], SingleInput),
            ("transform", &["source"], SingleInput),
            ("sink", &["transform"], SingleInput),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            let certs = certify(&m, &m, comparison);
            assert!(certs.values().all(StageCertification::is_certified));
        }
    }

    #[test]
    fn unordered_fan_in_poisons_descendants_and_names_the_blocker() {
        let m = manifest(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Unordered),
            ("downstream", &["merge"], SingleInput),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            let certs = certify(&m, &m, comparison);
            assert!(certs["a"].is_certified());
            assert!(certs["b"].is_certified());
            assert_eq!(
                certs["merge"],
                StageCertification::NotOrderCertified {
                    blocking: names(&["merge"])
                }
            );
            assert_eq!(
                certs["downstream"],
                StageCertification::NotOrderCertified {
                    blocking: names(&["merge"])
                }
            );
        }
    }

    #[test]
    fn canonical_fan_in_certifies_across_independent_runs_and_replay() {
        let m = manifest(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Canonical),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            assert!(certify(&m, &m, comparison)["merge"].is_certified());
        }
    }

    #[test]
    fn recorded_fan_in_certifies_against_its_replay_only() {
        let m = manifest(&[
            ("batch", &[], SingleInput),
            ("live", &[], SingleInput),
            ("merge", &["batch", "live"], Recorded),
            ("fold", &["merge"], SingleInput),
        ]);
        let replay = certify(&m, &m, Comparison::Replay);
        assert!(replay.values().all(StageCertification::is_certified));

        let independent = certify(&m, &m, Comparison::IndependentRuns);
        assert!(independent["batch"].is_certified());
        assert_eq!(
            independent["merge"],
            StageCertification::OrderRecorded {
                blocking: names(&["merge"])
            }
        );
        assert_eq!(
            independent["fold"],
            StageCertification::OrderRecorded {
                blocking: names(&["merge"])
            }
        );
    }

    #[test]
    fn an_unordered_ancestor_outranks_a_recorded_one_and_both_are_named() {
        let m = manifest(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("loose", &["a", "b"], Unordered),
            ("recorded", &["loose", "b"], Recorded),
        ]);
        let certs = certify(&m, &m, Comparison::IndependentRuns);
        assert_eq!(
            certs["recorded"],
            StageCertification::NotOrderCertified {
                blocking: names(&["loose", "recorded"])
            }
        );
    }

    #[test]
    fn hydrating_join_with_one_stream_reader_certifies_in_both() {
        let m = manifest(&[
            ("accounts", &[], SingleInput),
            ("ledger", &[], SingleInput),
            ("posted", &["accounts", "ledger"], Structural),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            assert!(certify(&m, &m, comparison)["posted"].is_certified());
        }
    }

    #[test]
    fn delivery_order_mismatch_blocks_the_stage() {
        let baseline = manifest(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Recorded),
        ]);
        let candidate = manifest(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Canonical),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            assert_eq!(
                certify(&baseline, &candidate, comparison)["merge"],
                StageCertification::NotOrderCertified {
                    blocking: names(&["merge"])
                }
            );
        }
    }

    #[test]
    fn cycles_terminate_and_stay_uncertified() {
        let m = manifest(&[
            ("source", &[], SingleInput),
            ("loop_a", &["source", "loop_b"], Unordered),
            ("loop_b", &["loop_a"], Unordered),
        ]);
        let certs = certify(&m, &m, Comparison::Replay);
        assert!(!certs["loop_a"].is_certified());
        assert!(!certs["loop_b"].is_certified());
        assert!(certs["source"].is_certified());
    }
}
