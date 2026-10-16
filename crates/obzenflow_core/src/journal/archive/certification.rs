// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Order certification decides which stages of two runs reproduce their
//! delivery order position by position. A replay reproduces every order except
//! `unordered`; independent runs agree only where the order follows from the inputs.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::manifest::{DeliveryOrder, RunManifest};

/// What the two runs are to each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// The candidate replays, or resumes, the baseline's lineage.
    Replay,
    /// Two runs that each read their inputs live.
    IndependentRuns,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StageCertification {
    Certified,
    /// Not positionally comparable. `blocking` names the stage or ancestors whose
    /// delivery order is unordered or differs between the runs.
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

/// One stage's inbound edges and how its input order is fixed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageOrder {
    pub inbound: Vec<String>,
    pub delivery_order: DeliveryOrder,
}

/// Every stage's inbound edges and delivery order, by stage key, as a run
/// manifest records them or a flow derives them before its manifest exists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StageOrders(pub BTreeMap<String, StageOrder>);

impl From<&RunManifest> for StageOrders {
    fn from(manifest: &RunManifest) -> Self {
        Self(
            manifest
                .stages
                .iter()
                .map(|(key, stage)| {
                    (
                        key.clone(),
                        StageOrder {
                            inbound: stage.inbound.clone(),
                            delivery_order: stage.delivery_order,
                        },
                    )
                })
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Blocker {
    Recorded,
    Unconstrained,
}

/// Compute certification for every stage of `candidate` against two run
/// manifests.
pub fn certify(
    baseline: &RunManifest,
    candidate: &RunManifest,
    comparison: Comparison,
) -> BTreeMap<String, StageCertification> {
    certify_orders(&baseline.into(), &candidate.into(), comparison)
}

/// Compute certification for every stage of `candidate`. A stage blocks when the
/// baseline lacks it, its inbound edges or delivery order differ, or it is unordered.
pub fn certify_orders(
    baseline: &StageOrders,
    candidate: &StageOrders,
    comparison: Comparison,
) -> BTreeMap<String, StageCertification> {
    let blockers: HashMap<&str, Blocker> = candidate
        .0
        .iter()
        .filter_map(|(key, stage)| {
            stage_blocker(baseline.0.get(key), stage, comparison)
                .map(|blocker| (key.as_str(), blocker))
        })
        .collect();

    let mut result = BTreeMap::new();
    for key in candidate.0.keys() {
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
    baseline: Option<&StageOrder>,
    candidate: &StageOrder,
    comparison: Comparison,
) -> Option<Blocker> {
    if baseline != Some(candidate) || candidate.delivery_order == DeliveryOrder::Unordered {
        return Some(Blocker::Unconstrained);
    }
    match comparison {
        Comparison::Replay => None,
        Comparison::IndependentRuns if candidate.delivery_order.agrees_across_runs() => None,
        Comparison::IndependentRuns => Some(Blocker::Recorded),
    }
}

fn collect_blocking(
    key: &str,
    orders: &StageOrders,
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
    if let Some(stage) = orders.0.get(key) {
        for upstream in &stage.inbound {
            collect_blocking(upstream, orders, blockers, visited, blocking);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DeliveryOrder::{Canonical, Recorded, SingleInput, Structural, Unordered};

    fn orders(stages: &[(&str, &[&str], DeliveryOrder)]) -> StageOrders {
        StageOrders(
            stages
                .iter()
                .map(|(key, inbound, delivery_order)| {
                    (
                        key.to_string(),
                        StageOrder {
                            inbound: inbound.iter().map(|s| s.to_string()).collect(),
                            delivery_order: *delivery_order,
                        },
                    )
                })
                .collect(),
        )
    }

    fn names(blocking: &[&str]) -> Vec<String> {
        blocking.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn single_input_chain_is_fully_certified() {
        let m = orders(&[
            ("source", &[], SingleInput),
            ("transform", &["source"], SingleInput),
            ("sink", &["transform"], SingleInput),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            let certs = certify_orders(&m, &m, comparison);
            assert!(certs.values().all(StageCertification::is_certified));
        }
    }

    #[test]
    fn unordered_fan_in_poisons_descendants_and_names_the_blocker() {
        let m = orders(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Unordered),
            ("downstream", &["merge"], SingleInput),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            let certs = certify_orders(&m, &m, comparison);
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
        let m = orders(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Canonical),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            assert!(certify_orders(&m, &m, comparison)["merge"].is_certified());
        }
    }

    #[test]
    fn recorded_fan_in_certifies_against_its_replay_only() {
        let m = orders(&[
            ("batch", &[], SingleInput),
            ("live", &[], SingleInput),
            ("merge", &["batch", "live"], Recorded),
            ("fold", &["merge"], SingleInput),
        ]);
        let replay = certify_orders(&m, &m, Comparison::Replay);
        assert!(replay.values().all(StageCertification::is_certified));

        let independent = certify_orders(&m, &m, Comparison::IndependentRuns);
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
        let m = orders(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("loose", &["a", "b"], Unordered),
            ("recorded", &["loose", "b"], Recorded),
        ]);
        let certs = certify_orders(&m, &m, Comparison::IndependentRuns);
        assert_eq!(
            certs["recorded"],
            StageCertification::NotOrderCertified {
                blocking: names(&["loose", "recorded"])
            }
        );
    }

    #[test]
    fn hydrating_join_with_one_stream_reader_certifies_in_both() {
        let m = orders(&[
            ("accounts", &[], SingleInput),
            ("ledger", &[], SingleInput),
            ("posted", &["accounts", "ledger"], Structural),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            assert!(certify_orders(&m, &m, comparison)["posted"].is_certified());
        }
    }

    #[test]
    fn delivery_order_mismatch_blocks_the_stage() {
        let baseline = orders(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Recorded),
        ]);
        let candidate = orders(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("merge", &["a", "b"], Canonical),
        ]);
        for comparison in [Comparison::Replay, Comparison::IndependentRuns] {
            assert_eq!(
                certify_orders(&baseline, &candidate, comparison)["merge"],
                StageCertification::NotOrderCertified {
                    blocking: names(&["merge"])
                }
            );
        }
    }

    #[test]
    fn changed_inbound_edges_and_a_missing_stage_block() {
        let baseline = orders(&[
            ("a", &[], SingleInput),
            ("normalize", &["a"], SingleInput),
            ("sink", &["normalize"], SingleInput),
        ]);
        let rewired = orders(&[
            ("a", &[], SingleInput),
            ("b", &[], SingleInput),
            ("normalize", &["b"], SingleInput),
            ("sink", &["normalize"], SingleInput),
        ]);
        let certs = certify_orders(&baseline, &rewired, Comparison::Replay);
        assert!(certs["a"].is_certified());
        assert_eq!(
            certs["sink"],
            StageCertification::NotOrderCertified {
                blocking: names(&["b", "normalize"])
            }
        );
    }

    #[test]
    fn cycles_terminate_and_stay_uncertified() {
        let m = orders(&[
            ("source", &[], SingleInput),
            ("loop_a", &["source", "loop_b"], Unordered),
            ("loop_b", &["loop_a"], Unordered),
        ]);
        let certs = certify_orders(&m, &m, Comparison::Replay);
        assert!(!certs["loop_a"].is_certified());
        assert!(!certs["loop_b"].is_certified());
        assert!(certs["source"].is_certified());
    }
}
