// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The per-flow effective view produced at build (FLOWIP-010 Phase B):
//! every registered knob resolved at every applicable point, with full
//! provenance, plus the manifest evidence projection (§6a).

use super::candidates::{ConfigValue, DslCandidates};
use super::model::{doc_for, Resolved};
use super::schema::knob;
use obzenflow_core::config::{ConfigScope, EffectiveConfigEvidence, LineagePolicy};
use obzenflow_core::StageKey;
use std::collections::{BTreeMap, BTreeSet};

/// Structural facts the flow build hands to `materialize_flow_config`:
/// identity, the stage-key set, the directed-edge set, and the DSL-declared
/// candidates collected at their declaration sites (§4a).
#[derive(Debug, Clone, Default)]
pub struct FlowResolutionContext {
    pub flow_name: String,
    pub stages: BTreeSet<StageKey>,
    pub edges: BTreeSet<(StageKey, StageKey)>,
    pub dsl: DslCandidates,
}

/// Immutable per-flow resolution: knob key path -> point address ->
/// resolved value. The point address is where the value applies; the
/// value's meta carries the WINNING scope (which may be coarser).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FlowEffectiveConfig {
    values: BTreeMap<String, BTreeMap<ConfigScope, Resolved<ConfigValue>>>,
    warnings: Vec<String>,
}

impl FlowEffectiveConfig {
    pub(super) fn new(
        values: BTreeMap<String, BTreeMap<ConfigScope, Resolved<ConfigValue>>>,
        warnings: Vec<String>,
    ) -> Self {
        Self { values, warnings }
    }

    pub fn get(&self, key_path: &str, point: &ConfigScope) -> Option<&Resolved<ConfigValue>> {
        self.values
            .get(key_path)
            .and_then(|points| points.get(point))
    }

    pub fn points(
        &self,
        key_path: &str,
    ) -> impl Iterator<Item = (&ConfigScope, &Resolved<ConfigValue>)> {
        self.values
            .get(key_path)
            .into_iter()
            .flat_map(|points| points.iter())
    }

    /// Build-time validation warnings (the §4c fan-in footgun).
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn u64_at(&self, key_path: &str, point: &ConfigScope) -> Option<u64> {
        self.get(key_path, point).and_then(|r| r.value.as_u64())
    }

    /// Per-stage lineage policy (`runtime.max_lineage_depth`), the value
    /// threaded into stage resources by the build (§7 carrier).
    pub fn lineage_policy_for(&self, stage: &StageKey) -> LineagePolicy {
        let depth = self
            .u64_at(
                "runtime.max_lineage_depth",
                &ConfigScope::Stage {
                    stage: stage.clone(),
                },
            )
            .expect("registry default guarantees a lineage depth");
        LineagePolicy {
            max_lineage_depth: depth as usize,
        }
    }

    pub fn cycle_max_iterations(&self) -> u64 {
        self.u64_at("runtime.cycle_max_iterations", &ConfigScope::Flow)
            .expect("registry default guarantees cycle_max_iterations")
    }

    pub fn heartbeat_interval_for(&self, stage: &StageKey) -> u64 {
        self.u64_at(
            "runtime.heartbeat_interval",
            &ConfigScope::Stage {
                stage: stage.clone(),
            },
        )
        .expect("registry default guarantees heartbeat_interval")
    }

    pub fn metrics_drain_timeout_ms(&self) -> u64 {
        self.u64_at("runtime.metrics_drain_timeout_ms", &ConfigScope::Global)
            .expect("registry default guarantees metrics_drain_timeout_ms")
    }

    /// `contracts.source_contract_strict_mode` token: `abort` or `warn`.
    pub fn source_contract_strict_mode(&self) -> &str {
        self.get(
            "contracts.source_contract_strict_mode",
            &ConfigScope::Global,
        )
        .and_then(|r| r.value.as_text())
        .expect("registry default guarantees strict mode")
    }

    /// The backpressure window resolved for one directed edge, when any
    /// rung supplied one (`OptionalAbsent` until FLOWIP-115e).
    pub fn backpressure_window_for(
        &self,
        upstream: &StageKey,
        downstream: &StageKey,
    ) -> Option<&Resolved<ConfigValue>> {
        self.get(
            "runtime.backpressure.window",
            &ConfigScope::Edge {
                upstream: upstream.clone(),
                downstream: downstream.clone(),
            },
        )
    }

    pub fn breaker_threshold_for(&self, stage: &StageKey) -> Option<&Resolved<ConfigValue>> {
        self.get(
            "effects.circuit_breaker.threshold",
            &ConfigScope::Stage {
                stage: stage.clone(),
            },
        )
    }

    pub fn limiter_events_per_second_for(
        &self,
        stage: &StageKey,
    ) -> Option<&Resolved<ConfigValue>> {
        self.get(
            "effects.rate_limiter.events_per_second",
            &ConfigScope::Stage {
                stage: stage.clone(),
            },
        )
    }

    pub fn limiter_burst_capacity_for(&self, stage: &StageKey) -> Option<&Resolved<ConfigValue>> {
        self.get(
            "effects.rate_limiter.burst_capacity",
            &ConfigScope::Stage {
                stage: stage.clone(),
            },
        )
    }

    /// The §6a manifest projection: one doc per distinct
    /// `(key_path, winning scope, source, value)`. Identical per-point
    /// resolutions collapse (a knob at its default does not emit one doc
    /// per stage), and every override is preserved because its winning
    /// scope names its coverage.
    pub fn manifest_evidence(&self) -> EffectiveConfigEvidence {
        let mut docs: Vec<_> = self
            .values
            .iter()
            .flat_map(|(key_path, points)| {
                let spec = knob(key_path).expect("only registered knobs are materialized");
                points.values().map(move |resolved| doc_for(spec, resolved))
            })
            .collect();
        docs.sort_by(|a, b| {
            (&a.key_path, &a.scope, &a.source)
                .cmp(&(&b.key_path, &b.scope, &b.source))
                .then_with(|| a.value.to_string().cmp(&b.value.to_string()))
        });
        docs.dedup();
        EffectiveConfigEvidence::new(docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::candidates::{CandidateSet, ScopedCandidate};
    use crate::runtime_config::model::ResolvedRuntimeConfig;
    use crate::runtime_config::resolve::materialize_flow_config;
    use obzenflow_core::config::{ConfigScope, ConfigSource};

    fn two_stage_ctx() -> FlowResolutionContext {
        FlowResolutionContext {
            flow_name: "f".to_string(),
            stages: BTreeSet::from([StageKey::from("src"), StageKey::from("sink")]),
            edges: BTreeSet::from([(StageKey::from("src"), StageKey::from("sink"))]),
            dsl: Default::default(),
        }
    }

    #[test]
    fn evidence_collapses_identical_per_point_resolutions() {
        let snapshot = ResolvedRuntimeConfig::builtin_defaults();
        let effective = materialize_flow_config(&snapshot, two_stage_ctx()).unwrap();
        let evidence = effective.manifest_evidence();
        // Both stages resolve the same default lineage depth: one doc, not two.
        let lineage_docs: Vec<_> = evidence
            .values
            .iter()
            .filter(|d| d.key_path == "runtime.max_lineage_depth")
            .collect();
        assert_eq!(lineage_docs.len(), 1);
        assert_eq!(lineage_docs[0].scope, "global");
        assert_eq!(lineage_docs[0].source, "default");
    }

    #[test]
    fn overrides_survive_evidence_with_their_winning_scope() {
        let mut set = CandidateSet::default();
        set.admit(ScopedCandidate {
            key_path: "runtime.max_lineage_depth".to_string(),
            scope: ConfigScope::stage("sink"),
            source: ConfigSource::File,
            value: ConfigValue::U64(7),
        })
        .unwrap();
        let snapshot = ResolvedRuntimeConfig::new(set);
        let effective = materialize_flow_config(&snapshot, two_stage_ctx()).unwrap();

        assert_eq!(
            effective
                .lineage_policy_for(&StageKey::from("sink"))
                .max_lineage_depth,
            7
        );
        assert_eq!(
            effective
                .lineage_policy_for(&StageKey::from("src"))
                .max_lineage_depth,
            100
        );

        let evidence = effective.manifest_evidence();
        let docs: Vec<_> = evidence
            .values
            .iter()
            .filter(|d| d.key_path == "runtime.max_lineage_depth")
            .collect();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].scope, "global");
        assert_eq!(docs[1].scope, "stage:sink");
        assert_eq!(docs[1].value, serde_json::json!(7));
        assert_eq!(docs[1].source, "file");
    }

    #[test]
    fn typed_getters_serve_flow_and_global_points() {
        let snapshot = ResolvedRuntimeConfig::builtin_defaults();
        let effective = materialize_flow_config(&snapshot, two_stage_ctx()).unwrap();
        assert_eq!(effective.cycle_max_iterations(), 30);
        assert_eq!(effective.metrics_drain_timeout_ms(), 5000);
        assert_eq!(effective.source_contract_strict_mode(), "abort");
        assert!(effective
            .backpressure_window_for(&StageKey::from("src"), &StageKey::from("sink"))
            .is_none());
    }
}
