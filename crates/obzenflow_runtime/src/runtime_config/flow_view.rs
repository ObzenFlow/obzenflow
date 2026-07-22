// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The per-flow effective view produced at build (FLOWIP-010 Phase B):
//! every registered knob resolved at every applicable point, with full
//! provenance, plus the manifest evidence projection (§6a).

use super::candidates::{ConfigValue, DslCandidates};
use super::model::{doc_for, doc_for_at, Resolved};
use super::resolve::ResolutionPoint;
use super::schema::knob;
use obzenflow_core::config::{
    ConfigScope, ConfigSource, EffectiveConfigEvidence, LineagePolicy, ResolvedForDoc,
    ResolvedValueDoc,
};
use obzenflow_core::event::EffectType;
use obzenflow_core::StageKey;
use std::collections::{BTreeMap, BTreeSet};

/// Resolved backpressure enforcement for one directed edge (FLOWIP-115e).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureMode {
    Off,
    Track,
    Enforce,
}

impl BackpressureMode {
    /// The single source of truth for the wire tokens the
    /// `runtime.backpressure.mode` knob carries. Everything else (the Token
    /// knob's allowed set, the resolver, the DSL clause) derives from here.
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Track => "track",
            Self::Enforce => "enforce",
        }
    }

    /// The valid tokens for the Token knob, in schema order.
    pub const TOKENS: &'static [&'static str] = &[
        Self::Off.as_token(),
        Self::Track.as_token(),
        Self::Enforce.as_token(),
    ];

    /// Parse a resolved token; `None` if it is not a known mode.
    pub fn from_token(token: &str) -> Option<Self> {
        [Self::Off, Self::Track, Self::Enforce]
            .into_iter()
            .find(|mode| mode.as_token() == token)
    }
}

/// Structural facts the flow build hands to `materialize_flow_config`:
/// identity, the stage-key set, the directed-edge set, and the DSL-declared
/// candidates collected at their declaration sites (§4a).
#[derive(Debug, Clone, Default)]
pub struct FlowResolutionContext {
    pub flow_name: String,
    pub stages: BTreeSet<StageKey>,
    pub edges: BTreeSet<(StageKey, StageKey)>,
    /// Declared effect subjects, used only to validate exact file addresses.
    /// Actual config consumers are carried separately from defaults in `dsl`.
    pub declared_effects: BTreeMap<StageKey, BTreeSet<EffectType>>,
    pub dsl: DslCandidates,
}

/// Immutable per-flow resolution: knob key path -> point address ->
/// resolved value. The point address is where the value applies; the
/// value's meta carries the WINNING scope (which may be coarser).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FlowEffectiveConfig {
    values: BTreeMap<String, BTreeMap<ResolutionPoint, Resolved<ConfigValue>>>,
    warnings: Vec<String>,
}

impl FlowEffectiveConfig {
    pub(super) fn new(
        values: BTreeMap<String, BTreeMap<ResolutionPoint, Resolved<ConfigValue>>>,
        warnings: Vec<String>,
    ) -> Self {
        Self { values, warnings }
    }

    pub fn get(&self, key_path: &str, point: &ConfigScope) -> Option<&Resolved<ConfigValue>> {
        let point = match point {
            ConfigScope::Global => ResolutionPoint::Global,
            ConfigScope::Flow => ResolutionPoint::Flow,
            ConfigScope::Stage { stage } => ResolutionPoint::Stage(stage.clone()),
            ConfigScope::Edge {
                upstream,
                downstream,
            } => ResolutionPoint::Edge {
                upstream: upstream.clone(),
                downstream: downstream.clone(),
            },
        };
        self.get_at(key_path, &point)
    }

    pub fn get_at(
        &self,
        key_path: &str,
        point: &ResolutionPoint,
    ) -> Option<&Resolved<ConfigValue>> {
        self.values
            .get(key_path)
            .and_then(|points| points.get(point))
    }

    pub fn points(
        &self,
        key_path: &str,
    ) -> impl Iterator<Item = (&ResolutionPoint, &Resolved<ConfigValue>)> {
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
    /// rung supplied one. Required where the edge's mode resolves to
    /// `enforce`, via the materialization pass.
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

    /// The enforcement mode and its winning source for one directed edge.
    /// Always resolves, since the registry default is `off`; the source
    /// lets the SCC auto-enable distinguish the built-in default from an
    /// explicit `off` (FLOWIP-115e).
    pub fn backpressure_mode_for(
        &self,
        upstream: &StageKey,
        downstream: &StageKey,
    ) -> (BackpressureMode, ConfigSource) {
        self.get(
            "runtime.backpressure.mode",
            &ConfigScope::Edge {
                upstream: upstream.clone(),
                downstream: downstream.clone(),
            },
        )
        .and_then(|resolved| {
            resolved
                .value
                .as_text()
                .and_then(BackpressureMode::from_token)
                .map(|mode| (mode, resolved.meta.source.clone()))
        })
        .unwrap_or((BackpressureMode::Off, ConfigSource::Default))
    }

    /// The stall timeout resolved for one directed edge, in milliseconds.
    /// Required where the edge's mode resolves to `enforce`, via the
    /// materialization pass.
    pub fn backpressure_stall_timeout_for(
        &self,
        upstream: &StageKey,
        downstream: &StageKey,
    ) -> Option<u64> {
        self.u64_at(
            "runtime.backpressure.stall_timeout_ms",
            &ConfigScope::Edge {
                upstream: upstream.clone(),
                downstream: downstream.clone(),
            },
        )
    }

    pub fn effect_value(
        &self,
        key_path: &str,
        stage: &StageKey,
        effect_type: &EffectType,
    ) -> Option<&Resolved<ConfigValue>> {
        self.get_at(
            key_path,
            &ResolutionPoint::Effect {
                stage: stage.clone(),
                effect_type: effect_type.clone(),
            },
        )
    }

    pub fn exact_view(&self, point: ResolutionPoint) -> ExactConfigView<'_> {
        ExactConfigView {
            effective: self,
            point,
        }
    }

    /// The §6a manifest projection. Identical non-effect resolutions collapse
    /// (a defaulted knob does not emit one row per stage), while effect rows
    /// retain their concrete `resolved_for` identity even when equal values
    /// inherit the same broadcast winner.
    pub fn manifest_evidence(&self) -> EffectiveConfigEvidence {
        let mut docs: Vec<_> = self
            .values
            .iter()
            .flat_map(|(key_path, points)| {
                let spec = knob(key_path).expect("only registered knobs are materialized");
                points.iter().map(move |(point, resolved)| match point {
                    ResolutionPoint::Effect { stage, effect_type } => doc_for_at(
                        spec,
                        resolved,
                        Some(ResolvedForDoc::Effect {
                            stage: stage.as_str().to_string(),
                            effect_type: effect_type.as_str().to_string(),
                        }),
                    ),
                    _ => doc_for(spec, resolved),
                })
            })
            .collect();
        docs.sort_by(|a, b| {
            (&a.key_path, &a.resolved_for, &a.scope, &a.source)
                .cmp(&(&b.key_path, &b.resolved_for, &b.scope, &b.source))
                .then_with(|| a.value.to_string().cmp(&b.value.to_string()))
        });
        docs.dedup();
        EffectiveConfigEvidence::new(docs)
    }

    /// Per-stage docs: every knob resolved AT this stage's point address
    /// (the winning scope in the meta may be coarser). Serves the HTTP
    /// stage route and the CLI from one projection.
    pub fn stage_docs(&self, stage: &StageKey) -> Vec<ResolvedValueDoc> {
        let point = ResolutionPoint::Stage(stage.clone());
        let mut docs: Vec<_> = self
            .values
            .iter()
            .filter_map(|(key_path, points)| {
                let spec = knob(key_path).expect("only registered knobs are materialized");
                points.get(&point).map(|resolved| doc_for(spec, resolved))
            })
            .collect();
        docs.sort_by(|a, b| a.key_path.cmp(&b.key_path));
        docs
    }

    /// Per-edge docs for this stage's outgoing edges, keyed by the §4c
    /// display spelling `up|>down`.
    pub fn edge_docs_for_upstream(
        &self,
        stage: &StageKey,
    ) -> BTreeMap<String, Vec<ResolvedValueDoc>> {
        let mut edges: BTreeMap<String, Vec<ResolvedValueDoc>> = BTreeMap::new();
        for (key_path, points) in &self.values {
            let spec = knob(key_path).expect("only registered knobs are materialized");
            for (point, resolved) in points {
                if let ResolutionPoint::Edge {
                    upstream,
                    downstream,
                } = point
                {
                    if upstream == stage {
                        edges
                            .entry(format!("{upstream}|>{downstream}"))
                            .or_default()
                            .push(doc_for(spec, resolved));
                    }
                }
            }
        }
        for docs in edges.values_mut() {
            docs.sort_by(|a, b| a.key_path.cmp(&b.key_path));
        }
        edges
    }

    /// Per-effect docs for one stage, keyed by stable effect type.
    pub fn effect_docs_for_stage(
        &self,
        stage: &StageKey,
    ) -> BTreeMap<String, Vec<ResolvedValueDoc>> {
        let mut effects: BTreeMap<String, Vec<ResolvedValueDoc>> = BTreeMap::new();
        for (key_path, points) in &self.values {
            let spec = knob(key_path).expect("only registered knobs are materialized");
            for (point, resolved) in points {
                if let ResolutionPoint::Effect {
                    stage: point_stage,
                    effect_type,
                } = point
                {
                    if point_stage == stage {
                        effects
                            .entry(effect_type.as_str().to_string())
                            .or_default()
                            .push(doc_for_at(
                                spec,
                                resolved,
                                Some(ResolvedForDoc::Effect {
                                    stage: stage.as_str().to_string(),
                                    effect_type: effect_type.as_str().to_string(),
                                }),
                            ));
                    }
                }
            }
        }
        for docs in effects.values_mut() {
            docs.sort_by(|a, b| a.key_path.cmp(&b.key_path));
        }
        effects
    }
}

/// Immutable policy-neutral view resolved for exactly one application point.
pub struct ExactConfigView<'a> {
    effective: &'a FlowEffectiveConfig,
    point: ResolutionPoint,
}

impl<'a> ExactConfigView<'a> {
    pub fn point(&self) -> &ResolutionPoint {
        &self.point
    }

    pub fn get(&self, key_path: &str) -> Option<&'a Resolved<ConfigValue>> {
        self.effective.get_at(key_path, &self.point)
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
            declared_effects: BTreeMap::new(),
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
            address: ConfigScope::stage("sink").into(),
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
