// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The resolution ladder: scope-major, source-minor, post-ladder default
//! (FLOWIP-010 §2, §4c). One implementation serves startup views, flow-build
//! materialization, and the introspection surface.

use super::candidates::{CandidateSet, ConfigValue};
use super::error::ConfigResolveError;
use super::flow_view::{FlowEffectiveConfig, FlowResolutionContext};
use super::model::{Resolved, ResolvedRuntimeConfig};
use super::schema::{knob_registry, EdgeEndpoint, KnobDefault, KnobSpec, KnobTarget};
use obzenflow_core::config::{ConfigScope, ConfigSource, ConfigValueMeta};
use obzenflow_core::StageKey;
use std::collections::BTreeMap;

/// Where a knob is being resolved. Points are generated from the knob's
/// target: Global-target knobs resolve at `Global`, Stage-target knobs once
/// per stage, Edge-target knobs once per directed edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionPoint {
    Global,
    Flow,
    Stage(StageKey),
    Edge {
        upstream: StageKey,
        downstream: StageKey,
    },
}

impl ResolutionPoint {
    /// The address this point's resolution is stored under.
    pub fn address(&self) -> ConfigScope {
        match self {
            Self::Global => ConfigScope::Global,
            Self::Flow => ConfigScope::Flow,
            Self::Stage(stage) => ConfigScope::Stage {
                stage: stage.clone(),
            },
            Self::Edge {
                upstream,
                downstream,
            } => ConfigScope::Edge {
                upstream: upstream.clone(),
                downstream: downstream.clone(),
            },
        }
    }

    fn display(&self) -> String {
        match self {
            Self::Global => "the runtime".to_string(),
            Self::Flow => "the flow".to_string(),
            Self::Stage(stage) => format!("stage '{}'", stage.as_str()),
            Self::Edge {
                upstream,
                downstream,
            } => format!("edge {}|>{}", upstream.as_str(), downstream.as_str()),
        }
    }
}

/// The scope ladder for one knob at one point, most to least specific
/// (§2 lock; §4c for edge-target knobs, whose stage rung binds one
/// endpoint).
fn ladder(spec: &KnobSpec, point: &ResolutionPoint) -> Vec<ConfigScope> {
    match (&spec.target, point) {
        (
            KnobTarget::Edge { stage_binding },
            ResolutionPoint::Edge {
                upstream,
                downstream,
            },
        ) => {
            let bound = match stage_binding {
                EdgeEndpoint::Upstream => upstream,
                EdgeEndpoint::Downstream => downstream,
            };
            vec![
                ConfigScope::Edge {
                    upstream: upstream.clone(),
                    downstream: downstream.clone(),
                },
                ConfigScope::Stage {
                    stage: bound.clone(),
                },
                ConfigScope::Flow,
                ConfigScope::Global,
            ]
        }
        (KnobTarget::Stage, ResolutionPoint::Stage(stage)) => vec![
            ConfigScope::Stage {
                stage: stage.clone(),
            },
            ConfigScope::Flow,
            ConfigScope::Global,
        ],
        (KnobTarget::Flow, ResolutionPoint::Flow) => {
            vec![ConfigScope::Flow, ConfigScope::Global]
        }
        (KnobTarget::Global, ResolutionPoint::Global) => vec![ConfigScope::Global],
        // The registry generates points from targets, so mismatches are
        // programmer errors, not user input.
        (target, point) => {
            unreachable!("resolution point {point:?} does not match knob target {target:?}")
        }
    }
}

/// Source precedence within one scope: CLI > file > env > DSL (§2).
fn source_order(
    slots: &super::candidates::SourceSlots,
) -> [(ConfigSource, &Option<ConfigValue>); 4] {
    [
        (ConfigSource::Cli, &slots.cli),
        (ConfigSource::File, &slots.file),
        (ConfigSource::Env, &slots.env),
        (ConfigSource::Dsl, &slots.dsl),
    ]
}

/// Resolve one knob at one point. `Ok(None)` is a legal outcome only for
/// `OptionalAbsent` knobs; `Required` knobs error with the full supply help.
pub fn resolve_at(
    spec: &KnobSpec,
    point: &ResolutionPoint,
    set: &CandidateSet,
) -> Result<Option<Resolved<ConfigValue>>, ConfigResolveError> {
    for scope in ladder(spec, point) {
        if let Some(slots) = set.get(spec.key_path, &scope) {
            for (source, slot) in source_order(slots) {
                if let Some(value) = slot {
                    return Ok(Some(Resolved {
                        value: value.clone(),
                        meta: ConfigValueMeta {
                            source,
                            scope,
                            key_path: spec.key_path.to_string(),
                        },
                    }));
                }
            }
        }
    }
    match &spec.default {
        KnobDefault::Value(value) => Ok(Some(Resolved {
            value: value.clone(),
            meta: ConfigValueMeta {
                source: ConfigSource::Default,
                scope: ConfigScope::Global,
                key_path: spec.key_path.to_string(),
            },
        })),
        KnobDefault::OptionalAbsent => Ok(None),
        KnobDefault::Required => Err(ConfigResolveError::RequiredKnobUnresolved {
            key_path: spec.key_path.to_string(),
            point: point.display(),
            supply_help: supply_help(spec, point),
        }),
    }
}

/// The "supply it at one of: ..." help, generated from the ladder and the
/// registry so file addresses and env spellings stay mechanical (§4c).
fn supply_help(spec: &KnobSpec, point: &ResolutionPoint) -> String {
    let mut parts = Vec::new();
    for scope in ladder(spec, point) {
        let file = file_address(spec, &scope);
        let part = match &scope {
            ConfigScope::Edge { .. } => {
                format!("edge scope (file {file}, DSL edge declaration)")
            }
            ConfigScope::Stage { stage } => format!(
                "stage scope for '{}' (file {file}, DSL stage declaration)",
                stage.as_str()
            ),
            ConfigScope::Flow => format!("flow scope (file {file}, DSL flow declaration)"),
            ConfigScope::Global => match spec.env_name() {
                Some(env) => format!("global scope (file {file}, env {env})"),
                None => format!("global scope (file {file})"),
            },
        };
        parts.push(part);
    }
    format!("supply it at one of: {}", parts.join(", "))
}

/// `[table]` address for a knob at a scope, derived from the key path and
/// the §4c nested layout: `[table]`, `[table.flow]`, `[table.stages.<s>]`,
/// `[table.stages.<up>.edges.<down>]`.
fn file_address(spec: &KnobSpec, scope: &ConfigScope) -> String {
    let path = spec.file_path.unwrap_or(spec.key_path);
    let table = match path.rsplit_once('.') {
        Some((table, _field)) => table,
        None => path,
    };
    match scope {
        ConfigScope::Global => format!("[{table}]"),
        ConfigScope::Flow => format!("[{table}.flow]"),
        ConfigScope::Stage { stage } => format!("[{table}.stages.{}]", stage.as_str()),
        ConfigScope::Edge {
            upstream,
            downstream,
        } => format!(
            "[{table}.stages.{}.edges.{}]",
            upstream.as_str(),
            downstream.as_str()
        ),
    }
}

/// Phase B: merge DSL candidates, validate scoped entries against the
/// topology (010f-shaped diagnostics), and resolve every registered knob at
/// every applicable point (§4c). Returns the immutable per-flow view.
pub fn materialize_flow_config(
    snapshot: &ResolvedRuntimeConfig,
    ctx: FlowResolutionContext,
) -> Result<FlowEffectiveConfig, ConfigResolveError> {
    let mut working = snapshot.candidates().clone();
    for candidate in ctx.dsl.entries() {
        working.admit(candidate.clone())?;
    }

    let mut warnings = Vec::new();
    for spec in knob_registry() {
        for scope in working.scopes_for(spec.key_path) {
            match scope {
                ConfigScope::Stage { stage } => {
                    if !ctx.stages.contains(stage) {
                        return Err(ConfigResolveError::UnknownStage {
                            key_path: spec.key_path.to_string(),
                            stage: stage.as_str().to_string(),
                        });
                    }
                    // §4c fan-in footgun: the stage rung of an edge-target
                    // knob binds upstream, so an entry on a stage with no
                    // outgoing edges governs nothing.
                    if matches!(spec.target, KnobTarget::Edge { .. })
                        && !ctx.edges.iter().any(|(up, _)| up == stage)
                    {
                        warnings.push(format!(
                            "config warning at {}: stage-scoped entry on '{}' has no effect; \
                             the stage rung binds the writing (upstream) stage and '{}' has no \
                             outgoing edges",
                            spec.key_path,
                            stage.as_str(),
                            stage.as_str()
                        ));
                    }
                }
                ConfigScope::Edge {
                    upstream,
                    downstream,
                } => {
                    if !ctx.edges.contains(&(upstream.clone(), downstream.clone())) {
                        return Err(ConfigResolveError::UnknownEdge {
                            key_path: spec.key_path.to_string(),
                            upstream: upstream.as_str().to_string(),
                            downstream: downstream.as_str().to_string(),
                        });
                    }
                }
                ConfigScope::Global | ConfigScope::Flow => {}
            }
        }
    }
    for warning in &warnings {
        tracing::warn!("{warning}");
    }

    let mut values: BTreeMap<String, BTreeMap<ConfigScope, Resolved<ConfigValue>>> =
        BTreeMap::new();
    for spec in knob_registry() {
        let points: Vec<ResolutionPoint> = match spec.target {
            KnobTarget::Global => vec![ResolutionPoint::Global],
            KnobTarget::Flow => vec![ResolutionPoint::Flow],
            KnobTarget::Stage => ctx
                .stages
                .iter()
                .map(|stage| ResolutionPoint::Stage(stage.clone()))
                .collect(),
            KnobTarget::Edge { .. } => ctx
                .edges
                .iter()
                .map(|(upstream, downstream)| ResolutionPoint::Edge {
                    upstream: upstream.clone(),
                    downstream: downstream.clone(),
                })
                .collect(),
        };
        for point in points {
            if let Some(resolved) = resolve_at(spec, &point, &working)? {
                values
                    .entry(spec.key_path.to_string())
                    .or_default()
                    .insert(point.address(), resolved);
            }
        }
    }

    // FLOWIP-115e required-where-enforce: an edge whose mode resolves to
    // `enforce` must resolve a window and a stall timeout, or the build
    // fails naming the edge and the scopes that may supply them. A window
    // resolved where the mode stays off or track governs nothing and draws
    // a diagnostic.
    for (upstream, downstream) in &ctx.edges {
        let address = ConfigScope::Edge {
            upstream: upstream.clone(),
            downstream: downstream.clone(),
        };
        let mode = values
            .get("runtime.backpressure.mode")
            .and_then(|per_scope| per_scope.get(&address))
            .and_then(|resolved| resolved.value.as_text())
            .unwrap_or("off");
        if mode == "enforce" {
            let point = ResolutionPoint::Edge {
                upstream: upstream.clone(),
                downstream: downstream.clone(),
            };
            for key in [
                "runtime.backpressure.window",
                "runtime.backpressure.stall_timeout_ms",
            ] {
                let resolved = values
                    .get(key)
                    .and_then(|per_scope| per_scope.get(&address));
                if resolved.is_none() {
                    let spec = crate::runtime_config::schema::knob(key)
                        .expect("backpressure knobs are registered");
                    return Err(ConfigResolveError::RequiredKnobUnresolved {
                        key_path: key.to_string(),
                        point: point.display(),
                        supply_help: supply_help(spec, &point),
                    });
                }
            }
        } else if values
            .get("runtime.backpressure.window")
            .and_then(|per_scope| per_scope.get(&address))
            .is_some()
        {
            let warning = format!(
                "config warning at runtime.backpressure.window: a window resolved for \
                 edge {}|>{} whose mode is '{}'; it has no effect until the mode \
                 resolves to 'enforce'",
                upstream.as_str(),
                downstream.as_str(),
                mode
            );
            tracing::warn!("{warning}");
            warnings.push(warning);
        }
    }

    Ok(FlowEffectiveConfig::new(values, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::candidates::ScopedCandidate;
    use crate::runtime_config::schema::{EnvBinding, KnobType, Mutability, Redaction};
    use std::collections::BTreeSet;

    fn admit(
        set: &mut CandidateSet,
        key: &str,
        scope: ConfigScope,
        source: ConfigSource,
        value: ConfigValue,
    ) {
        set.admit(ScopedCandidate {
            key_path: key.to_string(),
            scope,
            source,
            value,
        })
        .unwrap();
    }

    fn window_spec() -> &'static KnobSpec {
        crate::runtime_config::schema::knob("runtime.backpressure.window").unwrap()
    }

    fn lineage_spec() -> &'static KnobSpec {
        crate::runtime_config::schema::knob("runtime.max_lineage_depth").unwrap()
    }

    #[test]
    fn scope_beats_source_and_source_breaks_ties_within_scope() {
        let mut set = CandidateSet::default();
        // Global CLI candidate vs stage-scoped file candidate: stage wins.
        admit(
            &mut set,
            "runtime.max_lineage_depth",
            ConfigScope::Global,
            ConfigSource::Cli,
            ConfigValue::U64(50),
        );
        admit(
            &mut set,
            "runtime.max_lineage_depth",
            ConfigScope::stage("enricher"),
            ConfigSource::File,
            ConfigValue::U64(25),
        );
        let resolved = resolve_at(
            lineage_spec(),
            &ResolutionPoint::Stage(StageKey::from("enricher")),
            &set,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(25));
        assert_eq!(resolved.meta.scope, ConfigScope::stage("enricher"));
        assert_eq!(resolved.meta.source, ConfigSource::File);

        // Within one scope: file beats env beats dsl.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.max_lineage_depth",
            ConfigScope::Global,
            ConfigSource::Env,
            ConfigValue::U64(10),
        );
        admit(
            &mut set,
            "runtime.max_lineage_depth",
            ConfigScope::Global,
            ConfigSource::File,
            ConfigValue::U64(20),
        );
        let resolved = resolve_at(
            lineage_spec(),
            &ResolutionPoint::Stage(StageKey::from("any")),
            &set,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(20));
        assert_eq!(resolved.meta.source, ConfigSource::File);
        // The winning scope is Global even though the point is a stage.
        assert_eq!(resolved.meta.scope, ConfigScope::Global);
    }

    #[test]
    fn the_4a_worked_example_resolves_verbatim() {
        // The flow's `backpressure:` clause declares a DSL-tier window of
        // 1000; `enricher` declares a deliberate 200 exception; a global
        // file entry sets 500.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::Flow,
            ConfigSource::Dsl,
            ConfigValue::U64(1000),
        );
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::stage("enricher"),
            ConfigSource::Dsl,
            ConfigValue::U64(200),
        );
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::Global,
            ConfigSource::File,
            ConfigValue::U64(500),
        );

        // Edge out of enricher: the stage DSL declaration wins (scope=stage,
        // source=dsl); the global file entry reaches past neither DSL value.
        let enricher_edge = ResolutionPoint::Edge {
            upstream: StageKey::from("enricher"),
            downstream: StageKey::from("merger"),
        };
        let resolved = resolve_at(window_spec(), &enricher_edge, &set)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(200));
        assert_eq!(resolved.meta.scope, ConfigScope::stage("enricher"));
        assert_eq!(resolved.meta.source, ConfigSource::Dsl);

        // Every other stage resolves the flow DSL value (scope=flow, source=dsl).
        let other_edge = ResolutionPoint::Edge {
            upstream: StageKey::from("fetcher"),
            downstream: StageKey::from("enricher"),
        };
        let resolved = resolve_at(window_spec(), &other_edge, &set)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(1000));
        assert_eq!(resolved.meta.scope, ConfigScope::Flow);
        assert_eq!(resolved.meta.source, ConfigSource::Dsl);

        // The operator who wants the last word adds a stage-scoped file
        // entry (same scope as the stage declaration; file beats dsl).
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::stage("enricher"),
            ConfigSource::File,
            ConfigValue::U64(400),
        );
        let resolved = resolve_at(window_spec(), &enricher_edge, &set)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(400));
        assert_eq!(resolved.meta.scope, ConfigScope::stage("enricher"));
        assert_eq!(resolved.meta.source, ConfigSource::File);
    }

    #[test]
    fn edge_ladder_binds_the_upstream_stage_and_edge_beats_stage() {
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::stage("a"),
            ConfigSource::File,
            ConfigValue::U64(500),
        );
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::edge("a", "b"),
            ConfigSource::File,
            ConfigValue::U64(2000),
        );
        // Edge entry wins on (a, b).
        let resolved = resolve_at(
            window_spec(),
            &ResolutionPoint::Edge {
                upstream: StageKey::from("a"),
                downstream: StageKey::from("b"),
            },
            &set,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(2000));
        assert_eq!(resolved.meta.scope, ConfigScope::edge("a", "b"));

        // (a, c) falls to a's stage rung.
        let resolved = resolve_at(
            window_spec(),
            &ResolutionPoint::Edge {
                upstream: StageKey::from("a"),
                downstream: StageKey::from("c"),
            },
            &set,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(500));

        // Fan-in: (x, b) does NOT see b's or a's values; the stage rung
        // binds the upstream stage.
        let resolved = resolve_at(
            window_spec(),
            &ResolutionPoint::Edge {
                upstream: StageKey::from("x"),
                downstream: StageKey::from("b"),
            },
            &set,
        )
        .unwrap();
        assert!(
            resolved.is_none(),
            "window is OptionalAbsent with no candidates"
        );
    }

    #[test]
    fn defaults_report_default_source_at_global_scope() {
        let set = CandidateSet::default();
        let resolved = resolve_at(
            lineage_spec(),
            &ResolutionPoint::Stage(StageKey::from("s")),
            &set,
        )
        .unwrap()
        .unwrap();
        assert_eq!(resolved.value, ConfigValue::U64(100));
        assert_eq!(resolved.meta.source, ConfigSource::Default);
        assert_eq!(resolved.meta.scope, ConfigScope::Global);
    }

    #[test]
    fn required_knob_failure_names_the_edge_and_every_scope() {
        let spec = KnobSpec {
            key_path: "runtime.backpressure.window",
            file_path: None,
            value_type: KnobType::U64 {
                min: 1,
                max: u64::MAX,
            },
            target: KnobTarget::Edge {
                stage_binding: EdgeEndpoint::Upstream,
            },
            default: KnobDefault::Required,
            mutability: Mutability::Restartful,
            redaction: Redaction::Plain,
            env: EnvBinding::Canonical,
        };
        let err = resolve_at(
            &spec,
            &ResolutionPoint::Edge {
                upstream: StageKey::from("enricher"),
                downstream: StageKey::from("merger"),
            },
            &CandidateSet::default(),
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(message.starts_with(
            "config error at runtime.backpressure.window: required knob unresolved for edge enricher|>merger"
        ));
        assert!(message.contains("[runtime.backpressure.stages.enricher.edges.merger]"));
        assert!(message
            .contains("stage scope for 'enricher' (file [runtime.backpressure.stages.enricher]"));
        assert!(message.contains("[runtime.backpressure.flow]"));
        assert!(message.contains(
            "global scope (file [runtime.backpressure], env OBZENFLOW_RUNTIME_BACKPRESSURE_WINDOW)"
        ));
    }

    #[test]
    fn materialize_validates_topology_and_collects_the_footgun_warning() {
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::stage("sink"),
            ConfigSource::File,
            ConfigValue::U64(100),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let ctx = FlowResolutionContext {
            flow_name: "f".to_string(),
            stages: BTreeSet::from([StageKey::from("src"), StageKey::from("sink")]),
            edges: BTreeSet::from([(StageKey::from("src"), StageKey::from("sink"))]),
            dsl: Default::default(),
        };
        let effective = materialize_flow_config(&snapshot, ctx).unwrap();
        assert_eq!(effective.warnings().len(), 1);
        assert!(effective.warnings()[0].contains("no outgoing edges"));

        // Unknown stage in a scoped entry fails the build.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.max_lineage_depth",
            ConfigScope::stage("ghost"),
            ConfigSource::File,
            ConfigValue::U64(5),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let ctx = FlowResolutionContext {
            flow_name: "f".to_string(),
            stages: BTreeSet::from([StageKey::from("src")]),
            edges: BTreeSet::new(),
            dsl: Default::default(),
        };
        let err = materialize_flow_config(&snapshot, ctx).unwrap_err();
        assert!(err
            .to_string()
            .contains("configured stage does not exist: 'ghost'"));
    }

    fn one_edge_ctx() -> FlowResolutionContext {
        FlowResolutionContext {
            flow_name: "f".to_string(),
            stages: BTreeSet::from([StageKey::from("src"), StageKey::from("sink")]),
            edges: BTreeSet::from([(StageKey::from("src"), StageKey::from("sink"))]),
            dsl: Default::default(),
        }
    }

    #[test]
    fn enforce_without_window_or_stall_timeout_fails_the_build() {
        // mode=enforce with nothing else: the window is required-where-enforce.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::Text("enforce".to_string()),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let err = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("runtime.backpressure.window") && message.contains("edge src|>sink"),
            "window required where enforce resolves: {message}"
        );

        // A window alone still fails: the stall timeout is equally required.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::Text("enforce".to_string()),
        );
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::U64(1000),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let err = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("runtime.backpressure.stall_timeout_ms")
                && message.contains("edge src|>sink"),
            "stall timeout required where enforce resolves: {message}"
        );

        // Both values present: the build succeeds and the edge resolves.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::Text("enforce".to_string()),
        );
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::U64(1000),
        );
        admit(
            &mut set,
            "runtime.backpressure.stall_timeout_ms",
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::U64(30_000),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let effective = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap();
        let (mode, source) =
            effective.backpressure_mode_for(&StageKey::from("src"), &StageKey::from("sink"));
        assert_eq!(mode, crate::runtime_config::BackpressureMode::Enforce);
        assert_eq!(source, ConfigSource::File);
        assert_eq!(
            effective
                .backpressure_stall_timeout_for(&StageKey::from("src"), &StageKey::from("sink")),
            Some(30_000)
        );
        assert!(effective.warnings().is_empty());
    }

    #[test]
    fn track_needs_no_window_and_an_off_window_draws_the_diagnostic() {
        // mode=track builds with no window at all.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::edge("src", "sink"),
            ConfigSource::File,
            ConfigValue::Text("track".to_string()),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let effective = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap();
        let (mode, _) =
            effective.backpressure_mode_for(&StageKey::from("src"), &StageKey::from("sink"));
        assert_eq!(mode, crate::runtime_config::BackpressureMode::Track);
        assert!(effective.warnings().is_empty());

        // A window resolved where the mode stays off governs nothing.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::edge("src", "sink"),
            ConfigSource::File,
            ConfigValue::U64(1000),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let effective = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap();
        assert!(
            effective
                .warnings()
                .iter()
                .any(|warning| warning.contains("has no effect") && warning.contains("src|>sink")),
            "off-with-window diagnostic expected: {:?}",
            effective.warnings()
        );
    }

    #[test]
    fn a_file_entry_unsets_a_dsl_declared_enforce_at_the_same_scope() {
        // The clause desugar: DSL declares enforce + window + stall at stage
        // scope. An environment file sets mode=off at the same scope, which
        // outranks the DSL tier: enforcement is off with no build error, and
        // the now-inert window draws the diagnostic.
        let mut dsl = crate::runtime_config::DslCandidates::default();
        dsl.declare(
            "runtime.backpressure.mode",
            ConfigScope::stage("src"),
            ConfigValue::Text("enforce".to_string()),
        );
        dsl.declare(
            "runtime.backpressure.window",
            ConfigScope::stage("src"),
            ConfigValue::U64(1000),
        );
        dsl.declare(
            "runtime.backpressure.stall_timeout_ms",
            ConfigScope::stage("src"),
            ConfigValue::U64(30_000),
        );

        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::stage("src"),
            ConfigSource::File,
            ConfigValue::Text("off".to_string()),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let mut ctx = one_edge_ctx();
        ctx.dsl = dsl;
        let effective = materialize_flow_config(&snapshot, ctx).unwrap();
        let (mode, source) =
            effective.backpressure_mode_for(&StageKey::from("src"), &StageKey::from("sink"));
        assert_eq!(mode, crate::runtime_config::BackpressureMode::Off);
        assert_eq!(source, ConfigSource::File, "operators override code");
        assert!(effective
            .warnings()
            .iter()
            .any(|warning| warning.contains("has no effect")));
    }
}
