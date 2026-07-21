// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The resolution ladder: scope-major, source-minor, post-ladder default
//! (FLOWIP-010 §2, §4c). One implementation serves startup views, flow-build
//! materialization, and the introspection surface.

use super::candidates::{CandidateSet, ConfigValue};
use super::error::ConfigResolveError;
use super::flow_view::{BackpressureMode, FlowEffectiveConfig, FlowResolutionContext};
use super::model::{Resolved, ResolvedRuntimeConfig};
use super::schema::{knob_registry, EdgeEndpoint, KnobDefault, KnobSpec, KnobTarget};
use obzenflow_core::config::{
    ConfigAddress, ConfigScope, ConfigSource, ConfigSubject, ConfigValueMeta,
};
use obzenflow_core::event::EffectType;
use obzenflow_core::StageKey;
use std::collections::BTreeMap;

/// Where a knob is being resolved. Points are generated from the knob's
/// target: Global-target knobs resolve at `Global`, Stage-target knobs once
/// per stage, Edge-target knobs once per directed edge.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResolutionPoint {
    Global,
    Flow,
    Stage(StageKey),
    Effect {
        stage: StageKey,
        effect_type: EffectType,
    },
    Edge {
        upstream: StageKey,
        downstream: StageKey,
    },
}

impl ResolutionPoint {
    /// The address this point's resolution is stored under.
    pub fn scope(&self) -> ConfigScope {
        match self {
            Self::Global => ConfigScope::Global,
            Self::Flow => ConfigScope::Flow,
            Self::Stage(stage) => ConfigScope::Stage {
                stage: stage.clone(),
            },
            Self::Effect { stage, .. } => ConfigScope::Stage {
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
            Self::Effect { stage, effect_type } => format!(
                "effect '{}' on stage '{}'",
                effect_type.as_str(),
                stage.as_str()
            ),
            Self::Edge {
                upstream,
                downstream,
            } => format!("edge {}|>{}", upstream.as_str(), downstream.as_str()),
        }
    }
}

/// Specificity rank of a resolved value's winning scope, coarsest first. Used
/// to tell an inherited default (broader) from an override (more specific)
/// when deciding whether a shadowed value is stray config or intentional.
fn scope_rank(scope: &ConfigScope) -> u8 {
    match scope {
        ConfigScope::Global => 0,
        ConfigScope::Flow => 1,
        ConfigScope::Stage { .. } => 2,
        ConfigScope::Edge { .. } => 3,
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
        (KnobTarget::Stage | KnobTarget::StageOrEffect, ResolutionPoint::Stage(stage)) => vec![
            ConfigScope::Stage {
                stage: stage.clone(),
            },
            ConfigScope::Flow,
            ConfigScope::Global,
        ],
        (KnobTarget::Effect | KnobTarget::StageOrEffect, ResolutionPoint::Effect { stage, .. }) => {
            vec![
                ConfigScope::Stage {
                    stage: stage.clone(),
                },
                ConfigScope::Flow,
                ConfigScope::Global,
            ]
        }
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

fn value_for_source<'a>(
    slots: &'a super::candidates::SourceSlots,
    source: &ConfigSource,
) -> Option<&'a ConfigValue> {
    match source {
        ConfigSource::Cli => slots.cli.as_ref(),
        ConfigSource::File => slots.file.as_ref(),
        ConfigSource::Env => slots.env.as_ref(),
        ConfigSource::Dsl => slots.dsl.as_ref(),
        _ => None,
    }
}

/// Resolve one knob at one point. `Ok(None)` is a legal outcome only for
/// `OptionalAbsent` knobs; `Required` knobs error with the full supply help.
pub fn resolve_at(
    spec: &KnobSpec,
    point: &ResolutionPoint,
    set: &CandidateSet,
) -> Result<Option<Resolved<ConfigValue>>, ConfigResolveError> {
    for scope in ladder(spec, point) {
        let broadcast = ConfigAddress::unqualified(scope.clone());
        let exact = match (point, &scope) {
            (ResolutionPoint::Effect { effect_type, .. }, ConfigScope::Stage { .. }) => {
                Some(ConfigAddress {
                    scope: scope.clone(),
                    subject: ConfigSubject::Effect {
                        effect_type: effect_type.clone(),
                    },
                })
            }
            _ => None,
        };

        // Operator supremacy is explicit: source precedence is evaluated
        // before subject specificity. Within one source, exact beats broadcast.
        for source in [
            ConfigSource::Cli,
            ConfigSource::File,
            ConfigSource::Env,
            ConfigSource::Dsl,
        ] {
            for address in exact.iter().chain(std::iter::once(&broadcast)) {
                if let Some(value) = set
                    .get_at(spec.key_path, address)
                    .and_then(|slots| value_for_source(slots, &source))
                {
                    return Ok(Some(Resolved {
                        value: value.clone(),
                        meta: ConfigValueMeta {
                            source: source.clone(),
                            scope: scope.clone(),
                            subject: address.subject.clone(),
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
                subject: ConfigSubject::Unqualified,
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
        let broadcast = ConfigAddress::unqualified(scope.clone());
        let file = spec.file_address(&broadcast);
        let part = match &scope {
            ConfigScope::Edge { .. } => {
                format!("edge scope (file {file}, DSL edge declaration)")
            }
            ConfigScope::Stage { stage } if matches!(point, ResolutionPoint::Effect { .. }) => {
                let ResolutionPoint::Effect { effect_type, .. } = point else {
                    unreachable!("guarded by the effect-point match")
                };
                let exact =
                    spec.file_address(&ConfigAddress::effect(stage.clone(), effect_type.clone()));
                format!(
                    "stage scope for '{}' (exact-effect file {exact}, broadcast file {file}, \
                     DSL effect or stage declaration)",
                    stage.as_str()
                )
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

/// Phase B: merge DSL candidates, validate scoped entries against the
/// topology (010f-shaped diagnostics), and resolve every registered knob at
/// every applicable point (§4c). Returns the immutable per-flow view.
pub fn materialize_flow_config(
    snapshot: &ResolvedRuntimeConfig,
    ctx: FlowResolutionContext,
) -> Result<FlowEffectiveConfig, ConfigResolveError> {
    // Applicability comes from the surviving factories' explicit consumption
    // declarations, never from which defaults happened to be emitted for the
    // selected mode. The index is consumed during this build and is not a
    // runtime policy registry or state authority.
    let effect_consumers = ctx.dsl.effect_consumers();
    let stage_consumers = ctx.dsl.stage_consumers();

    let mut working = snapshot.candidates().clone();
    for candidate in ctx.dsl.entries() {
        working.admit(candidate.clone())?;
    }

    let mut warnings = Vec::new();
    for spec in knob_registry() {
        for address in working.addresses_for(spec.key_path) {
            match &address.scope {
                ConfigScope::Stage { stage } => {
                    if !ctx.stages.contains(stage) {
                        return Err(ConfigResolveError::UnknownStage {
                            key_path: spec.key_path.to_string(),
                            stage: stage.as_str().to_string(),
                            known: ctx.stages.iter().map(|s| s.as_str().to_string()).collect(),
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
                            known: ctx
                                .edges
                                .iter()
                                .map(|(up, down)| format!("{}|>{}", up.as_str(), down.as_str()))
                                .collect(),
                        });
                    }
                }
                ConfigScope::Global | ConfigScope::Flow => {}
            }

            if let ConfigSubject::Effect { effect_type } = &address.subject {
                let ConfigScope::Stage { stage } = &address.scope else {
                    unreachable!("candidate admission restricts exact effect subjects to stages")
                };
                let known: Vec<String> = ctx
                    .declared_effects
                    .get(stage)
                    .into_iter()
                    .flat_map(|effects| effects.iter())
                    .map(|effect| effect.as_str().to_string())
                    .collect();
                if !ctx
                    .declared_effects
                    .get(stage)
                    .is_some_and(|effects| effects.contains(effect_type))
                {
                    return Err(ConfigResolveError::UnknownEffect {
                        key_path: spec.key_path.to_string(),
                        stage: stage.as_str().to_string(),
                        effect_type: effect_type.as_str().to_string(),
                        known,
                    });
                }
                if !effect_consumers
                    .get(spec.key_path)
                    .is_some_and(|points| points.contains(&(stage.clone(), effect_type.clone())))
                {
                    return Err(ConfigResolveError::UnattachedEffectSubject {
                        key_path: spec.key_path.to_string(),
                        stage: stage.as_str().to_string(),
                        effect_type: effect_type.as_str().to_string(),
                    });
                }
            }
        }
    }
    for warning in &warnings {
        tracing::warn!("{warning}");
    }

    let mut values: BTreeMap<String, BTreeMap<ResolutionPoint, Resolved<ConfigValue>>> =
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
            KnobTarget::Effect => effect_consumers
                .get(spec.key_path)
                .into_iter()
                .flat_map(|points| points.iter())
                .map(|(stage, effect_type)| ResolutionPoint::Effect {
                    stage: stage.clone(),
                    effect_type: effect_type.clone(),
                })
                .collect(),
            KnobTarget::StageOrEffect => {
                let mut points: Vec<_> = stage_consumers
                    .get(spec.key_path)
                    .into_iter()
                    .flat_map(|points| points.iter())
                    .map(|stage| ResolutionPoint::Stage(stage.clone()))
                    .collect();
                points.extend(
                    effect_consumers
                        .get(spec.key_path)
                        .into_iter()
                        .flat_map(|effect_points| effect_points.iter())
                        .map(|(stage, effect_type)| ResolutionPoint::Effect {
                            stage: stage.clone(),
                            effect_type: effect_type.clone(),
                        }),
                );
                points
            }
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
                    .insert(point, resolved);
            }
        }
    }

    // FLOWIP-115e required-where-enforce: an edge whose mode resolves to
    // `enforce` must resolve a window and a stall timeout, or the build
    // fails naming the edge and the scopes that may supply them. A window
    // resolved where the mode stays off or track governs nothing and draws
    // a diagnostic.
    for (upstream, downstream) in &ctx.edges {
        let address = ResolutionPoint::Edge {
            upstream: upstream.clone(),
            downstream: downstream.clone(),
        };
        let mode_resolved = values
            .get("runtime.backpressure.mode")
            .and_then(|per_scope| per_scope.get(&address));
        let mode = mode_resolved
            .and_then(|resolved| resolved.value.as_text())
            .and_then(BackpressureMode::from_token)
            .unwrap_or(BackpressureMode::Off);
        if mode == BackpressureMode::Enforce {
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
        } else if let Some(window_resolved) = values
            .get("runtime.backpressure.window")
            .and_then(|per_scope| per_scope.get(&address))
        {
            // A window resolved on a non-enforce edge governs nothing, but that
            // is only a mistake worth flagging for stray config authored at or
            // below the mode's scope. A flow- or global-wide window that a
            // more-specific override turned off or track is intentional
            // layering, so suppress the warning there.
            let window_rank = scope_rank(&window_resolved.meta.scope);
            let mode_rank = mode_resolved.map_or(0, |resolved| scope_rank(&resolved.meta.scope));
            if window_rank >= mode_rank {
                let warning = format!(
                    "config warning at runtime.backpressure.window: a window resolved for \
                     edge {}|>{} whose mode is '{}'; it has no effect until the mode \
                     resolves to '{}'",
                    upstream.as_str(),
                    downstream.as_str(),
                    mode.as_token(),
                    BackpressureMode::Enforce.as_token()
                );
                tracing::warn!("{warning}");
                warnings.push(warning);
            }
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
        set.admit(ScopedCandidate::unqualified(key, scope, source, value))
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
            declared_effects: Default::default(),
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
            declared_effects: Default::default(),
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
            declared_effects: Default::default(),
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
    fn an_inherited_window_shadowed_by_a_more_specific_off_draws_no_warning() {
        // A flow-wide enforce policy with a stage that overrides mode=off. The
        // inherited window governs nothing on that stage's edges, but the
        // override is intentional layering, not stray config, so no warning
        // fires. This is the common authoring shape: one flow default, sparse
        // per-stage exceptions.
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
        // `src` (the writing endpoint of the only edge) turns its outgoing
        // edges off; the flow window is inherited but shadowed.
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::stage("src"),
            ConfigSource::File,
            ConfigValue::Text("off".to_string()),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let effective = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap();
        let (mode, _) =
            effective.backpressure_mode_for(&StageKey::from("src"), &StageKey::from("sink"));
        assert_eq!(mode, crate::runtime_config::BackpressureMode::Off);
        assert!(
            effective.warnings().is_empty(),
            "an inherited window shadowed by a more-specific off must not warn: {:?}",
            effective.warnings()
        );
    }

    #[test]
    fn a_window_authored_at_the_same_scope_as_off_still_warns() {
        // Contrast to the inherited case: a window and an off mode authored at
        // the same stage scope is stray config, so the diagnostic still fires.
        let mut set = CandidateSet::default();
        admit(
            &mut set,
            "runtime.backpressure.mode",
            ConfigScope::stage("src"),
            ConfigSource::File,
            ConfigValue::Text("off".to_string()),
        );
        admit(
            &mut set,
            "runtime.backpressure.window",
            ConfigScope::stage("src"),
            ConfigSource::File,
            ConfigValue::U64(500),
        );
        let snapshot = ResolvedRuntimeConfig::new(set);
        let effective = materialize_flow_config(&snapshot, one_edge_ctx()).unwrap();
        assert!(
            effective
                .warnings()
                .iter()
                .any(|warning| warning.contains("has no effect") && warning.contains("src|>sink")),
            "same-scope window+off is stray config and must warn: {:?}",
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

    fn effect_context(
        stage_effects: &[(&str, &[&str])],
        mut dsl: crate::runtime_config::DslCandidates,
    ) -> FlowResolutionContext {
        // Most resolver fixtures model a default-producing effect factory. Its
        // consumed key is the same key as the fixture candidate; the optional
        // key test below adds its independent consumption declaration itself.
        let default_consumers: Vec<_> = dsl
            .entries()
            .iter()
            .filter_map(
                |candidate| match (&candidate.address.scope, &candidate.address.subject) {
                    (ConfigScope::Stage { stage }, ConfigSubject::Effect { effect_type }) => {
                        Some((
                            candidate.key_path.clone(),
                            stage.clone(),
                            effect_type.clone(),
                        ))
                    }
                    _ => None,
                },
            )
            .collect();
        for (key_path, stage, effect_type) in default_consumers {
            dsl.declare_effect_consumption(key_path, stage, effect_type);
        }
        let declared_effects = stage_effects
            .iter()
            .map(|(stage, effects)| {
                (
                    StageKey::from(*stage),
                    effects
                        .iter()
                        .map(|effect| EffectType::from(*effect))
                        .collect(),
                )
            })
            .collect();
        FlowResolutionContext {
            flow_name: "effect_config_test".to_string(),
            stages: stage_effects
                .iter()
                .map(|(stage, _)| StageKey::from(*stage))
                .collect(),
            edges: BTreeSet::new(),
            declared_effects,
            dsl,
        }
    }

    #[test]
    fn source_precedes_subject_within_one_scope_by_explicit_operator_supremacy() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let stage = StageKey::from("authorize_payment");
        let authorize = EffectType::from("payments.authorize");
        let refund = EffectType::from("payments.refund");

        let mut dsl = crate::runtime_config::DslCandidates::default();
        dsl.declare_for_effect(key, stage.clone(), authorize.clone(), ConfigValue::F64(2.0));
        dsl.declare_for_effect(key, stage.clone(), refund.clone(), ConfigValue::F64(3.0));

        let mut file = CandidateSet::default();
        file.admit(ScopedCandidate::unqualified(
            key,
            ConfigScope::stage(stage.clone()),
            ConfigSource::File,
            ConfigValue::F64(8.0),
        ))
        .unwrap();
        file.admit(ScopedCandidate {
            key_path: key.to_string(),
            address: ConfigAddress::effect(stage.clone(), authorize.clone()),
            source: ConfigSource::File,
            value: ConfigValue::F64(5.0),
        })
        .unwrap();

        let effective = materialize_flow_config(
            &ResolvedRuntimeConfig::new(file),
            effect_context(
                &[(
                    "authorize_payment",
                    &["payments.authorize", "payments.refund"],
                )],
                dsl,
            ),
        )
        .unwrap();

        let authorize_value = effective.effect_value(key, &stage, &authorize).unwrap();
        assert_eq!(authorize_value.value.as_f64(), Some(5.0));
        assert_eq!(authorize_value.meta.source, ConfigSource::File);
        assert_eq!(
            authorize_value.meta.subject,
            ConfigSubject::Effect {
                effect_type: authorize.clone()
            }
        );

        let refund_value = effective.effect_value(key, &stage, &refund).unwrap();
        assert_eq!(refund_value.value.as_f64(), Some(8.0));
        assert_eq!(refund_value.meta.source, ConfigSource::File);
        assert_eq!(refund_value.meta.subject, ConfigSubject::Unqualified);

        let rows: Vec<_> = effective
            .manifest_evidence()
            .values
            .into_iter()
            .filter(|row| row.key_path == key)
            .collect();
        assert_eq!(
            rows.len(),
            2,
            "equal or inherited values remain per-effect rows"
        );
        assert!(rows.iter().all(|row| row.resolved_for.is_some()));
    }

    #[test]
    fn a_more_specific_scope_still_beats_a_broader_operator_source() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let stage = StageKey::from("authorize_payment");
        let effect_type = EffectType::from("payments.authorize");
        let mut dsl = crate::runtime_config::DslCandidates::default();
        dsl.declare_for_effect(
            key,
            stage.clone(),
            effect_type.clone(),
            ConfigValue::F64(4.0),
        );
        let mut file = CandidateSet::default();
        file.admit(ScopedCandidate::unqualified(
            key,
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::F64(50.0),
        ))
        .unwrap();

        let effective = materialize_flow_config(
            &ResolvedRuntimeConfig::new(file),
            effect_context(&[("authorize_payment", &["payments.authorize"])], dsl),
        )
        .unwrap();
        let resolved = effective.effect_value(key, &stage, &effect_type).unwrap();
        assert_eq!(resolved.value.as_f64(), Some(4.0));
        assert_eq!(resolved.meta.scope, ConfigScope::stage(stage));
        assert_eq!(resolved.meta.source, ConfigSource::Dsl);
    }

    #[test]
    fn equal_broadcast_values_keep_distinct_resolved_for_rows() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let stage = StageKey::from("authorize_payment");
        let mut dsl = crate::runtime_config::DslCandidates::default();
        for effect in ["payments.authorize", "payments.refund"] {
            dsl.declare_for_effect(
                key,
                stage.clone(),
                EffectType::from(effect),
                ConfigValue::F64(1.0),
            );
        }
        let mut file = CandidateSet::default();
        file.admit(ScopedCandidate::unqualified(
            key,
            ConfigScope::stage(stage),
            ConfigSource::File,
            ConfigValue::F64(8.0),
        ))
        .unwrap();
        let effective = materialize_flow_config(
            &ResolvedRuntimeConfig::new(file),
            effect_context(
                &[(
                    "authorize_payment",
                    &["payments.refund", "payments.authorize"],
                )],
                dsl,
            ),
        )
        .unwrap();
        let rows: Vec<_> = effective
            .manifest_evidence()
            .values
            .into_iter()
            .filter(|row| row.key_path == key)
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].value, rows[1].value);
        assert_ne!(rows[0].resolved_for, rows[1].resolved_for);
        assert!(rows
            .iter()
            .all(|row| row.winner_subject == Some(ConfigSubject::Unqualified)));
    }

    #[test]
    fn same_effect_type_on_two_stages_has_independent_addresses() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let effect_type = EffectType::from("payments.authorize");
        let mut dsl = crate::runtime_config::DslCandidates::default();
        dsl.declare_for_effect(key, "primary", effect_type.clone(), ConfigValue::F64(3.0));
        dsl.declare_for_effect(key, "fallback", effect_type.clone(), ConfigValue::F64(7.0));
        let effective = materialize_flow_config(
            &ResolvedRuntimeConfig::builtin_defaults(),
            effect_context(
                &[
                    ("fallback", &["payments.authorize"]),
                    ("primary", &["payments.authorize"]),
                ],
                dsl,
            ),
        )
        .unwrap();
        assert_eq!(
            effective
                .effect_value(key, &StageKey::from("primary"), &effect_type)
                .and_then(|value| value.value.as_f64()),
            Some(3.0)
        );
        assert_eq!(
            effective
                .effect_value(key, &StageKey::from("fallback"), &effect_type)
                .and_then(|value| value.value.as_f64()),
            Some(7.0)
        );
    }

    #[test]
    fn optional_limiter_burst_is_applicable_without_an_invented_dsl_value() {
        let rate_key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let burst_key = crate::runtime_config::RATE_LIMITER_BURST_CAPACITY_KEY;
        let stage = StageKey::from("payments");
        let effect_type = EffectType::from("payments.authorize");
        let mut dsl = crate::runtime_config::DslCandidates::default();
        dsl.declare_for_effect(
            rate_key,
            stage.clone(),
            effect_type.clone(),
            ConfigValue::F64(10.0),
        );
        dsl.declare_effect_consumption(burst_key, stage.clone(), effect_type.clone());

        let without_burst = materialize_flow_config(
            &ResolvedRuntimeConfig::builtin_defaults(),
            effect_context(&[("payments", &["payments.authorize"])], dsl.clone()),
        )
        .unwrap();
        assert!(without_burst
            .effect_value(burst_key, &stage, &effect_type)
            .is_none());
        assert!(without_burst
            .manifest_evidence()
            .values
            .iter()
            .all(|row| row.key_path != burst_key));

        let mut file = CandidateSet::default();
        file.admit(ScopedCandidate {
            key_path: burst_key.to_string(),
            address: ConfigAddress::effect(stage.clone(), effect_type.clone()),
            source: ConfigSource::File,
            value: ConfigValue::F64(25.0),
        })
        .unwrap();
        let with_file_burst = materialize_flow_config(
            &ResolvedRuntimeConfig::new(file),
            effect_context(&[("payments", &["payments.authorize"])], dsl.clone()),
        )
        .unwrap();
        let burst = with_file_burst
            .effect_value(burst_key, &stage, &effect_type)
            .expect("the surviving limiter consumes the optional burst key");
        assert_eq!(burst.value.as_f64(), Some(25.0));
        assert_eq!(burst.meta.source, ConfigSource::File);
        let exact_row = with_file_burst
            .manifest_evidence()
            .values
            .into_iter()
            .find(|row| row.key_path == burst_key)
            .expect("the supplied optional value should use the normal evidence projection");
        assert_eq!(exact_row.source, "file");
        assert_eq!(
            exact_row.winner_subject,
            Some(ConfigSubject::Effect {
                effect_type: effect_type.clone(),
            })
        );
        assert_eq!(
            exact_row.resolved_for,
            Some(obzenflow_core::config::ResolvedForDoc::Effect {
                stage: stage.as_str().to_string(),
                effect_type: effect_type.as_str().to_string(),
            })
        );

        let mut broadcast_file = CandidateSet::default();
        broadcast_file
            .admit(ScopedCandidate::unqualified(
                burst_key,
                ConfigScope::stage(stage.clone()),
                ConfigSource::File,
                ConfigValue::F64(30.0),
            ))
            .unwrap();
        let with_broadcast_burst = materialize_flow_config(
            &ResolvedRuntimeConfig::new(broadcast_file),
            effect_context(&[("payments", &["payments.authorize"])], dsl),
        )
        .unwrap();
        assert_eq!(
            with_broadcast_burst
                .effect_value(burst_key, &stage, &effect_type)
                .and_then(|resolved| resolved.value.as_f64()),
            Some(30.0)
        );
        let broadcast_row = with_broadcast_burst
            .manifest_evidence()
            .values
            .into_iter()
            .find(|row| row.key_path == burst_key)
            .expect("the inherited optional value should retain exact attribution");
        assert_eq!(broadcast_row.source, "file");
        assert_eq!(
            broadcast_row.winner_subject,
            Some(ConfigSubject::Unqualified)
        );
        assert_eq!(
            broadcast_row.resolved_for,
            Some(obzenflow_core::config::ResolvedForDoc::Effect {
                stage: stage.as_str().to_string(),
                effect_type: effect_type.as_str().to_string(),
            })
        );
    }

    #[test]
    fn attached_and_unattached_effects_remain_distinct_for_broadcast_and_exact_addresses() {
        let key = crate::runtime_config::RATE_LIMITER_BURST_CAPACITY_KEY;
        let stage = StageKey::from("payments");
        let attached = EffectType::from("payments.authorize");
        let unattached = EffectType::from("payments.refund");
        let mut dsl = crate::runtime_config::DslCandidates::default();
        dsl.declare_effect_consumption(key, stage.clone(), attached.clone());

        let mut broadcast = CandidateSet::default();
        broadcast
            .admit(ScopedCandidate::unqualified(
                key,
                ConfigScope::stage(stage.clone()),
                ConfigSource::File,
                ConfigValue::F64(20.0),
            ))
            .unwrap();
        let broadcast_effective = materialize_flow_config(
            &ResolvedRuntimeConfig::new(broadcast),
            effect_context(
                &[("payments", &["payments.authorize", "payments.refund"])],
                dsl.clone(),
            ),
        )
        .unwrap();
        assert_eq!(
            broadcast_effective
                .effect_value(key, &stage, &attached)
                .and_then(|resolved| resolved.value.as_f64()),
            Some(20.0)
        );
        assert!(broadcast_effective
            .effect_value(key, &stage, &unattached)
            .is_none());
        assert_eq!(
            broadcast_effective
                .manifest_evidence()
                .values
                .into_iter()
                .filter(|row| row.key_path == key)
                .count(),
            1
        );

        let exact_snapshot = |effect_type: EffectType| {
            let mut candidates = CandidateSet::default();
            candidates
                .admit(ScopedCandidate {
                    key_path: key.to_string(),
                    address: ConfigAddress::effect(stage.clone(), effect_type),
                    source: ConfigSource::File,
                    value: ConfigValue::F64(25.0),
                })
                .unwrap();
            ResolvedRuntimeConfig::new(candidates)
        };
        let exact_attached = materialize_flow_config(
            &exact_snapshot(attached.clone()),
            effect_context(
                &[("payments", &["payments.authorize", "payments.refund"])],
                dsl.clone(),
            ),
        )
        .unwrap();
        assert_eq!(
            exact_attached
                .effect_value(key, &stage, &attached)
                .and_then(|resolved| resolved.value.as_f64()),
            Some(25.0)
        );

        let error = materialize_flow_config(
            &exact_snapshot(unattached.clone()),
            effect_context(
                &[("payments", &["payments.authorize", "payments.refund"])],
                dsl,
            ),
        )
        .expect_err("a declared but unattached exact subject must remain rejected");
        assert!(matches!(
            error,
            ConfigResolveError::UnattachedEffectSubject { effect_type, .. }
                if effect_type == unattached.as_str()
        ));
    }

    #[test]
    fn unknown_and_declared_but_unattached_effects_have_distinct_errors() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let exact_file = |effect_type: &str| {
            let mut set = CandidateSet::default();
            set.admit(ScopedCandidate {
                key_path: key.to_string(),
                address: ConfigAddress::effect("payments", effect_type),
                source: ConfigSource::File,
                value: ConfigValue::F64(5.0),
            })
            .unwrap();
            ResolvedRuntimeConfig::new(set)
        };

        let unknown = materialize_flow_config(
            &exact_file("payments.old_authorize"),
            effect_context(&[("payments", &["payments.authorize"])], Default::default()),
        )
        .unwrap_err();
        assert!(matches!(
            unknown,
            ConfigResolveError::UnknownEffect { effect_type, known, .. }
                if effect_type == "payments.old_authorize"
                    && known == vec!["payments.authorize".to_string()]
        ));

        let unattached = materialize_flow_config(
            &exact_file("payments.authorize"),
            effect_context(&[("payments", &["payments.authorize"])], Default::default()),
        )
        .unwrap_err();
        assert!(matches!(
            unattached,
            ConfigResolveError::UnattachedEffectSubject { effect_type, .. }
                if effect_type == "payments.authorize"
        ));
    }
}
