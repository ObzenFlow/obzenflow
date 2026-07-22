// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Scoped, type-validated candidates awaiting resolution.
//!
//! Admission enforces the first-pass source matrix (§2 lock): CLI and env
//! candidates are global-only; file candidates land wherever the raw struct
//! shapes admitted them; DSL candidates carry their declaration-site scope
//! (§4a lock), one per knob and site.

use super::error::ConfigResolveError;
use super::schema::{knob, KnobTarget};
use obzenflow_core::config::{ConfigAddress, ConfigScope, ConfigSource, ConfigSubject};
use obzenflow_core::event::EffectType;
use std::collections::{BTreeMap, BTreeSet};

/// A type-validated configuration value.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    Bool(bool),
    U64(u64),
    F64(f64),
    Text(String),
}

impl ConfigValue {
    pub fn type_label(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::U64(_) => "integer",
            Self::F64(_) => "float",
            Self::Text(_) => "text",
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Bool(v) => serde_json::json!(v),
            Self::U64(v) => serde_json::json!(v),
            Self::F64(v) => serde_json::json!(v),
            Self::Text(v) => serde_json::json!(v),
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(v) => Some(v),
            _ => None,
        }
    }
}

/// One candidate at one protected-unit-qualified address from one source.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedCandidate {
    pub key_path: String,
    pub address: ConfigAddress,
    pub source: ConfigSource,
    pub value: ConfigValue,
}

impl ScopedCandidate {
    pub fn unqualified(
        key_path: impl Into<String>,
        scope: ConfigScope,
        source: ConfigSource,
        value: ConfigValue,
    ) -> Self {
        Self {
            key_path: key_path.into(),
            address: ConfigAddress::unqualified(scope),
            source,
            value,
        }
    }
}

/// One scope-free DSL default contributed by a middleware factory.
#[derive(Debug, Clone, PartialEq)]
pub struct DslConfigDefault {
    pub key_path: &'static str,
    pub value: ConfigValue,
}

/// Per-scope source slots; the source-minor half of the ladder reads them
/// in precedence order (CLI > file > env > DSL).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceSlots {
    pub cli: Option<ConfigValue>,
    pub file: Option<ConfigValue>,
    pub env: Option<ConfigValue>,
    pub dsl: Option<ConfigValue>,
}

impl SourceSlots {
    fn slot_mut(&mut self, source: &ConfigSource) -> Option<&mut Option<ConfigValue>> {
        match source {
            ConfigSource::Cli => Some(&mut self.cli),
            ConfigSource::File => Some(&mut self.file),
            ConfigSource::Env => Some(&mut self.env),
            ConfigSource::Dsl => Some(&mut self.dsl),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cli.is_none() && self.file.is_none() && self.env.is_none() && self.dsl.is_none()
    }
}

/// The candidate table: knob key path -> qualified address -> source slots.
/// `BTreeMap` throughout for deterministic iteration and evidence ordering.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CandidateSet {
    by_knob: BTreeMap<String, BTreeMap<ConfigAddress, SourceSlots>>,
}

impl CandidateSet {
    /// Admit one candidate, enforcing the knob's scope matrix, the
    /// first-pass source matrix, type validation, and the one-candidate-
    /// per-(knob, scope, source) invariant (identical re-admission is a
    /// no-op; a differing value is a conflict).
    pub fn admit(&mut self, candidate: ScopedCandidate) -> Result<(), ConfigResolveError> {
        let ScopedCandidate {
            key_path,
            address,
            source,
            value,
        } = candidate;

        let spec = knob(&key_path).ok_or_else(|| ConfigResolveError::UnknownKnob {
            key_path: key_path.clone(),
        })?;

        if !address_admitted(spec.target, &address) {
            return Err(ConfigResolveError::ScopeNotAdmitted {
                key_path,
                address,
                reason: format!(
                    "the knob's target is {:?}; this scope/subject combination is invalid",
                    spec.target
                ),
            });
        }

        // §2 first-pass lock: scoped entries ride file and DSL only.
        if matches!(source, ConfigSource::Cli | ConfigSource::Env)
            && address != ConfigAddress::unqualified(ConfigScope::Global)
        {
            return Err(ConfigResolveError::ScopeNotAdmitted {
                key_path,
                address,
                reason: format!(
                    "{source} overrides are global-only in the first pass; use a file entry at this scope"
                ),
            });
        }

        if let Err(message) = spec.value_type.validate(&value) {
            return Err(ConfigResolveError::InvalidValue {
                key_path,
                address,
                message,
            });
        }

        let slots = self
            .by_knob
            .entry(key_path.clone())
            .or_default()
            .entry(address.clone())
            .or_default();
        let Some(slot) = slots.slot_mut(&source) else {
            return Err(ConfigResolveError::ScopeNotAdmitted {
                key_path,
                address,
                reason: format!("{source} is not an admissible candidate source"),
            });
        };
        match slot {
            Some(existing) if *existing != value => Err(ConfigResolveError::Conflict {
                key_path,
                address,
                source,
            }),
            _ => {
                *slot = Some(value);
                Ok(())
            }
        }
    }

    pub fn get(&self, key_path: &str, scope: &ConfigScope) -> Option<&SourceSlots> {
        self.get_at(key_path, &ConfigAddress::unqualified(scope.clone()))
    }

    pub fn get_at(&self, key_path: &str, address: &ConfigAddress) -> Option<&SourceSlots> {
        self.by_knob
            .get(key_path)
            .and_then(|addresses| addresses.get(address))
    }

    /// All qualified addresses carrying candidates for one knob.
    pub fn addresses_for(&self, key_path: &str) -> impl Iterator<Item = &ConfigAddress> {
        self.by_knob
            .get(key_path)
            .into_iter()
            .flat_map(|addresses| addresses.keys())
    }

    /// Knobs that carry at least one candidate.
    pub fn knob_paths(&self) -> impl Iterator<Item = &str> {
        self.by_knob.keys().map(String::as_str)
    }
}

/// Whether `scope` is admissible for a knob with `target` (§4c: the target
/// is the most specific admissible scope; coarser scopes always admit).
pub fn scope_admitted(target: KnobTarget, scope: &ConfigScope) -> bool {
    address_admitted(target, &ConfigAddress::unqualified(scope.clone()))
}

/// Explicit target × scope × subject compatibility matrix. Effect points are
/// not treated as a numeric specificity rung comparable with edges.
pub fn address_admitted(target: KnobTarget, address: &ConfigAddress) -> bool {
    if let ConfigSubject::Effect { .. } = &address.subject {
        return matches!(target, KnobTarget::Effect | KnobTarget::StageOrEffect)
            && matches!(address.scope, ConfigScope::Stage { .. });
    }

    let specificity = match &address.scope {
        ConfigScope::Global => 0,
        ConfigScope::Flow => 1,
        ConfigScope::Stage { .. } => 2,
        ConfigScope::Edge { .. } => 3,
    };
    let max = match target {
        KnobTarget::Global => 0,
        KnobTarget::Flow => 1,
        KnobTarget::Stage | KnobTarget::Effect | KnobTarget::StageOrEffect => 2,
        KnobTarget::Edge { .. } => 3,
    };
    specificity <= max
}

/// Build-only applicability supplied by surviving factories, independently of
/// the values those factories emit as DSL defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConfigConsumptionIndex {
    stage_consumers: BTreeMap<String, BTreeSet<obzenflow_core::StageKey>>,
    effect_consumers: BTreeMap<String, BTreeSet<(obzenflow_core::StageKey, EffectType)>>,
}

impl ConfigConsumptionIndex {
    fn consume_stage(
        &mut self,
        key_path: impl Into<String>,
        stage: impl Into<obzenflow_core::StageKey>,
    ) {
        self.stage_consumers
            .entry(key_path.into())
            .or_default()
            .insert(stage.into());
    }

    fn consume_effect(
        &mut self,
        key_path: impl Into<String>,
        stage: impl Into<obzenflow_core::StageKey>,
        effect_type: impl Into<EffectType>,
    ) {
        self.effect_consumers
            .entry(key_path.into())
            .or_default()
            .insert((stage.into(), effect_type.into()));
    }
}

/// DSL-declared candidates collected during flow build (§4a: declaration-
/// site scope, one candidate per knob and site; source is always `Dsl`). The
/// private consumption index travels in the same transient carrier but is not
/// itself a value candidate or runtime registry.
#[derive(Debug, Clone, Default)]
pub struct DslCandidates {
    entries: Vec<ScopedCandidate>,
    // Build-only applicability supplied by surviving factories. This is
    // deliberately separate from emitted values: an optional or inactive-mode
    // key can be consumable without inventing a DSL candidate for it.
    consumption: ConfigConsumptionIndex,
}

impl DslCandidates {
    pub fn declare(&mut self, key_path: impl Into<String>, scope: ConfigScope, value: ConfigValue) {
        self.entries.push(ScopedCandidate::unqualified(
            key_path,
            scope,
            ConfigSource::Dsl,
            value,
        ));
    }

    pub fn declare_for_effect(
        &mut self,
        key_path: impl Into<String>,
        stage: impl Into<obzenflow_core::StageKey>,
        effect_type: impl Into<EffectType>,
        value: ConfigValue,
    ) {
        self.entries.push(ScopedCandidate {
            key_path: key_path.into(),
            address: ConfigAddress::effect(stage, effect_type),
            source: ConfigSource::Dsl,
            value,
        });
    }

    pub fn entries(&self) -> &[ScopedCandidate] {
        &self.entries
    }

    /// Declare that one surviving factory consumes `key_path` at a stage
    /// resolution point. This is build metadata, not a runtime registry.
    #[doc(hidden)]
    pub fn declare_stage_consumption(
        &mut self,
        key_path: impl Into<String>,
        stage: impl Into<obzenflow_core::StageKey>,
    ) {
        self.consumption.consume_stage(key_path, stage);
    }

    /// Declare that one surviving factory consumes `key_path` at an exact
    /// effect resolution point. Configuration still cannot create the factory
    /// or any optional aggregate component.
    #[doc(hidden)]
    pub fn declare_effect_consumption(
        &mut self,
        key_path: impl Into<String>,
        stage: impl Into<obzenflow_core::StageKey>,
        effect_type: impl Into<EffectType>,
    ) {
        self.consumption
            .consume_effect(key_path, stage, effect_type);
    }

    pub(crate) fn stage_consumers(&self) -> &BTreeMap<String, BTreeSet<obzenflow_core::StageKey>> {
        &self.consumption.stage_consumers
    }

    pub(crate) fn effect_consumers(
        &self,
    ) -> &BTreeMap<String, BTreeSet<(obzenflow_core::StageKey, EffectType)>> {
        &self.consumption.effect_consumers
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        key: &str,
        scope: ConfigScope,
        source: ConfigSource,
        value: ConfigValue,
    ) -> ScopedCandidate {
        ScopedCandidate {
            key_path: key.to_string(),
            address: ConfigAddress::unqualified(scope),
            source,
            value,
        }
    }

    #[test]
    fn cli_and_env_candidates_are_global_only() {
        let mut set = CandidateSet::default();
        let err = set
            .admit(candidate(
                "runtime.max_lineage_depth",
                ConfigScope::stage("enricher"),
                ConfigSource::Env,
                ConfigValue::U64(5),
            ))
            .unwrap_err();
        assert!(err.to_string().contains("global-only in the first pass"));

        set.admit(candidate(
            "runtime.max_lineage_depth",
            ConfigScope::Global,
            ConfigSource::Env,
            ConfigValue::U64(5),
        ))
        .unwrap();
    }

    #[test]
    fn scope_more_specific_than_target_is_rejected() {
        let mut set = CandidateSet::default();
        // metrics_drain_timeout_ms is Global-target: a stage entry is invalid.
        let err = set
            .admit(candidate(
                "runtime.metrics_drain_timeout_ms",
                ConfigScope::stage("enricher"),
                ConfigSource::File,
                ConfigValue::U64(1000),
            ))
            .unwrap_err();
        assert!(matches!(err, ConfigResolveError::ScopeNotAdmitted { .. }));
        // An edge entry for a Stage-target knob is invalid (§4c).
        let err = set
            .admit(candidate(
                "effects.circuit_breaker.threshold",
                ConfigScope::edge("a", "b"),
                ConfigSource::File,
                ConfigValue::U64(3),
            ))
            .unwrap_err();
        assert!(matches!(err, ConfigResolveError::ScopeNotAdmitted { .. }));
    }

    #[test]
    fn duplicate_admission_is_idempotent_and_conflicts_error() {
        let mut set = CandidateSet::default();
        let dsl = |v: u64| {
            candidate(
                "runtime.backpressure.window",
                ConfigScope::stage("enricher"),
                ConfigSource::Dsl,
                ConfigValue::U64(v),
            )
        };
        set.admit(dsl(200)).unwrap();
        set.admit(dsl(200)).unwrap();
        let err = set.admit(dsl(300)).unwrap_err();
        assert!(matches!(err, ConfigResolveError::Conflict { .. }));
    }

    #[test]
    fn exact_effect_subjects_are_independent_candidate_identities() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let exact = |effect_type: &str, value: f64| ScopedCandidate {
            key_path: key.to_string(),
            address: ConfigAddress::effect("payments", effect_type),
            source: ConfigSource::File,
            value: ConfigValue::F64(value),
        };

        let mut set = CandidateSet::default();
        set.admit(exact("payments.authorize", 5.0)).unwrap();
        set.admit(exact("payments.refund", 7.0)).unwrap();
        set.admit(exact("payments.authorize", 5.0)).unwrap();

        let error = set.admit(exact("payments.authorize", 9.0)).unwrap_err();
        assert!(matches!(
            error,
            ConfigResolveError::Conflict { address, .. }
                if address == ConfigAddress::effect("payments", "payments.authorize")
        ));
        assert_eq!(set.addresses_for(key).count(), 2);
    }

    #[test]
    fn consumption_index_is_an_order_independent_idempotent_set_union() {
        let key = crate::runtime_config::RATE_LIMITER_BURST_CAPACITY_KEY;
        let mut first = DslCandidates::default();
        first.declare_stage_consumption(key, "source");
        first.declare_stage_consumption(key, "source");
        first.declare_effect_consumption(key, "payments", "payments.refund");
        first.declare_effect_consumption(key, "payments", "payments.authorize");

        let mut reordered = DslCandidates::default();
        reordered.declare_effect_consumption(key, "payments", "payments.authorize");
        reordered.declare_effect_consumption(key, "payments", "payments.refund");
        reordered.declare_stage_consumption(key, "source");

        assert_eq!(first.consumption, reordered.consumption);
        assert_eq!(first.stage_consumers()[key].len(), 1);
        assert_eq!(first.effect_consumers()[key].len(), 2);
    }

    #[test]
    fn declaring_a_default_does_not_imply_a_consumption_point() {
        let key = crate::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY;
        let mut dsl = DslCandidates::default();
        dsl.declare_for_effect(key, "payments", "payments.authorize", ConfigValue::F64(5.0));

        assert!(dsl.effect_consumers().get(key).is_none());
        assert_eq!(dsl.entries().len(), 1);
    }

    #[test]
    fn exact_effect_subject_is_rejected_for_non_effect_targets() {
        let mut set = CandidateSet::default();
        let error = set
            .admit(ScopedCandidate {
                key_path: "runtime.max_lineage_depth".to_string(),
                address: ConfigAddress::effect("payments", "payments.authorize"),
                source: ConfigSource::File,
                value: ConfigValue::U64(5),
            })
            .unwrap_err();
        assert!(matches!(error, ConfigResolveError::ScopeNotAdmitted { .. }));
    }

    #[test]
    fn type_validation_runs_at_admission() {
        let mut set = CandidateSet::default();
        let err = set
            .admit(candidate(
                "runtime.cycle_max_iterations",
                ConfigScope::Global,
                ConfigSource::File,
                ConfigValue::U64(70000),
            ))
            .unwrap_err();
        assert!(err.to_string().contains("must be in range 1..=65535"));
    }

    #[test]
    fn unknown_knobs_are_rejected() {
        let mut set = CandidateSet::default();
        let err = set
            .admit(candidate(
                "runtime.no_such_knob",
                ConfigScope::Global,
                ConfigSource::File,
                ConfigValue::U64(1),
            ))
            .unwrap_err();
        assert!(matches!(err, ConfigResolveError::UnknownKnob { .. }));
    }
}
