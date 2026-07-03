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
use obzenflow_core::config::{ConfigScope, ConfigSource};
use std::collections::BTreeMap;

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

/// One candidate at one scope from one source.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedCandidate {
    pub key_path: String,
    pub scope: ConfigScope,
    pub source: ConfigSource,
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

/// The scoped candidate table: knob key path -> scope -> source slots.
/// `BTreeMap` throughout for deterministic iteration and evidence ordering.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CandidateSet {
    by_knob: BTreeMap<String, BTreeMap<ConfigScope, SourceSlots>>,
}

impl CandidateSet {
    /// Admit one candidate, enforcing the knob's scope matrix, the
    /// first-pass source matrix, type validation, and the one-candidate-
    /// per-(knob, scope, source) invariant (identical re-admission is a
    /// no-op; a differing value is a conflict).
    pub fn admit(&mut self, candidate: ScopedCandidate) -> Result<(), ConfigResolveError> {
        let ScopedCandidate {
            key_path,
            scope,
            source,
            value,
        } = candidate;

        let spec = knob(&key_path).ok_or_else(|| ConfigResolveError::UnknownKnob {
            key_path: key_path.clone(),
        })?;

        if !scope_admitted(spec.target, &scope) {
            return Err(ConfigResolveError::ScopeNotAdmitted {
                key_path,
                scope,
                reason: format!(
                    "the knob's target is {:?}; entries more specific than the target are invalid",
                    spec.target
                ),
            });
        }

        // §2 first-pass lock: scoped entries ride file and DSL only.
        if matches!(source, ConfigSource::Cli | ConfigSource::Env) && scope != ConfigScope::Global {
            return Err(ConfigResolveError::ScopeNotAdmitted {
                key_path,
                scope,
                reason: format!(
                    "{source} overrides are global-only in the first pass; use a file entry at this scope"
                ),
            });
        }

        if let Err(message) = spec.value_type.validate(&value) {
            return Err(ConfigResolveError::InvalidValue {
                key_path,
                scope,
                message,
            });
        }

        let slots = self
            .by_knob
            .entry(key_path.clone())
            .or_default()
            .entry(scope.clone())
            .or_default();
        let Some(slot) = slots.slot_mut(&source) else {
            return Err(ConfigResolveError::ScopeNotAdmitted {
                key_path,
                scope,
                reason: format!("{source} is not an admissible candidate source"),
            });
        };
        match slot {
            Some(existing) if *existing != value => Err(ConfigResolveError::Conflict {
                key_path,
                scope,
                source,
            }),
            _ => {
                *slot = Some(value);
                Ok(())
            }
        }
    }

    pub fn get(&self, key_path: &str, scope: &ConfigScope) -> Option<&SourceSlots> {
        self.by_knob
            .get(key_path)
            .and_then(|scopes| scopes.get(scope))
    }

    /// All scopes carrying candidates for one knob (validation walks this).
    pub fn scopes_for(&self, key_path: &str) -> impl Iterator<Item = &ConfigScope> {
        self.by_knob
            .get(key_path)
            .into_iter()
            .flat_map(|scopes| scopes.keys())
    }

    /// Knobs that carry at least one candidate.
    pub fn knob_paths(&self) -> impl Iterator<Item = &str> {
        self.by_knob.keys().map(String::as_str)
    }
}

/// Whether `scope` is admissible for a knob with `target` (§4c: the target
/// is the most specific admissible scope; coarser scopes always admit).
pub fn scope_admitted(target: KnobTarget, scope: &ConfigScope) -> bool {
    let specificity = match scope {
        ConfigScope::Global => 0,
        ConfigScope::Flow => 1,
        ConfigScope::Stage { .. } => 2,
        ConfigScope::Edge { .. } => 3,
    };
    let max = match target {
        KnobTarget::Global => 0,
        KnobTarget::Flow => 1,
        KnobTarget::Stage => 2,
        KnobTarget::Edge { .. } => 3,
    };
    specificity <= max
}

/// DSL-declared candidates collected during flow build (§4a: declaration-
/// site scope, one candidate per knob and site; source is always `Dsl`).
#[derive(Debug, Clone, Default)]
pub struct DslCandidates {
    entries: Vec<ScopedCandidate>,
}

impl DslCandidates {
    pub fn declare(&mut self, key_path: impl Into<String>, scope: ConfigScope, value: ConfigValue) {
        self.entries.push(ScopedCandidate {
            key_path: key_path.into(),
            scope,
            source: ConfigSource::Dsl,
            value,
        });
    }

    pub fn entries(&self) -> &[ScopedCandidate] {
        &self.entries
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
            scope,
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
