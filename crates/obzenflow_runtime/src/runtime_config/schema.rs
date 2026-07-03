// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The knob registry: one `KnobSpec` per configuration key (FLOWIP-010 §10).
//!
//! A knob's target is the most specific scope it admits (§4c); entries at a
//! more specific scope are validation errors. `Required` ships as machinery
//! with no first-pass registry consumer; FLOWIP-115e flips the backpressure
//! window downstream.

use super::candidates::ConfigValue;

/// The most specific scope a knob admits. The stage rung of an edge-target
/// knob binds to one endpoint (§4c; backpressure binds upstream, matching
/// `BackpressurePlan::stage_defaults`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnobTarget {
    Global,
    Flow,
    Stage,
    Edge { stage_binding: EdgeEndpoint },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeEndpoint {
    Upstream,
    Downstream,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KnobType {
    Bool,
    U64 { min: u64, max: u64 },
    F64 { min_exclusive: f64 },
    Text,
    Token { allowed: &'static [&'static str] },
    Path,
}

impl KnobType {
    /// Range/token admission; type mismatches are also rejected here.
    pub fn validate(&self, value: &ConfigValue) -> Result<(), String> {
        match (self, value) {
            (Self::Bool, ConfigValue::Bool(_)) => Ok(()),
            (Self::U64 { min, max }, ConfigValue::U64(v)) => {
                if v < min || v > max {
                    Err(format!("must be in range {min}..={max}, got {v}"))
                } else {
                    Ok(())
                }
            }
            (Self::F64 { min_exclusive }, ConfigValue::F64(v)) => {
                if !v.is_finite() || v <= min_exclusive {
                    Err(format!("must be finite and > {min_exclusive}, got {v}"))
                } else {
                    Ok(())
                }
            }
            (Self::Text, ConfigValue::Text(_)) => Ok(()),
            (Self::Path, ConfigValue::Text(_)) => Ok(()),
            (Self::Token { allowed }, ConfigValue::Text(v)) => {
                if allowed.contains(&v.as_str()) {
                    Ok(())
                } else {
                    Err(format!(
                        "unknown value {v:?}; expected one of {}",
                        allowed.join(", ")
                    ))
                }
            }
            (expected, got) => Err(format!("expected {expected:?}, got {}", got.type_label())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum KnobDefault {
    /// Built-in default; the post-ladder fallback, reported (default, global).
    Value(ConfigValue),
    /// Absence is a legal resolution (`Ok(None)`).
    OptionalAbsent,
    /// No default: ladder exhaustion fails the build naming the knob and
    /// the scopes that may supply it (§2).
    Required,
}

/// §10 mutability classes. Nothing is `Live` in the first pass: live-class
/// knobs must be served through the owned snapshot handle (§7 linkage), and
/// no such serving path exists until FLOWIP-010b.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutability {
    Live,
    Restartful,
    Immutable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Redaction {
    Plain,
    /// The value is resolved secret material (§13 definition); docs carry
    /// `***redacted***`. Reference NAMES are `Plain`.
    SecretValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvBinding {
    /// `OBZENFLOW_<KEY_PATH>` uppercased (§2 environment-tier lock).
    Canonical,
    /// A fixed spelling (the `runtime.resume.*` view keeps the 010h names).
    Named(&'static str),
    /// No env binding.
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnobSpec {
    /// Canonical dotted key, e.g. `runtime.max_lineage_depth`.
    pub key_path: &'static str,
    /// File table path when it differs from the key path (the
    /// `runtime.resume.*` view over the 010h `[replay]` table).
    pub file_path: Option<&'static str>,
    pub value_type: KnobType,
    pub target: KnobTarget,
    pub default: KnobDefault,
    pub mutability: Mutability,
    pub redaction: Redaction,
    pub env: EnvBinding,
}

impl KnobSpec {
    pub fn env_name(&self) -> Option<String> {
        match self.env {
            EnvBinding::Canonical => Some(canonical_env_name(self.key_path)),
            EnvBinding::Named(name) => Some(name.to_string()),
            EnvBinding::None => None,
        }
    }
}

/// `runtime.max_lineage_depth` -> `OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH`.
pub fn canonical_env_name(key_path: &str) -> String {
    format!(
        "OBZENFLOW_{}",
        key_path.replace('.', "_").to_ascii_uppercase()
    )
}

const TOKENS_STRICT_MODE: &[&str] = &["abort", "warn"];
const TOKENS_AI_PROVIDER: &[&str] = &["ollama", "openai", "openai_compatible"];

/// The first-pass registry: consumers-only namespaces (FLOWIP-010
/// implementation shape). Sorted by key path; a unit test enforces it.
pub fn knob_registry() -> &'static [KnobSpec] {
    static REGISTRY: std::sync::OnceLock<Vec<KnobSpec>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| {
        vec![
            KnobSpec {
                key_path: "ai.models.api_key_env",
                file_path: None,
                value_type: KnobType::Text,
                target: KnobTarget::Global,
                default: KnobDefault::Value(ConfigValue::Text("OPENAI_API_KEY".to_string())),
                mutability: Mutability::Restartful,
                // The reference NAME is the non-secret shape (§13).
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "ai.models.base_url",
                file_path: None,
                value_type: KnobType::Text,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "ai.models.model",
                file_path: None,
                value_type: KnobType::Text,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "ai.models.provider",
                file_path: None,
                value_type: KnobType::Token {
                    allowed: TOKENS_AI_PROVIDER,
                },
                target: KnobTarget::Global,
                default: KnobDefault::Value(ConfigValue::Text("ollama".to_string())),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "contracts.source_contract_strict_mode",
                file_path: None,
                value_type: KnobType::Token {
                    allowed: TOKENS_STRICT_MODE,
                },
                target: KnobTarget::Global,
                default: KnobDefault::Value(ConfigValue::Text("abort".to_string())),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "effects.circuit_breaker.threshold",
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Stage,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "effects.rate_limiter.burst_capacity",
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::Stage,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "effects.rate_limiter.events_per_second",
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::Stage,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.backpressure.window",
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Edge {
                    stage_binding: EdgeEndpoint::Upstream,
                },
                // OptionalAbsent preserves today's opt-in behaviour; flips
                // to Required in FLOWIP-115e.
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.cycle_max_iterations",
                file_path: None,
                value_type: KnobType::U64 { min: 1, max: 65535 },
                target: KnobTarget::Flow,
                default: KnobDefault::Value(ConfigValue::U64(30)),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.heartbeat_interval",
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Stage,
                default: KnobDefault::Value(ConfigValue::U64(1000)),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.max_lineage_depth",
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Stage,
                default: KnobDefault::Value(ConfigValue::U64(
                    obzenflow_core::config::DEFAULT_MAX_LINEAGE_DEPTH as u64,
                )),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.metrics_drain_timeout_ms",
                file_path: None,
                value_type: KnobType::U64 {
                    min: 0,
                    max: u64::MAX,
                },
                target: KnobTarget::Global,
                default: KnobDefault::Value(ConfigValue::U64(5000)),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            // The runtime.resume.* view over the 010h [replay] parse
            // (absorption without moving the parse; Immutable: run identity).
            KnobSpec {
                key_path: "runtime.resume.allow_duplicate_sink_delivery",
                file_path: Some("replay.allow_duplicate_sink_delivery"),
                value_type: KnobType::Bool,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Immutable,
                redaction: Redaction::Plain,
                env: EnvBinding::Named("OBZENFLOW_ALLOW_DUPLICATE_SINK_DELIVERY"),
            },
            KnobSpec {
                key_path: "runtime.resume.allow_incomplete_archive",
                file_path: Some("replay.allow_incomplete_archive"),
                value_type: KnobType::Bool,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Immutable,
                redaction: Redaction::Plain,
                env: EnvBinding::Named("OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE"),
            },
            KnobSpec {
                key_path: "runtime.resume.replay_from",
                file_path: Some("replay.from"),
                value_type: KnobType::Path,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Immutable,
                redaction: Redaction::Plain,
                env: EnvBinding::Named("OBZENFLOW_REPLAY_FROM"),
            },
            KnobSpec {
                key_path: "runtime.resume.resume_from",
                file_path: Some("replay.resume_from"),
                value_type: KnobType::Path,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Immutable,
                redaction: Redaction::Plain,
                env: EnvBinding::Named("OBZENFLOW_RESUME_FROM"),
            },
            KnobSpec {
                key_path: "runtime.resume.verify",
                file_path: Some("replay.verify"),
                value_type: KnobType::Bool,
                target: KnobTarget::Global,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Immutable,
                redaction: Redaction::Plain,
                env: EnvBinding::Named("OBZENFLOW_REPLAY_VERIFY"),
            },
        ]
    })
}

pub fn knob(key_path: &str) -> Option<&'static KnobSpec> {
    knob_registry()
        .iter()
        .find(|spec| spec.key_path == key_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_sorted_unique_and_coherent() {
        let registry = knob_registry();
        for pair in registry.windows(2) {
            assert!(
                pair[0].key_path < pair[1].key_path,
                "registry must stay sorted and unique: {} vs {}",
                pair[0].key_path,
                pair[1].key_path
            );
        }
        for spec in registry {
            // Required implies no default value by construction of the enum;
            // Edge targets carry a binding by construction. The checkable
            // invariants: no Live entries in the first pass (§7 linkage),
            // and aliases only on the runtime.resume view.
            assert_ne!(
                spec.mutability,
                Mutability::Live,
                "{}: nothing is Live before 010b",
                spec.key_path
            );
            if spec.file_path.is_some() || matches!(spec.env, EnvBinding::Named(_)) {
                assert!(
                    spec.key_path.starts_with("runtime.resume."),
                    "{}: aliases are reserved for the runtime.resume view",
                    spec.key_path
                );
            }
        }
    }

    #[test]
    fn canonical_env_names_match_documented_spellings() {
        assert_eq!(
            canonical_env_name("runtime.max_lineage_depth"),
            "OBZENFLOW_RUNTIME_MAX_LINEAGE_DEPTH"
        );
        assert_eq!(
            canonical_env_name("contracts.source_contract_strict_mode"),
            "OBZENFLOW_CONTRACTS_SOURCE_CONTRACT_STRICT_MODE"
        );
        assert_eq!(
            canonical_env_name("ai.models.provider"),
            "OBZENFLOW_AI_MODELS_PROVIDER"
        );
        assert_eq!(
            canonical_env_name("runtime.backpressure.window"),
            "OBZENFLOW_RUNTIME_BACKPRESSURE_WINDOW"
        );
    }

    #[test]
    fn knob_type_validation_reports_ranges_and_tokens() {
        let u = KnobType::U64 { min: 1, max: 65535 };
        assert!(u.validate(&ConfigValue::U64(1)).is_ok());
        assert!(u.validate(&ConfigValue::U64(0)).is_err());
        assert!(u.validate(&ConfigValue::U64(70000)).is_err());
        assert!(u.validate(&ConfigValue::Bool(true)).is_err());

        let t = KnobType::Token {
            allowed: TOKENS_STRICT_MODE,
        };
        assert!(t.validate(&ConfigValue::Text("warn".to_string())).is_ok());
        let err = t
            .validate(&ConfigValue::Text("statsd".to_string()))
            .unwrap_err();
        assert!(err.contains("expected one of abort, warn"));

        let f = KnobType::F64 { min_exclusive: 0.0 };
        assert!(f.validate(&ConfigValue::F64(0.1)).is_ok());
        assert!(f.validate(&ConfigValue::F64(0.0)).is_err());
        assert!(f.validate(&ConfigValue::F64(f64::NAN)).is_err());
    }
}
