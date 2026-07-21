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
use super::flow_view::BackpressureMode;
use obzenflow_core::config::{ConfigAddress, ConfigScope, ConfigSubject};

pub const CIRCUIT_BREAKER_THRESHOLD_KEY: &str = "effects.circuit_breaker.threshold";
pub const RATE_LIMITER_BURST_CAPACITY_KEY: &str = "effects.rate_limiter.burst_capacity";
pub const RATE_LIMITER_EVENTS_PER_SECOND_KEY: &str = "effects.rate_limiter.events_per_second";
pub const RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY: &str =
    "effects.resilience.breaker.consecutive_failures";
pub const RESILIENCE_BREAKER_COUNT_WINDOW_KEY: &str = "effects.resilience.breaker.count_window";
pub const RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY: &str =
    "effects.resilience.breaker.failure_rate_threshold";
pub const RESILIENCE_BREAKER_MINIMUM_CALLS_KEY: &str = "effects.resilience.breaker.minimum_calls";
pub const RESILIENCE_BREAKER_MODE_KEY: &str = "effects.resilience.breaker.mode";
pub const RESILIENCE_BREAKER_OPEN_FOR_MS_KEY: &str = "effects.resilience.breaker.open_for_ms";
pub const RESILIENCE_BREAKER_PROBES_KEY: &str = "effects.resilience.breaker.probes";
pub const RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY: &str =
    "effects.resilience.breaker.rate_limited_counts_as_failure";
pub const RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY: &str =
    "effects.resilience.breaker.slow_call_duration_ms";
pub const RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY: &str =
    "effects.resilience.breaker.slow_call_rate_threshold";
pub const RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY: &str =
    "effects.resilience.rate_limiter.burst_capacity";
pub const RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY: &str =
    "effects.resilience.rate_limiter.cost_per_attempt";
pub const RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY: &str =
    "effects.resilience.rate_limiter.events_per_second";
pub const RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY: &str =
    "effects.resilience.retry.attempt_start_window_ms";
pub const RESILIENCE_RETRY_FIXED_DELAY_MS_KEY: &str = "effects.resilience.retry.fixed_delay_ms";
pub const RESILIENCE_RETRY_KIND_KEY: &str = "effects.resilience.retry.kind";
pub const RESILIENCE_RETRY_MAX_ATTEMPTS_KEY: &str = "effects.resilience.retry.max_attempts";
pub const RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY: &str = "effects.resilience.retry.max_backoff_ms";

/// The most specific scope a knob admits. The stage rung of an edge-target
/// knob binds to one endpoint (§4c; backpressure binds upstream, matching
/// `BackpressurePlan::stage_defaults`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnobTarget {
    Global,
    Flow,
    Stage,
    /// One declared effect. Broadcast candidates may live at global, flow,
    /// or stage scope; exact subjects live only at stage scope.
    Effect,
    /// A multi-surface policy knob: unqualified stage points for source,
    /// ingress, and sink attachments, plus exact effect points.
    StageOrEffect,
    Edge {
        stage_binding: EdgeEndpoint,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeEndpoint {
    Upstream,
    Downstream,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KnobType {
    Bool,
    U64 {
        min: u64,
        max: u64,
    },
    F64 {
        min_exclusive: f64,
    },
    F64Range {
        min_exclusive: f64,
        max_inclusive: f64,
    },
    Text,
    Token {
        allowed: &'static [&'static str],
    },
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
            (
                Self::F64Range {
                    min_exclusive,
                    max_inclusive,
                },
                ConfigValue::F64(v),
            ) => {
                if !v.is_finite() || v <= min_exclusive || v > max_inclusive {
                    Err(format!(
                        "must be finite and in ({min_exclusive}, {max_inclusive}], got {v}"
                    ))
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

    /// Canonical TOML table for one admitted address. Effect layout is an
    /// explicit registry mapping because its parser nests topology scope before
    /// the policy component (`effects.stages.<stage>...rate_limiter`).
    pub fn file_address(&self, address: &ConfigAddress) -> String {
        if let Some(component) = effect_file_component(self.key_path) {
            return match (&address.scope, &address.subject) {
                (ConfigScope::Global, ConfigSubject::Unqualified) => {
                    format!("[effects.{component}]")
                }
                (ConfigScope::Flow, ConfigSubject::Unqualified) => {
                    format!("[effects.flow.{component}]")
                }
                (ConfigScope::Stage { stage }, ConfigSubject::Unqualified) => {
                    format!("[effects.stages.{}.{component}]", stage.as_str())
                }
                (ConfigScope::Stage { stage }, ConfigSubject::Effect { effect_type }) => format!(
                    "[effects.stages.{}.by_type.\"{}\".{component}]",
                    stage.as_str(),
                    effect_type.as_str()
                ),
                _ => unreachable!(
                    "effect knob {} received inadmissible config address {address}",
                    self.key_path
                ),
            };
        }

        let path = self.file_path.unwrap_or(self.key_path);
        let table = path.rsplit_once('.').map_or(path, |(table, _)| table);
        match &address.scope {
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
}

/// Explicit parser layout for the effect knobs. Do not infer this by splitting
/// arbitrary dotted keys: adding a new effect component requires registering
/// its real TOML component here and covering it with a parser test.
fn effect_file_component(key_path: &str) -> Option<&'static str> {
    match key_path {
        CIRCUIT_BREAKER_THRESHOLD_KEY => Some("circuit_breaker"),
        RATE_LIMITER_BURST_CAPACITY_KEY | RATE_LIMITER_EVENTS_PER_SECOND_KEY => {
            Some("rate_limiter")
        }
        RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY
        | RESILIENCE_BREAKER_COUNT_WINDOW_KEY
        | RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY
        | RESILIENCE_BREAKER_MINIMUM_CALLS_KEY
        | RESILIENCE_BREAKER_MODE_KEY
        | RESILIENCE_BREAKER_OPEN_FOR_MS_KEY
        | RESILIENCE_BREAKER_PROBES_KEY
        | RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY
        | RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY
        | RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY => Some("resilience.breaker"),
        RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY
        | RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY
        | RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY => Some("resilience.rate_limiter"),
        RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY
        | RESILIENCE_RETRY_FIXED_DELAY_MS_KEY
        | RESILIENCE_RETRY_KIND_KEY
        | RESILIENCE_RETRY_MAX_ATTEMPTS_KEY
        | RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY => Some("resilience.retry"),
        _ => None,
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
const TOKENS_RESILIENCE_BREAKER_MODE: &[&str] = &["consecutive", "rate_based"];
const TOKENS_RESILIENCE_RETRY_KIND: &[&str] = &["fixed", "exponential"];

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
                key_path: CIRCUIT_BREAKER_THRESHOLD_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::StageOrEffect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RATE_LIMITER_BURST_CAPACITY_KEY,
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::StageOrEffect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::StageOrEffect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u32::MAX as u64,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_COUNT_WINDOW_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u32::MAX as u64,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
                file_path: None,
                value_type: KnobType::F64Range {
                    min_exclusive: 0.0,
                    max_inclusive: 1.0,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_MINIMUM_CALLS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u32::MAX as u64,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_MODE_KEY,
                file_path: None,
                value_type: KnobType::Token {
                    allowed: TOKENS_RESILIENCE_BREAKER_MODE,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_OPEN_FOR_MS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_PROBES_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u32::MAX as u64,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
                file_path: None,
                value_type: KnobType::Bool,
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
                file_path: None,
                value_type: KnobType::F64Range {
                    min_exclusive: 0.0,
                    max_inclusive: 1.0,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY,
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY,
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                file_path: None,
                value_type: KnobType::F64 { min_exclusive: 0.0 },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RETRY_FIXED_DELAY_MS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RETRY_KIND_KEY,
                file_path: None,
                value_type: KnobType::Token {
                    allowed: TOKENS_RESILIENCE_RETRY_KIND,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RETRY_MAX_ATTEMPTS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u32::MAX as u64,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY,
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Effect,
                default: KnobDefault::OptionalAbsent,
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.backpressure.mode",
                file_path: None,
                value_type: KnobType::Token {
                    allowed: BackpressureMode::TOKENS,
                },
                target: KnobTarget::Edge {
                    stage_binding: EdgeEndpoint::Upstream,
                },
                // Enforcement is opt-in: the built-in default is `off`, and
                // window + stall timeout are required where `enforce` resolves
                // (the FLOWIP-115e materialization pass, not a registry
                // Required).
                default: KnobDefault::Value(ConfigValue::Text(
                    BackpressureMode::Off.as_token().to_string(),
                )),
                mutability: Mutability::Restartful,
                redaction: Redaction::Plain,
                env: EnvBinding::Canonical,
            },
            KnobSpec {
                key_path: "runtime.backpressure.stall_timeout_ms",
                file_path: None,
                value_type: KnobType::U64 {
                    min: 1,
                    max: u64::MAX,
                },
                target: KnobTarget::Edge {
                    stage_binding: EdgeEndpoint::Upstream,
                },
                // Required where mode resolves to `enforce`, via the
                // materialization pass.
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
                // Required where mode resolves to `enforce`, via the
                // materialization pass.
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

/// Serializable §10 schema projection (gap 13 string forms): one doc per
/// registered knob, serving the HTTP schema route and the CLI from one
/// projection.
#[derive(Debug, Clone, serde::Serialize)]
pub struct KnobSchemaDoc {
    pub key_path: String,
    pub value_type: String,
    /// The §4c resolution target: the most specific admissible scope.
    pub target: String,
    pub admissible_scopes: Vec<String>,
    pub admissible_subjects: Vec<String>,
    pub canonical_file_address: String,
    pub default: Option<serde_json::Value>,
    pub required: bool,
    pub mutability: String,
    pub redaction: String,
    pub env: Option<String>,
}

pub fn schema_view() -> Vec<KnobSchemaDoc> {
    knob_registry()
        .iter()
        .map(|spec| {
            let (target, admissible_scopes, admissible_subjects, address_suffix) = match spec.target
            {
                KnobTarget::Global => ("global", vec!["global"], vec!["unqualified"], ""),
                KnobTarget::Flow => ("flow", vec!["global", "flow"], vec!["unqualified"], ".flow"),
                KnobTarget::Stage => (
                    "stage",
                    vec!["global", "flow", "stage"],
                    vec!["unqualified"],
                    ".stages.<stage>",
                ),
                KnobTarget::Effect => (
                    "effect",
                    vec!["global", "flow", "stage"],
                    vec!["unqualified", "effect (stage only)"],
                    ".stages.<stage>.by_type.\"<effect-type>\"",
                ),
                KnobTarget::StageOrEffect => (
                    "stage_or_effect",
                    vec!["global", "flow", "stage"],
                    vec!["unqualified", "effect (stage only)"],
                    ".stages.<stage>.by_type.\"<effect-type>\"",
                ),
                KnobTarget::Edge { stage_binding } => (
                    match stage_binding {
                        EdgeEndpoint::Upstream => "edge (binds upstream)",
                        EdgeEndpoint::Downstream => "edge (binds downstream)",
                    },
                    vec!["global", "flow", "stage", "edge"],
                    vec!["unqualified"],
                    ".stages.<upstream>.edges.<downstream>",
                ),
            };
            let canonical_file_address =
                if let Some(component) = effect_file_component(spec.key_path) {
                    format!("[effects.stages.<stage>.by_type.\"<effect-type>\".{component}]")
                } else {
                    let path = spec.file_path.unwrap_or(spec.key_path);
                    let table = path.rsplit_once('.').map_or(path, |(table, _)| table);
                    format!("[{table}{address_suffix}]")
                };
            KnobSchemaDoc {
                key_path: spec.key_path.to_string(),
                value_type: match spec.value_type {
                    KnobType::Bool => "bool".to_string(),
                    KnobType::U64 { min, max } => format!("u64 ({min}..={max})"),
                    KnobType::F64 { min_exclusive } => format!("f64 (> {min_exclusive})"),
                    KnobType::F64Range {
                        min_exclusive,
                        max_inclusive,
                    } => format!("f64 (({min_exclusive}, {max_inclusive}])"),
                    KnobType::Text => "text".to_string(),
                    KnobType::Token { allowed } => format!("token ({})", allowed.join(" | ")),
                    KnobType::Path => "path".to_string(),
                },
                target: target.to_string(),
                admissible_scopes: admissible_scopes.into_iter().map(str::to_string).collect(),
                admissible_subjects: admissible_subjects
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                canonical_file_address,
                default: match &spec.default {
                    KnobDefault::Value(value) => Some(value.to_json()),
                    KnobDefault::OptionalAbsent | KnobDefault::Required => None,
                },
                required: matches!(spec.default, KnobDefault::Required),
                mutability: match spec.mutability {
                    Mutability::Live => "live".to_string(),
                    Mutability::Restartful => "restartful".to_string(),
                    Mutability::Immutable => "immutable".to_string(),
                },
                redaction: match spec.redaction {
                    Redaction::Plain => "plain".to_string(),
                    Redaction::SecretValue => "secret-value".to_string(),
                },
                env: spec.env_name(),
            }
        })
        .collect()
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
    fn effect_file_addresses_match_the_parser_layout() {
        let limiter = knob(RATE_LIMITER_EVENTS_PER_SECOND_KEY).unwrap();
        assert_eq!(
            limiter.file_address(&ConfigAddress::unqualified(ConfigScope::Global)),
            "[effects.rate_limiter]"
        );
        assert_eq!(
            limiter.file_address(&ConfigAddress::unqualified(ConfigScope::Flow)),
            "[effects.flow.rate_limiter]"
        );
        assert_eq!(
            limiter.file_address(&ConfigAddress::unqualified(ConfigScope::stage("payments"))),
            "[effects.stages.payments.rate_limiter]"
        );
        assert_eq!(
            limiter.file_address(&ConfigAddress::effect("payments", "payments.authorize")),
            "[effects.stages.payments.by_type.\"payments.authorize\".rate_limiter]"
        );

        let breaker = knob(CIRCUIT_BREAKER_THRESHOLD_KEY).unwrap();
        assert_eq!(
            breaker.file_address(&ConfigAddress::effect("payments", "payments.authorize")),
            "[effects.stages.payments.by_type.\"payments.authorize\".circuit_breaker]"
        );
    }

    #[test]
    fn effect_schema_projects_the_canonical_exact_address() {
        let limiter = schema_view()
            .into_iter()
            .find(|doc| doc.key_path == RATE_LIMITER_EVENTS_PER_SECOND_KEY)
            .unwrap();
        assert_eq!(limiter.target, "stage_or_effect");
        assert_eq!(
            limiter.canonical_file_address,
            "[effects.stages.<stage>.by_type.\"<effect-type>\".rate_limiter]"
        );
        assert_eq!(
            limiter.admissible_subjects,
            vec!["unqualified", "effect (stage only)"]
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

        let rate = KnobType::F64Range {
            min_exclusive: 0.0,
            max_inclusive: 1.0,
        };
        assert!(rate.validate(&ConfigValue::F64(1.0)).is_ok());
        assert!(rate.validate(&ConfigValue::F64(0.0)).is_err());
        assert!(rate.validate(&ConfigValue::F64(1.01)).is_err());
    }
}
