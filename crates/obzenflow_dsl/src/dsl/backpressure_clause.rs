// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The `backpressure:` stage clause (FLOWIP-115e).
//!
//! Pure sugar for DSL-tier config candidates: a clause declares
//! `runtime.backpressure.mode` (and optionally the window and stall timeout)
//! at its declaration site's scope, and the FLOWIP-010 ladder resolves them
//! with file entries outranking these code-declared defaults. No attachment
//! semantics: enforcement is the resolved mode, never the clause's presence.

use obzenflow_core::config::{ConfigScope, ConfigSource};
use obzenflow_runtime::runtime_config::{BackpressureMode, ConfigValue, DslCandidates};
use std::num::NonZeroU64;

/// What a resolved edge contributes to the `BackpressurePlan` (FLOWIP-115e).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeBackpressure {
    /// Enforce with the resolved window and stall timeout.
    Enforce,
    /// Track in-flight accounting without blocking.
    Track,
    /// No backpressure state for this edge.
    Disabled,
}

/// The per-edge plan-feeding decision, provenance-aware for the SCC
/// auto-enable: a cycle-internal edge whose `off` is only the built-in
/// default upgrades to `track`, but an explicit `off` from any tier (file,
/// env, or a DSL `off()` clause) is respected.
pub fn resolve_edge_backpressure(
    mode: BackpressureMode,
    mode_source: ConfigSource,
    cycle_internal: bool,
) -> EdgeBackpressure {
    match mode {
        BackpressureMode::Enforce => EdgeBackpressure::Enforce,
        BackpressureMode::Track => EdgeBackpressure::Track,
        BackpressureMode::Off => {
            if cycle_internal && matches!(mode_source, ConfigSource::Default) {
                EdgeBackpressure::Track
            } else {
                EdgeBackpressure::Disabled
            }
        }
    }
}

/// One stage's (or the flow's) backpressure declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackpressureClause {
    mode: &'static str,
    window: Option<NonZeroU64>,
    stall_timeout_ms: Option<u64>,
}

/// Enforce with a DSL-tier window default; config may override per scope.
pub fn enforced(window: u64) -> BackpressureClause {
    BackpressureClause {
        mode: "enforce",
        window: Some(NonZeroU64::new(window).expect("window must be > 0")),
        stall_timeout_ms: None,
    }
}

/// Enforce with no DSL-tier values: the build fails unless config supplies
/// the window and stall timeout for the declaring scope's edges.
pub fn enforced_from_config() -> BackpressureClause {
    BackpressureClause {
        mode: "enforce",
        window: None,
        stall_timeout_ms: None,
    }
}

/// Track in-flight accounting without ever blocking (window-free).
pub fn track_only() -> BackpressureClause {
    BackpressureClause {
        mode: "track",
        window: None,
        stall_timeout_ms: None,
    }
}

/// Explicit off; pins a cycle-internal edge against the SCC auto-enable.
pub fn off() -> BackpressureClause {
    BackpressureClause {
        mode: "off",
        window: None,
        stall_timeout_ms: None,
    }
}

impl BackpressureClause {
    /// DSL-tier stall-timeout default for the enforced forms.
    pub fn stall_timeout_ms(mut self, ms: u64) -> Self {
        assert!(ms > 0, "stall timeout must be > 0 ms");
        self.stall_timeout_ms = Some(ms);
        self
    }

    /// Declare this clause's candidates at `scope` (the declaration site).
    pub fn declare(&self, candidates: &mut DslCandidates, scope: ConfigScope) {
        candidates.declare(
            "runtime.backpressure.mode",
            scope.clone(),
            ConfigValue::Text(self.mode.to_string()),
        );
        if let Some(window) = self.window {
            candidates.declare(
                "runtime.backpressure.window",
                scope.clone(),
                ConfigValue::U64(window.get()),
            );
        }
        if let Some(ms) = self.stall_timeout_ms {
            candidates.declare(
                "runtime.backpressure.stall_timeout_ms",
                scope,
                ConfigValue::U64(ms),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // C6: provenance-aware SCC auto-enable.
    #[test]
    fn default_off_cycle_internal_edge_upgrades_to_track() {
        assert_eq!(
            resolve_edge_backpressure(BackpressureMode::Off, ConfigSource::Default, true),
            EdgeBackpressure::Track
        );
    }

    #[test]
    fn explicit_off_survives_auto_enable_on_a_cycle_edge() {
        // A DSL `off()` clause, a file entry, or an env override all resolve
        // through a non-Default source and must be respected.
        for source in [ConfigSource::Dsl, ConfigSource::File, ConfigSource::Env] {
            assert_eq!(
                resolve_edge_backpressure(BackpressureMode::Off, source.clone(), true),
                EdgeBackpressure::Disabled,
                "explicit off from {source:?} must survive the auto-enable"
            );
        }
    }

    #[test]
    fn default_off_acyclic_edge_stays_disabled() {
        assert_eq!(
            resolve_edge_backpressure(BackpressureMode::Off, ConfigSource::Default, false),
            EdgeBackpressure::Disabled
        );
    }

    #[test]
    fn enforce_and_track_pass_through_regardless_of_cycle_or_source() {
        for cycle_internal in [true, false] {
            assert_eq!(
                resolve_edge_backpressure(
                    BackpressureMode::Enforce,
                    ConfigSource::Default,
                    cycle_internal
                ),
                EdgeBackpressure::Enforce
            );
            assert_eq!(
                resolve_edge_backpressure(
                    BackpressureMode::Track,
                    ConfigSource::File,
                    cycle_internal
                ),
                EdgeBackpressure::Track
            );
        }
    }
}
