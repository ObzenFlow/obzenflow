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

use obzenflow_runtime::runtime_config::{ConfigValue, DslCandidates};
use obzenflow_core::config::ConfigScope;
use std::num::NonZeroU64;

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
