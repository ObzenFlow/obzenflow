// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline and stage configuration types

use crate::stages::common::handlers::source::traits::{FiniteSourceHandler, InfiniteSourceHandler};
use crate::stages::common::handlers::{SinkHandler, TransformHandler};
use obzenflow_core::{SccId, StageId};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

use super::MaxIterations;

/// Supervisor-level cycle protection configuration (FLOWIP-051l).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleGuardConfig {
    /// Maximum round trips allowed for a single event within this SCC.
    ///
    /// Enforced by the cycle guard at the SCC entry point (FLOWIP-051p).
    pub max_iterations: MaxIterations,

    /// Opaque SCC identifier.
    ///
    /// Deterministic for a given topology, but not stable across topology changes.
    /// Used only for grouping cycle-member stages within a single materialisation.
    pub scc_id: SccId,

    /// Upstream stages outside this stage's SCC.
    ///
    /// EOF/Drain from these upstreams means "no more new data entering the cycle".
    pub external_upstreams: HashSet<StageId>,

    /// Upstream stages inside this stage's SCC.
    pub internal_upstreams: HashSet<StageId>,

    /// Whether this stage is the SCC entry point.
    ///
    /// Only the entry point buffers terminal signals and evaluates SCC quiescence.
    pub is_entry_point: bool,

    /// All (upstream, downstream) edge pairs within this SCC.
    ///
    /// Only populated for entry-point stages.
    pub scc_internal_edges: Vec<(StageId, StageId)>,
}

/// Stage handler type that can be converted to BoxedStageHandle
pub enum StageHandlerType {
    FiniteSource(Box<dyn FiniteSourceHandler>),
    InfiniteSource(Box<dyn InfiniteSourceHandler>),
    Transform(Box<dyn TransformHandler>),
    Sink(Box<dyn SinkHandler>),
    // TODO: FLOWIP-080 will fix Stateful with proper type erasure for associated types
}

/// Stage configuration data - metadata about the stage
pub struct StageConfig {
    pub stage_id: StageId,
    pub name: String,
    pub flow_name: String,
    pub cycle_guard: Option<CycleGuardConfig>,
    /// FLOWIP-010 §7: build-resolved lineage policy used when handlers and
    /// stage-local runtime resources are materialised.
    pub lineage: obzenflow_core::config::LineagePolicy,
    /// Immutable per-flow effective config. Middleware receives only an
    /// exact-point view derived from this carrier at materialisation.
    pub effective_config: Arc<crate::runtime_config::FlowEffectiveConfig>,
}

// TODO: Observers need redesign for FLOWIP-084
// For now, keeping a placeholder struct

/// Observer configuration - for side effects like monitoring
/// NOTE: This needs redesign as part of FLOWIP-084 completion
pub struct ObserverConfig {
    pub name: String,
    // TODO: Replace with appropriate observer handler trait when designed
}

/// Structural middleware configuration for a stage (FLOWIP-059).
///
/// Contains both the ordered list of middleware names and their static configuration
/// snapshots for the topology observability API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareStackConfig {
    /// Ordered list of middleware names in the stack
    pub stack: Vec<String>,
    /// Circuit breaker static config (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<serde_json::Value>,
    /// Rate limiter static config (if present)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiter: Option<serde_json::Value>,
}

impl MiddlewareStackConfig {
    /// Create a new middleware stack config with just names (no detailed config)
    pub fn names_only(stack: Vec<String>) -> Self {
        Self {
            stack,
            circuit_breaker: None,
            rate_limiter: None,
        }
    }
}

/// Strictness mode for source at-least-once contracts.
///
/// This is a minimal, flow-wide toggle for how contract failures on
/// *source* edges influence pipeline behaviour:
/// - `Abort` (default): any failed source contract aborts the pipeline.
/// - `Warn`: failures are logged and surfaced via contract events, but
///   do not cause a pipeline abort. This is intended as a transitional
///   mode until full contract strictness plumbing lands in 090d.
///
/// FLOWIP-010: build-resolved from `contracts.source_contract_strict_mode`
/// and carried on `PipelineContext`; the registry rejects unknown tokens at
/// startup (the old env coercion is gone).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SourceContractStrictMode {
    #[default]
    Abort,
    Warn,
}

impl SourceContractStrictMode {
    /// Parse the registry-validated token (`abort` or `warn`).
    pub(crate) fn from_token(token: &str) -> Self {
        match token {
            "warn" => SourceContractStrictMode::Warn,
            _ => SourceContractStrictMode::Abort,
        }
    }
}
