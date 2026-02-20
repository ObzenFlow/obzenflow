// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline and stage configuration types

use crate::stages::common::handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::{SccId, StageId};
use std::collections::HashSet;

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
}

// TODO: Observers need redesign for FLOWIP-084
// For now, keeping a placeholder struct

/// Observer configuration - for side effects like monitoring
/// NOTE: This needs redesign as part of FLOWIP-084 completion
pub struct ObserverConfig {
    pub name: String,
    // TODO: Replace with appropriate observer handler trait when designed
}
