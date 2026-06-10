// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use thiserror::Error;

use obzenflow_adapters::middleware::MiddlewareFactoryError;
use obzenflow_topology::TopologyError;

use crate::dsl::typing::EdgeInputRole;
use crate::middleware_resolution::MiddlewareResolutionError;

#[derive(Debug, Error)]
pub enum StageCreationError {
    #[error(transparent)]
    MiddlewareFactory(#[from] MiddlewareFactoryError),

    #[error(transparent)]
    MiddlewareResolution(#[from] MiddlewareResolutionError),

    #[error("{0}")]
    Message(String),
}

pub type StageCreationResult<T> = Result<T, StageCreationError>;

impl From<String> for StageCreationError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

impl From<&str> for StageCreationError {
    fn from(message: &str) -> Self {
        Self::Message(message.to_string())
    }
}

/// Discriminator for the shape of an edge typing mismatch (FLOWIP-114c).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeTypingMismatchKind {
    /// One upstream emits a different `Exact` type than the downstream declares.
    SingleEdge,
    /// Two or more upstreams emit differing `Exact` types into the same
    /// non-join downstream slot, or the same join leg.
    HeterogeneousFanIn {
        other_upstream_stages: Vec<String>,
        other_actual_types: Vec<String>,
    },
}

/// Structured error type for failures during flow construction
#[derive(Debug, Error)]
pub enum FlowBuildError {
    #[error("Topology validation failed: {0}")]
    TopologyValidationFailed(#[source] TopologyError),

    #[error("Unsupported cycle topology: {0}")]
    UnsupportedCycleTopology(String),

    #[error("Journal factory failed: {0}")]
    JournalFactoryFailed(String),

    #[error("Stage resources build failed: {0}")]
    StageResourcesFailed(String),

    #[error("Failed to create stage '{stage_name}': {source}")]
    StageCreationFailed {
        stage_name: String,
        #[source]
        source: StageCreationError,
    },

    #[error("Pipeline build failed: {0}")]
    PipelineBuildFailed(String),

    #[error("Duplicate stage descriptor name '{name}' (used by '{first_var}' and '{second_var}')")]
    DuplicateStageName {
        name: String,
        first_var: String,
        second_var: String,
    },

    #[error(
        "{}",
        FlowBuildError::fmt_edge_typing_mismatch(
            upstream_stage,
            downstream_stage,
            *role,
            actual_type,
            expected_type,
            kind,
            suggested_fix,
        )
    )]
    EdgeTypingMismatch {
        upstream_stage: String,
        downstream_stage: String,
        role: EdgeInputRole,
        expected_type: String,
        actual_type: String,
        kind: EdgeTypingMismatchKind,
        suggested_fix: String,
    },

    #[error(
        "Stage '{stage_name}' carries no typing metadata. After FLOWIP-114c, every \
         DSL-authored stage must declare its types via the typed macro form (e.g. \
         `transform!(In -> Out => handler)`)."
    )]
    StageMissingTypingMetadata { stage_name: String },

    #[error(
        "Stage '{stage_name}' has Unspecified typing on the {slot} slot, which is applicable \
         for this stage role. Declare the type via the typed macro form."
    )]
    UnspecifiedTypingOnApplicableSlot { stage_name: String, slot: String },

    #[error(
        "Effectful stage '{stage_name}' is downstream of nondeterministic fan-in. \
         FLOWIP-095d auto-enables the canonical deterministic merge on fan-ins above \
         effectful stages, so this rejection means the order cannot be made stable: a \
         fan-in on the path is part of a cycle, a cycle feeds an ordered fan-in from \
         above, or the flow was built without the flow! enablement walk. \
         Move the effect to a single-input deterministic path or out of the cycle."
    )]
    EffectfulFanInRequiresDeterministicOrder { stage_name: String },
}

impl FlowBuildError {
    /// Render an `EdgeTypingMismatch` body. Branches on `kind` so the
    /// `HeterogeneousFanIn` form lists every offending upstream and its actual
    /// type, instead of the misleading "X emits T, expected T" shape that
    /// reusing the SingleEdge template produced for the focal upstream in
    /// earlier revisions.
    pub fn fmt_edge_typing_mismatch(
        upstream_stage: &str,
        downstream_stage: &str,
        role: EdgeInputRole,
        actual_type: &str,
        expected_type: &str,
        kind: &EdgeTypingMismatchKind,
        suggested_fix: &str,
    ) -> String {
        match kind {
            EdgeTypingMismatchKind::SingleEdge => format!(
                "Edge typing mismatch on {role} into '{downstream_stage}': '{upstream_stage}' \
                 emits '{actual_type}', expected '{expected_type}'. {suggested_fix}"
            ),
            EdgeTypingMismatchKind::HeterogeneousFanIn {
                other_upstream_stages,
                other_actual_types,
            } => {
                let mut msg = format!(
                    "Heterogeneous fan-in on {role} into '{downstream_stage}': \
                     '{upstream_stage}' emits '{actual_type}'"
                );
                for (stage, ty) in other_upstream_stages.iter().zip(other_actual_types.iter()) {
                    msg.push_str(&format!(", '{stage}' emits '{ty}'"));
                }
                msg.push_str(&format!(". {suggested_fix}"));
                msg
            }
        }
    }
}

impl From<FlowBuildError> for String {
    fn from(err: FlowBuildError) -> Self {
        err.to_string()
    }
}
