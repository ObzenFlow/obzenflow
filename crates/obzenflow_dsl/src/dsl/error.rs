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

    #[error("Runtime resource preflight failed: {0}")]
    ResourcePreflightFailed(String),

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
        "Stage '{stage_name}' declares policy middleware '{middleware}' on a pure sync surface. \
         Policy middleware attaches to live I/O units only: sources, the effect boundary of an \
         effectful stage, or sink delivery (FLOWIP-120c H1). A deterministic handler shell has \
         no unreliable call to protect; move the policy to the stage that performs the I/O."
    )]
    PolicyMiddlewareOnPureStage {
        stage_name: String,
        middleware: String,
    },

    #[error(
        "Flow-level policy middleware '{middleware}' is not allowed. Policy middleware attaches \
         to live I/O units only: sources, the effect boundary of an effectful stage, or sink \
         delivery (FLOWIP-120c H1). A flow-level policy would be broadcast onto stages that may \
         have no protected dependency; attach it to the specific live I/O unit instead."
    )]
    PolicyMiddlewareOnFlowScope { middleware: String },

    #[error(
        "Stage '{stage_name}' declares policy middleware '{middleware}' on an effectful stateful \
         stage before FLOWIP-120l installs the stateful effect boundary. Accepting this would \
         advertise protection while stateful dispatch still has effect_boundary: None."
    )]
    PolicyMiddlewareOnPendingEffectfulStateful {
        stage_name: String,
        middleware: String,
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
        "Effectful stage '{stage_name}' carries middleware '{middleware_label}' whose '{policy}' \
         policy silently drops events at the effect boundary. The handler awaits a value from \
         `fx.perform`, so transport truncation has no coherent boundary behaviour; use \
         `OpenPolicy::EmitFallback` with a typed fallback or `OpenPolicy::FailFast` instead \
         (FLOWIP-120h)."
    )]
    MiddlewarePolicyIncompatibleWithEffectfulStage {
        stage_name: String,
        middleware_label: String,
        policy: String,
    },

    #[error(
        "Stage '{stage_name}' has a type-shaping middleware configuration error: {message} \
         (FLOWIP-120h)."
    )]
    TypeShapingConfiguration { stage_name: String, message: String },

    #[error(
        "Stage '{stage_name}': middleware '{middleware_label}' may author branch fact '{fact}', \
         which is not a member of the stage output contract {contract}. Add it to the arrow \
         (FLOWIP-120h corrected Option A: membership is enforced, never inferred)."
    )]
    MiddlewareContributedFactNotInContract {
        stage_name: String,
        middleware_label: String,
        fact: String,
        contract: String,
    },

    #[error(
        "Stage '{stage_name}': effect '{effect_type}' may produce fact '{fact}', which is not a \
         member of the stage output contract {contract}. Add it to the arrow (FLOWIP-120m \
         unconditional producer-side containment check)."
    )]
    EffectFactNotInContract {
        stage_name: String,
        effect_type: String,
        fact: String,
        contract: String,
    },

    #[error(
        "Stage '{stage_name}': effect '{effect_type}' declares two outcome members with the same \
         event type `{event_type}` ('{first_member}' and '{second_member}'). Dispatch is by \
         event type, so carrier members must have distinct event types (FLOWIP-120m)."
    )]
    DuplicateEffectOutcomeFactEventType {
        stage_name: String,
        effect_type: String,
        event_type: String,
        first_member: String,
        second_member: String,
    },

    #[error(
        "Stage '{stage_name}' declares both branch-shaped and outcome-shaped fallback \
         registrations. A stage uses one fallback shape: the Guarded carrier with branch \
         facts (build_typed) or the plain perform with an outcome-shaped fallback \
         (build_outcome) (FLOWIP-120m)."
    )]
    MixedFallbackShapesOnStage { stage_name: String },

    #[error(
        "Stage '{stage_name}': outcome-shaped fallback '{middleware_label}' may synthesize \
         fact '{fact}', which is not in effect '{effect_type}'s declared outcome fact set. \
         An outcome-shaped fallback produces the protected effect's own facts (FLOWIP-120m)."
    )]
    OutcomeFallbackFactNotInEffectFactSet {
        stage_name: String,
        middleware_label: String,
        fact: String,
        effect_type: String,
    },

    #[error(
        "Effectful stage '{stage_name}' is downstream of nondeterministic fan-in. \
         FLOWIP-095d auto-enables the canonical deterministic merge on fan-ins above \
         effectful stages, so this rejection means the order cannot be made stable: a \
         fan-in on the path is part of a cycle, a cycle feeds an ordered fan-in from \
         above, or the flow was built without the flow! enablement walk. \
         Move the effect to a single-input deterministic path or out of the cycle."
    )]
    EffectfulFanInRequiresDeterministicOrder { stage_name: String },

    #[error(
        "Order-observing stage '{stage_name}' is downstream of nondeterministic fan-in. \
         FLOWIP-095m auto-enables the canonical deterministic merge on fan-ins above \
         stateful stages and live joins, so this rejection means the order cannot be \
         made stable. The usual cause is a cycle on the path, because a stateful or \
         live-join stage below a cycle-fed fan-in cannot reconstruct a stable input \
         order in v1. Move the stage off the cycle's output, or make the upstream \
         path acyclic."
    )]
    OrderObserverFanInRequiresDeterministicOrder { stage_name: String },

    #[error(
        "Sink '{stage}' declares a non-idempotent external delivery path; resume re-delivers \
         the recorded prefix during catch-up, which would duplicate those writes \
         (FLOWIP-120n F16). Pass `allow_duplicate_sink_delivery` to accept duplication."
    )]
    ResumeRefusedNonIdempotentSink { stage: String },

    #[error(
        "Sink '{stage}' has no declared delivery safety; resume re-delivers the recorded \
         prefix during catch-up and fails closed on undeclared sinks (FLOWIP-120n F16). \
         Declare it where the handler lives: `delivery: idempotent` on a sink! closure, \
         `.idempotent()` / `.non_idempotent()` on a typed sink handler expression, \
         `SinkHandler::delivery_safety()` on a custom handler type, or `SAFETY` on a typed \
         `Delivery`. Or pass `allow_duplicate_sink_delivery`."
    )]
    ResumeRefusedUndeclaredSink { stage: String },

    #[error(
        "Resume requires a durable current run (FLOWIP-120u F13). The live continuation \
         records new effect outcomes, and an ephemeral run has no location: the continuation \
         could never be resumed, and its effects would re-execute on the next resume of the \
         original archive. Switch `journals:` to `disk_journals(...)`, or use `--replay-from` \
         for an effect-free bounded rehearsal."
    )]
    ResumeRefusedEphemeralRun,
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
