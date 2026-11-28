// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use thiserror::Error;

use obzenflow_adapters::middleware::MiddlewareFactoryError;
use obzenflow_runtime::runtime_config::{
    ApplicationSettingsError, ConfigResolveError, JournalSync,
};
use obzenflow_topology::TopologyError;

use crate::dsl::typing::EdgeInputRole;
use std::path::{Path, PathBuf};

fn journal_root_unused(root: &Path, run_path: Option<&Path>) -> String {
    match run_path {
        None => format!(
            "journal.root is {}, but this flow records to memory journals, which keep no run \
             location; unset journal.root or record the flow with disk_journals",
            root.display()
        ),
        Some(run_path) => format!(
            "journal.root is {}, but the flow's journal provider placed this run at {}",
            root.display(),
            run_path.display()
        ),
    }
}

/// A failed frozen middleware plan, preserving its authored context and cause.
#[derive(Debug, Error)]
pub enum MiddlewarePlanError {
    #[error("stage '{stage}'{effect}: {message}")]
    InvalidAttachment {
        stage: String,
        effect: String,
        message: String,
    },
    #[error("stage '{stage}'{effect}: {source}")]
    InvalidConfiguration {
        stage: String,
        effect: String,
        #[source]
        source: MiddlewareFactoryError,
    },
    #[error("stage '{stage}'{effect}: {source}")]
    InvalidBinding {
        stage: String,
        effect: String,
        #[source]
        source: obzenflow_adapters::middleware::MiddlewareAttachmentValidationError,
    },
    #[error("stage '{stage}', effect '{effect}': {source}")]
    InvalidComposition {
        stage: String,
        effect: String,
        #[source]
        source: obzenflow_adapters::middleware::ControlCompositionError,
    },
    #[error("{0}")]
    InvalidStageRow(String),
}

impl MiddlewarePlanError {
    pub(crate) fn invalid(stage: &str, effect: Option<&str>, message: impl Into<String>) -> Self {
        Self::InvalidAttachment {
            stage: stage.to_owned(),
            effect: Self::effect_context(effect),
            message: message.into(),
        }
    }
    pub(crate) fn effect_context(effect: Option<&str>) -> String {
        effect
            .map(|value| format!(", effect '{value}'"))
            .unwrap_or_default()
    }
}

#[derive(Debug, Error)]
pub enum StageCreationError {
    #[error(transparent)]
    MiddlewareFactory(#[from] MiddlewareFactoryError),

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
    /// A composite-declared internal feed payload (FLOWIP-128a D3) is not in
    /// the upstream member's output contract; the lane could never deliver.
    DeclaredFeedPayloadMissing,
}

/// An upstream emits a type the downstream input slot does not declare.
#[derive(Debug, Error)]
#[error(
    "{}",
    FlowBuildError::fmt_edge_typing_mismatch(
        .upstream_stage,
        .downstream_stage,
        *.role,
        .actual_type,
        .expected_type,
        .kind,
        .suggested_fix,
    )
)]
pub struct EdgeTypingMismatch {
    pub upstream_stage: String,
    pub downstream_stage: String,
    pub role: EdgeInputRole,
    pub expected_type: String,
    pub actual_type: String,
    pub kind: EdgeTypingMismatchKind,
    pub suggested_fix: String,
}

/// An effect may produce a fact outside its stage's outcome contract.
#[derive(Debug, Error)]
#[error(
    "Stage '{stage_name}': effect '{effect_type}' may produce fact '{fact}', which is not a \
     member of the outcome contract {contract}. Add it to the arrow (unconditional \
     producer-side containment check)."
)]
pub struct EffectFactNotInContract {
    pub stage_name: String,
    pub effect_type: String,
    pub fact: String,
    pub contract: String,
}

/// An effect declares two outcome members with the same event type.
#[derive(Debug, Error)]
#[error(
    "Stage '{stage_name}': effect '{effect_type}' declares two outcome members with the same \
     event type `{event_type}` ('{first_member}' and '{second_member}'). Dispatch is by \
     event type, so outcome members must have distinct event types."
)]
pub struct DuplicateEffectOutcomeFactEventType {
    pub stage_name: String,
    pub effect_type: String,
    pub event_type: String,
    pub first_member: String,
    pub second_member: String,
}

/// Structured error type for failures during flow construction
#[derive(Debug, Error)]
pub enum FlowBuildError {
    #[error(transparent)]
    MiddlewarePlan(Box<MiddlewarePlanError>),

    #[error("stages '{first_stage}' and '{second_stage}' subscribe to hosted ingress '{ingress_key}'; give each subscribe line its own endpoint")]
    HostedIngressSubscribedTwice {
        ingress_key: String,
        first_stage: String,
        second_stage: String,
    },

    #[error("stage '{stage_name}' subscribes to hosted ingress '{ingress_key}', which an earlier build gave to stage '{bound_stage}'; give this flow its own endpoint from hosted.http or app.ingress")]
    HostedIngressReused {
        ingress_key: String,
        stage_name: String,
        bound_stage: String,
    },

    #[error("Topology validation failed: {0}")]
    TopologyValidationFailed(#[source] Box<TopologyError>),

    /// FLOWIP-010: flow-build config resolution failed (required knob
    /// unresolved, scoped entry naming an unknown stage/edge, or a DSL
    /// candidate conflict). The message carries the full `config error at
    /// <path>` diagnostic including the scopes that may supply the knob.
    #[error("{0}")]
    ConfigResolution(#[source] Box<ConfigResolveError>),

    /// The `[app]` table did not fit the settings type, was extracted twice, or
    /// is non-empty in a build that extracts none; the message names keys, never values.
    #[error("{0}")]
    ApplicationSettings(#[source] ApplicationSettingsError),

    /// `journal.root` is set, but the flow's journal provider keeps no run
    /// location under it.
    #[error("{}", journal_root_unused(.root, .run_path.as_deref()))]
    JournalRootUnused {
        root: PathBuf,
        run_path: Option<PathBuf>,
    },

    /// `journal.sync` is set in the configuration file, but the flow records
    /// to memory journals, which never sync.
    #[error(
        "journal.sync is \"{}\", but this flow records to memory journals, which never sync; \
         unset journal.sync or record the flow with disk_journals",
        .sync.as_token()
    )]
    JournalSyncUnused { sync: JournalSync },

    #[error("flow binding '{binding}' configuration failed: {detail}")]
    BindingConfiguration { binding: String, detail: String },

    /// An `ai!` stage could not bind its model or lower its members.
    #[error("{0}")]
    AiStage(String),

    #[error(
        "flow `{flow_name}` builds two systems named `{binding}` for `{effect_type}`; build `{binding}` once and name that value in each row"
    )]
    DistinctEffectBindings {
        flow_name: &'static str,
        effect_type: &'static str,
        binding: String,
    },

    #[error(
        "flow `{flow_name}` cannot collect `{binding}` for `{effect_type}`: {detail}; build systems and bindings inside FlowDefinition::materialize"
    )]
    EffectBindingPackageUnavailable {
        flow_name: &'static str,
        effect_type: &'static str,
        binding: String,
        detail: &'static str,
    },

    /// A join or merge whose phases the build refuses: a then-live join, a
    /// `then_live` or `concat` merge, or a hold that could never end under
    /// enforced backpressure.
    #[error("{0}")]
    PhaseTopology(String),

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
        "stage '{stage_name}' attaches middleware family '{family_label}' at both \
         '{first_position}' and '{second_position}'; one family may occupy only one \
         control position per stage (FLOWIP-115s)"
    )]
    DuplicateStageMiddlewareFamily {
        stage_name: String,
        family_label: &'static str,
        first_position: &'static str,
        second_position: &'static str,
    },

    #[error(
        "Stage '{stage_name}' declares policy middleware '{middleware}' on a pure sync surface. \
         Attach the policy to a source poll or an effect call, including a sink call."
    )]
    PolicyMiddlewareOnPureStage {
        stage_name: String,
        middleware: String,
    },

    #[error(
        "Stage '{stage_name}' declares policy middleware '{middleware}' on an effectful stateful \
         stage before FLOWIP-120l installs the stateful effect boundary. Accepting this would \
         advertise protection while stateful dispatch still has effect_boundary: None."
    )]
    PolicyMiddlewareOnPendingEffectfulStateful {
        stage_name: String,
        middleware: String,
    },

    #[error(transparent)]
    EdgeTypingMismatch(Box<EdgeTypingMismatch>),

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

    #[error(transparent)]
    EffectFactNotInContract(Box<EffectFactNotInContract>),

    #[error(transparent)]
    DuplicateEffectOutcomeFactEventType(Box<DuplicateEffectOutcomeFactEventType>),

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

    /// A rebuild named one or more keys that name no sink stage of the flow.
    #[error("{}", unknown_sinks_text(run, keys, sinks))]
    UnknownSinks {
        run: std::path::PathBuf,
        keys: Vec<String>,
        sinks: Vec<String>,
    },

    /// Archive admission refused the verb. The error also contains the flow
    /// evidence that admission read.
    #[error("{refusal}")]
    ArchiveVerbRefused {
        refusal: obzenflow_core::journal::archive_actions::ArchiveRefusal,
        flow: Box<obzenflow_core::journal::archive_actions::FlowEvidence>,
    },

    #[error(
        "A resume needs a durable current run. The live continuation records new effect \
         outcomes, and an ephemeral run has no location. Nothing can resume the continuation, \
         and the next resume of the original archive executes its effects again. Change \
         `journals:` to `disk_journals(...)`. For a bounded rehearsal that executes no effects, \
         replay the archive instead."
    )]
    ResumeRefusedEphemeralRun,

    /// The resume could not take its parent's continuation claim (FLOWIP-134i
    /// B12); its run directory remains as an orphan that ran no stage.
    #[error(transparent)]
    ResumeRefusedClaim(obzenflow_core::journal::archive::ReplayError),
}

fn and_list(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

fn unknown_sinks_text(run: &std::path::Path, keys: &[String], sinks: &[String]) -> String {
    let quoted: Vec<String> = keys.iter().map(|key| format!("'{key}'")).collect();
    let cause = match keys {
        [_] => format!("the flow has no sink {}", and_list(&quoted)),
        _ => format!("the flow has no sinks {}", and_list(&quoted)),
    };
    let known = match sinks {
        [] => "This flow has no sink.".to_string(),
        [_] => format!("The sink of this flow is {}.", and_list(sinks)),
        _ => format!("The sinks of this flow are {}.", and_list(sinks)),
    };
    let near: Vec<(String, &str)> = keys
        .iter()
        .filter_map(|key| {
            obzenflow_runtime::runtime_config::error::closest_match(key, sinks)
                .map(|sink| (key.clone(), sink))
        })
        .collect();
    let hint = match near.as_slice() {
        [] => String::new(),
        [(_, sink)] if keys.len() == 1 => format!(" Did you mean '{sink}'?"),
        _ => {
            let pairs: Vec<String> = near
                .iter()
                .map(|(key, sink)| format!("'{sink}' for '{key}'"))
                .collect();
            format!(" Did you mean {}?", and_list(&pairs))
        }
    };
    format!(
        "ObzenFlow cannot rebuild from {}, because {cause}.\nName a sink with --stage. {known}{hint}",
        run.display()
    )
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
            EdgeTypingMismatchKind::DeclaredFeedPayloadMissing => format!(
                "Declared internal feed into '{downstream_stage}': lane payload \
                 '{expected_type}' is not in '{upstream_stage}''s output contract \
                 ({actual_type}). {suggested_fix}"
            ),
        }
    }
}

impl From<MiddlewarePlanError> for FlowBuildError {
    fn from(error: MiddlewarePlanError) -> Self {
        Self::MiddlewarePlan(Box::new(error))
    }
}

impl From<FlowBuildError> for String {
    fn from(err: FlowBuildError) -> Self {
        err.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::FlowBuildError;

    #[test]
    fn flow_build_error_stays_under_the_large_error_threshold() {
        let size = size_of::<FlowBuildError>();
        assert!(size < 128, "FlowBuildError is {size} bytes");
    }
}
