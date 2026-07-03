// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime configuration model and resolver (FLOWIP-010).
//!
//! Owns the knob registry, the scoped candidate table, the
//! scope-before-source resolution ladder, the immutable resolved snapshot,
//! and the flow-build materialization view. Source acquisition (file, env,
//! CLI) lives in `obzenflow_infra`; the provenance vocabulary and evidence
//! shapes live in `obzenflow_core::config`. Placement follows the
//! `bootstrap` precedent: runtime-owned, visible to dsl, adapters, and
//! infra. Nothing here touches the process-global bootstrap install.

pub mod candidates;
pub mod error;
pub mod flow_view;
pub mod model;
pub mod resolve;
pub mod schema;

pub use candidates::{CandidateSet, ConfigValue, DslCandidates, ScopedCandidate};
pub use error::ConfigResolveError;
pub use flow_view::{FlowEffectiveConfig, FlowResolutionContext};
pub use model::{AiModelsConfig, Resolved, ResolvedRuntimeConfig, RuntimeConfigOverlay};
pub use resolve::{materialize_flow_config, ResolutionPoint};
pub use schema::{
    canonical_env_name, knob_registry, EdgeEndpoint, EnvBinding, KnobDefault, KnobSpec, KnobTarget,
    KnobType, Mutability, Redaction,
};
