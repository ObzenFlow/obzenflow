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

pub use candidates::{CandidateSet, ConfigValue, DslCandidates, DslConfigDefault, ScopedCandidate};
pub use error::ConfigResolveError;
pub use flow_view::{
    BackpressureMode, ExactConfigView, FlowEffectiveConfig, FlowResolutionContext,
};
pub use model::{
    diff, doc_for, doc_for_at, AiModelsConfig, ConfigDiffEntry, Resolved, ResolvedRuntimeConfig,
    RuntimeConfigOverlay,
};
pub use resolve::{materialize_flow_config, ResolutionPoint};
pub use schema::{
    canonical_env_name, knob, knob_registry, schema_view, EdgeEndpoint, EnvBinding, KnobDefault,
    KnobSchemaDoc, KnobSpec, KnobTarget, KnobType, Mutability, Redaction,
    CIRCUIT_BREAKER_THRESHOLD_KEY, RATE_LIMITER_BURST_CAPACITY_KEY,
    RATE_LIMITER_EVENTS_PER_SECOND_KEY, RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
    RESILIENCE_BREAKER_COUNT_WINDOW_KEY, RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
    RESILIENCE_BREAKER_MINIMUM_CALLS_KEY, RESILIENCE_BREAKER_MODE_KEY,
    RESILIENCE_BREAKER_OPEN_FOR_MS_KEY, RESILIENCE_BREAKER_PROBES_KEY,
    RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
    RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY, RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
    RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY, RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY,
    RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY, RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY,
    RESILIENCE_RETRY_FIXED_DELAY_MS_KEY, RESILIENCE_RETRY_KIND_KEY,
    RESILIENCE_RETRY_MAX_ATTEMPTS_KEY, RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY,
};
