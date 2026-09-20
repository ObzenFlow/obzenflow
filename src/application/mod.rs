// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application facade for ObzenFlow.
//!
//! Run configured flows, choose presentation, verify recorded runs, and attach
//! ingress or other web surfaces.

pub use obzenflow_infra::application::{
    ApplicationError, Banner, ControlPlaneAuthModeArg, CorsModeArg, CurrentRunLocator,
    FlowApplication, FlowApplicationBuilder, FlowConfig, Footer, LogLevel, OnTerminalArg,
    Presentation, ReplayRunContext, RunMode, RunPresentationOutcome, RunSubstrateState,
    StartupMode, WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext,
};

pub mod ingress;

pub use obzenflow_core::config::{
    ConfigAddress, ConfigScope, ConfigSource, ConfigSubject, ConfigValueMeta, ResolvedForDoc,
    ResolvedValueDoc, SecretRef, SecretResolveError, SecretString,
};
pub use obzenflow_runtime::runtime_config::model::OverlayEntry;
pub use obzenflow_runtime::runtime_config::{
    AiModelsConfig, BackpressureMode, CandidateSet, ConfigResolveError, ConfigValue,
    ExactConfigView, FlowEffectiveConfig, ResolutionPoint, Resolved, ResolvedRuntimeConfig,
    RuntimeConfigOverlay, ScopedCandidate,
};

/// Replay output verification (FLOWIP-095j): compare two recorded runs of the
/// same flow from their journals.
pub use obzenflow_infra::verify::{
    render_verdict, verify_run_dirs, Verdict, VerifyOptions, VerifyOutcome, MATCHED_LINE,
};
