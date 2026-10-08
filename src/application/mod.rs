// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application facade for ObzenFlow.
//!
//! Run configured flows, choose presentation, verify recorded runs, and attach
//! ingress or other web surfaces.
//!
//! Tokio Console is configured by `--tokio-console` or the startup file:
//!
//! ```toml
//! [diagnostics.tokio_console]
//! enabled = true
//! bind = "127.0.0.1:6669"
//! ```
//!
//! Build with `tokio-console` and `--cfg tokio_unstable`. Diagnostics are off by
//! default; `--tokio-console=false` overrides file or builder enablement. The
//! address resolves from `--tokio-console-bind`, file, `TOKIO_CONSOLE_BIND`,
//! builder default, then `127.0.0.1:6669`. No application-specific setup is needed.
//! Blocking launch installs instrumentation before creating its runtime; async
//! launch observes framework tasks created afterwards, not earlier caller tasks.
//! An existing global tracing subscriber prevents an explicit Console request;
//! ordinary launches continue to respect an embedding application's subscriber.

pub use obzenflow_infra::application::{
    ApplicationError, Banner, ControlPlaneAuthModeArg, CorsModeArg, CurrentRunLocator,
    FlowApplication, FlowApplicationBuilder, FlowConfig, Footer, LogLevel, OnTerminalArg,
    Presentation, ReplayRunContext, RunMode, RunPresentationOutcome, RunSubstrateState,
    StartupMode, WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext,
};

pub mod control;
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
