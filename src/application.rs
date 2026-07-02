// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application facade for ObzenFlow.
//!
//! This module re-exports the runtime orchestration types from
//! [`obzenflow_infra::application`] so applications can depend on `obzenflow`
//! alone.

pub use obzenflow_infra::application::{
    ApplicationError, Banner, CurrentRunLocator, FlowApplication, FlowApplicationBuilder,
    FlowConfig, Footer, LogLevel, Presentation, RunPresentationOutcome, RunSubstrateState,
};

/// Replay output verification (FLOWIP-095j): compare two recorded runs of the
/// same flow from their journals.
pub use obzenflow_infra::verify::{
    render_verdict, verify_run_dirs, Verdict, VerifyOptions, VerifyOutcome, MATCHED_LINE,
};
