// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application facade for ObzenFlow.
//!
//! This module re-exports the runtime orchestration types from
//! [`obzenflow_infra::application`] so applications can depend on `obzenflow`
//! alone.

pub use obzenflow_infra::application::{
    ApplicationError, Banner, FlowApplication, FlowApplicationBuilder, FlowConfig, Footer,
    LogLevel, Presentation, RunPresentationOutcome,
};
