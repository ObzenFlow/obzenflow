// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application framework for ObzenFlow
//!
//! Provides Spring Boot-style lifecycle management for flows with automatic
//! CLI parsing, server management, and graceful shutdown handling.

mod config;
mod error;
mod flow_application;
mod presentation;
mod run_mode;
mod web_surface;

pub use config::FlowConfig;
pub use error::ApplicationError;
pub use flow_application::{FlowApplication, FlowApplicationBuilder, LogLevel};
pub use obzenflow_runtime::journal::{CurrentRunLocator, RunSubstrateState};
pub use presentation::{Banner, Footer, Presentation, RunPresentationOutcome};
pub use run_mode::{ReplayRunContext, RunMode};
pub use web_surface::{WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext};
