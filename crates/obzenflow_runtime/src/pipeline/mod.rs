// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline construction, public controls and supervised lifecycle execution.

pub mod builder;
pub mod config;
pub(crate) mod fsm;
pub mod handle;
mod lifecycle;
pub mod max_iterations;
mod metrics;
pub(crate) mod resources;
pub mod supervisor;
mod termination;

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod tests;

pub use builder::PipelineBuilder;
pub use config::{
    MiddlewareStackConfig, ObserverConfig, StageConfig as PipelineStageConfig, StageHandlerType,
};
pub use handle::FlowHandle;
pub use lifecycle::{FlowStartControlOutcome, FlowStopMode, PipelineControl, PipelineState};
pub use max_iterations::MaxIterations;
