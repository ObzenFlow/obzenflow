// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Authoring and execution context that is not a stored record component.

pub mod middleware_execution_scope;
pub mod processing_context;
pub mod stage_type;

pub use middleware_execution_scope::MiddlewareExecutionScope;
pub use processing_context::ProcessingContext;
pub use stage_type::{SimpleStageType, StageType};
