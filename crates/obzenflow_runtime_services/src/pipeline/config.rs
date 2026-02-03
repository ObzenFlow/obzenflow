// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline and stage configuration types

use crate::stages::common::handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::StageId;

/// Stage handler type that can be converted to BoxedStageHandle
pub enum StageHandlerType {
    FiniteSource(Box<dyn FiniteSourceHandler>),
    InfiniteSource(Box<dyn InfiniteSourceHandler>),
    Transform(Box<dyn TransformHandler>),
    Sink(Box<dyn SinkHandler>),
    // TODO: FLOWIP-080 will fix Stateful with proper type erasure for associated types
}

/// Stage configuration data - metadata about the stage
pub struct StageConfig {
    pub stage_id: StageId,
    pub name: String,
    pub flow_name: String,
}

// TODO: Observers need redesign for FLOWIP-084
// For now, keeping a placeholder struct

/// Observer configuration - for side effects like monitoring
/// NOTE: This needs redesign as part of FLOWIP-084 completion
pub struct ObserverConfig {
    pub name: String,
    // TODO: Replace with appropriate observer handler trait when designed
}
