// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform stage implementation
//!
//! Transforms are the workhorses of the pipeline - they process events
//! from upstream stages and emit transformed events downstream.
//!
//! Key features:
//! - Start processing immediately (no waiting)
//! - Ordered sequential processing (one event at a time)
//! - Owned handler storage (no stage-level locks)
//! - Control event strategies for customizing behavior
//! - Can filter (0 outputs), pass through (1 output), or expand (N outputs)

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod strategies;
pub mod supervisor;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
pub use builder::{AsyncTransformBuilder, TransformBuilder};
pub use config::TransformConfig;
pub use fsm::{TransformEvent, TransformState};
pub use handle::{TransformHandle, TransformHandleExt};

// Re-export transform strategies for ergonomic imports (FLOWIP-080h)
pub use strategies::{
    AsyncMap, AsyncMapTyped, AsyncTryMapWith, AsyncTryMapWithTyped, Filter, FilterMap,
    FilterMapTyped, FilterTyped, Map, MapTyped, TryMap, TryMapWith, TryMapWithTyped,
};

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    BackoffStrategy, CompositeStrategy, ControlEventAction, ControlEventStrategy,
    JonestownStrategy, RetryStrategy, WindowingStrategy,
};

// Note: TransformSupervisor is NOT exported! It's an implementation detail.
