//! Transform stage implementation
//!
//! Transforms are the workhorses of the pipeline - they process events
//! from upstream stages and emit transformed events downstream.
//!
//! Key features:
//! - Start processing immediately (no waiting)
//! - Stateless processing (handler uses Arc<H> not Arc<RwLock<H>>)
//! - Control event strategies for customizing behavior
//! - Can filter (0 outputs), pass through (1 output), or expand (N outputs)

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;
pub mod helpers;

// Public API - only expose builder, handle, and essential types
pub use builder::TransformBuilder;
pub use config::TransformConfig;
pub use handle::{TransformHandle, TransformHandleExt};
pub use fsm::{TransformState, TransformEvent};
pub use crate::stages::common::handlers::TransformHandler;

// Re-export helpers for ergonomic imports (FLOWIP-080h)
pub use helpers::{Filter, FilterTyped, Map, MapTyped, TryMap, TryMapWith, TryMapWithTyped};

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    ControlEventStrategy, ControlEventAction,
    JonestownStrategy, RetryStrategy, BackoffStrategy,
    WindowingStrategy, CompositeStrategy,
};

// Note: TransformSupervisor is NOT exported! It's an implementation detail.