// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common components shared across all stage types

pub(crate) mod backpressure_activity_pulse;
pub mod control_strategies;
pub(crate) mod cycle_guard;
pub mod handler_error;
pub mod handlers;
pub(crate) mod heartbeat;
pub mod middleware_mirror;
pub mod source_handle;
pub mod stage_handle;
pub(crate) mod supervision;

pub use heartbeat::HeartbeatConfig;
pub use heartbeat::LivenessRegistry;

// Re-export handler traits for convenience
pub use handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, ObserverHandler, ResourceManaged, SinkHandler,
    StatefulHandler, TransformHandler,
};

// Re-export handler error type so stage code can depend on a single error enum
// for handler failures without pulling in stage coordination errors.
pub use handler_error::HandlerError;

// Re-export control strategies
pub use control_strategies::{
    BackoffStrategy, CompositeStrategy, ControlEventAction, ControlEventStrategy,
    JonestownStrategy, ProcessingContext, RetryStrategy, WindowingStrategy,
};
