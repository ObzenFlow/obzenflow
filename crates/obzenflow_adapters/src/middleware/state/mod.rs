//! State management middleware
//!
//! This module contains middleware implementations that manage state and
//! event aggregation, such as windowing and stateful transformations.

pub mod windowing;

pub use windowing::{AggregationType, WindowingMiddleware, WindowingMiddlewareFactory};
