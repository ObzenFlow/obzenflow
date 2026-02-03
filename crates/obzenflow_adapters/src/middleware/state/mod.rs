// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! State management middleware
//!
//! This module contains middleware implementations that manage state and
//! event aggregation, such as windowing and stateful transformations.

pub mod windowing;

pub use windowing::{AggregationType, WindowingMiddleware, WindowingMiddlewareFactory};
