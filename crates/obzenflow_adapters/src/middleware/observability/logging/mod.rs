// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Logging observer middleware.

mod factory;
mod middleware;
mod observers;

pub use factory::{log_event, LoggingFamily, LoggingMiddlewareFactory};
pub use middleware::LoggingMiddleware;
pub use obzenflow_core::event::payloads::observability_payload::LoggingLevel;

#[cfg(test)]
mod tests;
