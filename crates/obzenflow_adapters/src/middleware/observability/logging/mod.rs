// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Logging observer middleware.

mod legacy;
mod middleware;
mod observers;

pub use middleware::LoggingMiddleware;

#[cfg(test)]
mod tests;
