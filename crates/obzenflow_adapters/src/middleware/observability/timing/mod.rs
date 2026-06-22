// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Timing observer middleware.

mod factory;
mod legacy;
mod middleware;
mod observers;

pub use factory::{timing, TimingFamily, TimingMiddlewareFactory};
pub use middleware::TimingMiddleware;

#[cfg(test)]
mod tests;
