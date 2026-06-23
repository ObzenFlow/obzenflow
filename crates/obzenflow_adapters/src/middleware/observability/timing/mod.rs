// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The built-in timing observer: the framework's value-preserving
//! processing-time stamp, attached to every stage by default. User-facing
//! latency *evidence* is the sibling `indicator` module.

mod middleware;
mod observers;

pub use middleware::TimingMiddleware;
