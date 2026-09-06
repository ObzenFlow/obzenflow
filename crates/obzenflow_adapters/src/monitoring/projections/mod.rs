// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deterministic provider projections. Hosting and output belong to Infra.

pub mod console;
pub mod prometheus;
pub use console::ConsoleProjection;
pub use prometheus::PrometheusProjection;
