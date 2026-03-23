// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Taxonomy helpers for viewing ObzenFlow metrics.
//!
//! These types provide Prometheus query snippets and dashboard JSON for different
//! monitoring lenses. They do not drive runtime instrumentation.

pub mod golden_signals;
pub mod red;
pub mod saafe;
pub mod use_taxonomy;

pub use golden_signals::GoldenSignals;
pub use red::RED;
pub use saafe::SAAFE;
pub use use_taxonomy::USE;
