// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application-level conformance kits.

pub mod journal;
#[doc(hidden)]
pub mod journal_bench;
pub mod sink;
#[cfg(feature = "warp-server")]
pub mod studio;
#[cfg(all(feature = "warp-server", feature = "bench-instrumentation"))]
pub mod studio_capacity;
