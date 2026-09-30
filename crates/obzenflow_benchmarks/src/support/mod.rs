// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Explicit shared fixtures and measurement support for component benchmarks.
//! Framework dependencies expose only their ordinary consumer contracts.

#[cfg(feature = "journal-benchmarks")]
pub mod allocations;
#[cfg(feature = "journal-benchmarks")]
pub mod journal;
#[cfg(feature = "journal-benchmarks")]
mod measurement;
#[cfg(feature = "validation-benchmarks")]
pub mod studio;
#[cfg(feature = "validation-benchmarks")]
pub mod validation;
#[cfg(feature = "journal-benchmarks")]
mod work;

#[cfg(feature = "journal-benchmarks")]
pub use measurement::{measure, timed, Census, Meter, Sample};

pub const DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
pub const MEASUREMENT_CONTRACT: &str = "public-operations-v1";

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
