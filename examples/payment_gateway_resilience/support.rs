// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[cfg(not(test))]
#[path = "config.rs"]
pub mod config;

#[path = "domain.rs"]
pub mod domain;

#[path = "fixtures.rs"]
pub mod fixtures;

#[path = "sources.rs"]
pub mod sources;

#[path = "sinks.rs"]
pub mod sinks;

#[path = "flow.rs"]
pub mod flow;

#[cfg(not(test))]
pub use config::DemoConfig;
#[cfg(not(test))]
pub use flow::run_example;
