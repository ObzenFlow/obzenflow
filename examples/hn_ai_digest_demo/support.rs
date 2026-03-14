// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "config.rs"]
pub mod config;

#[path = "decoder.rs"]
pub mod decoder;

#[path = "domain.rs"]
pub mod domain;

#[path = "flow.rs"]
pub mod flow;

#[path = "mock_server.rs"]
pub mod mock_server;

#[path = "util.rs"]
pub mod util;

pub use config::DemoConfig;
pub use flow::run_example;
