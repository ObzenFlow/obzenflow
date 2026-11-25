// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod http;
mod in_process;
mod list_detail;

pub use http::{HostedSource, HttpSourceConfig};
pub use in_process::{ChannelSource, ValueFeed, ValuesRead, ValuesSource};
pub use list_detail::{ListDetail, ListDetailRead};
