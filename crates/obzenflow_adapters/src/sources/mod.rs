// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

mod in_process;
mod list_detail;

pub use in_process::{ValueFeed, ValuesRead, ValuesSource};
pub use list_detail::{ListDetail, ListDetailRead, ListDetailStep};
