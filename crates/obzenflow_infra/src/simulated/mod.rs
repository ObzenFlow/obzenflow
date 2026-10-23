// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Building blocks for simulated outside systems, the stand-ins examples and
//! tests run in place of real vendors.

mod call_log;

pub(crate) use call_log::{read_lines, CallLogLine};
pub use call_log::{CallLog, CallLogError};
