// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/plain_stateful.rs"]
mod support;
use support::*;

fn main() {
    let _ = obzenflow_dsl::stateful!(Input -> First => RawHandler);
}
