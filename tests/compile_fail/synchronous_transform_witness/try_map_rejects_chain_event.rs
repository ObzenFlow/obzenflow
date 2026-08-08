// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/synchronous_transform.rs"]
mod support;
use obzenflow_core::ChainEvent;
use support::First;

fn main() {
    let _ = obzenflow::typed::transforms::try_map(
        |_event: ChainEvent| Ok::<First, &'static str>(First),
    );
}
