// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deprecated alias for `flight_delays_simple`.
//!
//! Run with: `cargo run --package obzenflow --example flight_delays_simple`

mod flight_delays_simple;

fn main() -> anyhow::Result<()> {
    flight_delays_simple::main()
}
