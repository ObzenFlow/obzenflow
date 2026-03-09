// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deprecated alias for `flight_delays_simple`.
//!
//! Run with: `cargo run --package obzenflow --example flight_delays_simple`

fn main() {
    eprintln!(
        "This example has been renamed to `flight_delays_simple`.\n\
         Run: cargo run --package obzenflow --example flight_delays_simple"
    );
    std::process::exit(1);
}
