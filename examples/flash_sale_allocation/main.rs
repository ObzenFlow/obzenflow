// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flash-sale allocation: an effectful-stateful Process Manager witness.
//!
//! Run live, then replay the emitted run directory with verification:
//!
//! ```text
//! cargo run -p obzenflow --example flash_sale_allocation
//! cargo run -p obzenflow --example flash_sale_allocation -- \
//!   --replay-from target/flash-sale-allocation-logs/flows/<flow_id> --verify
//! ```

mod allocation;
mod desk;
mod domain;
mod flow;
mod warehouse;

use obzenflow::application::{FlowApplication, LogLevel};

fn main() -> std::process::ExitCode {
    let config_file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/flash_sale_allocation/obzenflow.toml"
    );
    match FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_config_file(config_file)
        .run_blocking(flow::build_flow())
    {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::from(error.process_exit_code())
        }
    }
}
