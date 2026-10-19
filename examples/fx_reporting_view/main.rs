// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prices a day of payments for a reporting view at a simulated treasury's
//! hourly rates. Run from the repository root:
//!
//! ```text
//! cargo run -p obzenflow --example fx_reporting_view
//! ```

mod domain;
mod flow;

use obzenflow::application::{FlowApplication, LogLevel};

fn main() -> std::process::ExitCode {
    let config_file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/fx_reporting_view/obzenflow.toml"
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
