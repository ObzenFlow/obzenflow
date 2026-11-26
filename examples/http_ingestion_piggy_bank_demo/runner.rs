// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the piggy bank demo.

use super::flow::{self, ACCOUNTS_BASE_PATH, ACH_BASE_PATH, TX_BASE_PATH};
use anyhow::Result;
use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};

const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/http_ingestion_piggy_bank_demo/obzenflow.toml"
);

pub fn run_example() -> Result<()> {
    let presentation = Presentation::new(
        Banner::new("Piggy Bank")
            .description(
                "Batch and live data in one checkbook: accounts load from YAML and \
                 entries and ACH debits post from CSV, then live accounts, entries and \
                 debits arrive over HTTP.",
            )
            .config("accounts", format!("POST {ACCOUNTS_BASE_PATH}/events"))
            .config("transactions", format!("POST {TX_BASE_PATH}/events"))
            .config("ach debits", format!("POST {ACH_BASE_PATH}/events")),
    );

    let app = FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation);
    let hosted = app.hosted_ingress();

    app.run_blocking(flow::build_flow(hosted))?;

    Ok(())
}
