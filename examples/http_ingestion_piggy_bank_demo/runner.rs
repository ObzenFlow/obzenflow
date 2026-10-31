// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the piggy bank demo.

use super::flow::{self, AccountIngress, AchIngress, LedgerIngress};
use anyhow::Result;
use obzenflow::application::ingress::IngestionConfig;
use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};

const ACCOUNTS_BASE_PATH: &str = "/api/bank/accounts";
const TX_BASE_PATH: &str = "/api/bank/tx";
const ACH_BASE_PATH: &str = "/api/bank/ach";
const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/http_ingestion_piggy_bank_demo/obzenflow.toml"
);

fn ingress_config(base_path: &str) -> IngestionConfig {
    IngestionConfig {
        base_path: base_path.to_string(),
        ..Default::default()
    }
}

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

    let mut app = FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation);
    let new_accounts_source = app.http_ingress(AccountIngress, ingress_config(ACCOUNTS_BASE_PATH));
    let tx_source = app.http_ingress(LedgerIngress, ingress_config(TX_BASE_PATH));
    let ach_source = app.http_ingress(AchIngress, ingress_config(ACH_BASE_PATH));

    app.run_blocking(flow::build_flow(new_accounts_source, tx_source, ach_source))?;

    Ok(())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}
