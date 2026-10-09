// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the piggy bank demo.
//!
//! The application builder hosts HTTP ingress and returns the typed source for
//! `flow!`. Readiness, telemetry and shutdown wiring stay with the builder. The
//! accounts source is cold configuration; the file opens only for a live run.

use super::flow::{self, AccountYaml, LedgerIngress};
use anyhow::Result;
use obzenflow::application::ingress::IngestionConfig;
use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};
use obzenflow::stages::sources::{YamlSelection, YamlSource};

const TX_BASE_PATH: &str = "/api/bank/tx";
const ACCOUNTS_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/http_ingestion_piggy_bank_demo/accounts.yaml"
);
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
                "Stream-table join: accounts load from YAML, transactions post over \
                 HTTP against them, balances fold into a live checkbook.",
            )
            .config("accounts", "accounts.yaml, fixed at startup")
            .config("transactions", format!("POST {TX_BASE_PATH}/events")),
    );

    let accounts_source = YamlSource::builder(AccountYaml)
        .path(ACCOUNTS_FILE)
        .selection(YamlSelection::SequenceAt("/accounts".into()))
        .build()?;

    let mut app = FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation);
    let tx_source = app.http_ingress(LedgerIngress, ingress_config(TX_BASE_PATH));

    app.run_blocking(flow::build_flow(accounts_source, tx_source))?;

    Ok(())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}
