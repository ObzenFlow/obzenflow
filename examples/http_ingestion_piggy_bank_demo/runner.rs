// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the piggy bank demo.

use super::domain::{AccountOpened, LedgerEntry};
use super::{flow, ingress};
use anyhow::Result;
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::web::endpoints::event_ingestion::http_ingress;

pub fn run_example() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}

async fn run() -> Result<()> {
    let accounts_ingress =
        http_ingress::<AccountOpened>(ingress::config(ingress::ACCOUNTS_BASE_PATH));
    let accounts_source = accounts_ingress.source();

    let tx_ingress = http_ingress::<LedgerEntry>(ingress::config(ingress::TX_BASE_PATH));
    let tx_source = tx_ingress.source();

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_http_ingress(accounts_ingress)
        .with_http_ingress(tx_ingress)
        .run_async(flow::build_flow(accounts_source, tx_source))
        .await?;

    Ok(())
}
