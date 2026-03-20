// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the piggy bank demo.

use super::domain::{AccountOpened, LedgerEntry};
use super::{flow, ingress};
use anyhow::Result;
use obzenflow_infra::application::{FlowApplication, LogLevel};

pub fn run_example() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}

async fn run() -> Result<()> {
    let ingress::TypedIngress {
        surface: accounts_surface,
        source: accounts_source,
    } = ingress::typed_ingress::<AccountOpened>(ingress::ACCOUNTS_BASE_PATH);

    let ingress::TypedIngress {
        surface: tx_surface,
        source: tx_source,
    } = ingress::typed_ingress::<LedgerEntry>(ingress::TX_BASE_PATH);

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_web_surface(accounts_surface)
        .with_web_surface(tx_surface)
        .run_async(flow::build_flow(accounts_source, tx_source))
        .await?;

    Ok(())
}
