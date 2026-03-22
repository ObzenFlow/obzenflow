// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Example entrypoint and application hosting for the piggy bank demo.
//!
//! `HttpIngress<T>` deliberately spans two ownership boundaries:
//! - the hosted HTTP surface + readiness/telemetry wiring belong to `FlowApplication`
//! - the typed `HttpSource` belongs inside `flow!`
//!
//! That is why this runner extracts `source()` first, then moves the full ingress
//! bundles into `FlowApplication::builder()`. The flow topology stays source-oriented,
//! while the hosting shell stays ingress/surface-oriented.

use super::domain::{AccountOpened, LedgerEntry};
use super::flow;
use anyhow::Result;
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::web::endpoints::event_ingestion::{http_ingress, IngestionConfig};

const ACCOUNTS_BASE_PATH: &str = "/api/bank/accounts";
const TX_BASE_PATH: &str = "/api/bank/tx";

fn ingress_config(base_path: &str) -> IngestionConfig {
    IngestionConfig {
        base_path: base_path.to_string(),
        ..Default::default()
    }
}

pub fn run_example() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

#[cfg(test)]
pub fn run_example_in_tests() -> Result<()> {
    Ok(())
}

async fn run() -> Result<()> {
    let accounts_ingress = http_ingress::<AccountOpened>(ingress_config(ACCOUNTS_BASE_PATH));
    // Extract the typed source before moving the bundle into FlowApplication.
    let accounts_source = accounts_ingress.source();

    let tx_ingress = http_ingress::<LedgerEntry>(ingress_config(TX_BASE_PATH));
    // Same split here: source for `flow!`, bundle for the hosting shell.
    let tx_source = tx_ingress.source();

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_http_ingress(accounts_ingress)
        .with_http_ingress(tx_ingress)
        // `build_flow(...)` intentionally takes only sources. It stays unaware of
        // HTTP hosting concerns such as readiness wiring and ingress telemetry.
        .run_async(flow::build_flow(accounts_source, tx_source))
        .await?;

    Ok(())
}
