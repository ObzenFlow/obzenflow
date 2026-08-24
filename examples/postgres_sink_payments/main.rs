// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deliver typed payment events to PostgreSQL.
//!
//! Start the repository PostgreSQL service with `cargo xtask postgres up`, then
//! run this example through `cargo xtask postgres run -- cargo run -p obzenflow
//! --features postgres --example postgres_sink_payments`.

mod domain;
mod flow;

use anyhow::{Context, Result};
use obzenflow::application::FlowApplication;
use obzenflow::sinks::postgres::{PostgresConnection, PostgresTransport};
use std::path::PathBuf;

fn main() -> Result<()> {
    let connection =
        PostgresConnection::from_env("OBZENFLOW_POSTGRES_URL", PostgresTransport::VerifiedTls)
            .context("configure the PostgreSQL connection from OBZENFLOW_POSTGRES_URL")?;
    let schema = std::env::var("OBZENFLOW_POSTGRES_SCHEMA")
        .unwrap_or_else(|_| "obzenflow_example".to_string());
    let journals = std::env::var_os("OBZENFLOW_JOURNAL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/postgres-sink-payments"));

    let payment_flow = flow::build(journals, connection, schema)?;
    FlowApplication::builder().run_blocking(payment_flow)?;
    Ok(())
}
