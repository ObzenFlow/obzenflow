// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application fixture for PostgreSQL delivery and archive-redelivery tests.
//! Service setup and destination inspection remain in the acceptance tests.

mod domain;
mod flow;
#[path = "../../../examples/support/postgres_transport.rs"]
mod postgres_transport;

use anyhow::Result;
use obzenflow::application::FlowApplication;
use obzenflow::stages::sinks::postgres::PostgresConnection;
use std::{ffi::OsString, path::PathBuf};

pub fn run(args: Vec<OsString>) -> Result<()> {
    let connection = PostgresConnection::deferred_from_env(
        "OBZENFLOW_POSTGRES_URL",
        postgres_transport::from_environment()?,
    );
    let schema = std::env::var("OBZENFLOW_POSTGRES_SCHEMA")
        .unwrap_or_else(|_| "obzenflow_example".to_string());
    let journals = std::env::var_os("OBZENFLOW_JOURNAL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/postgres-sink-payments"));

    let payment_flow = flow::build(journals, connection, schema)?;
    FlowApplication::builder()
        .with_cli_args(args)
        .run_blocking(payment_flow)?;
    Ok(())
}
