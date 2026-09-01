// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deliver typed payment events to PostgreSQL.
//!
//! Supply any PostgreSQL 17 backing service through `OBZENFLOW_POSTGRES_URL` and
//! optionally `OBZENFLOW_POSTGRES_SCHEMA`. For the repository-managed development
//! service, run `cargo xtask postgres up`, inspect its connection with
//! `cargo xtask postgres connection`, then launch this example through
//! `cargo xtask postgres run -- cargo run -p obzenflow --features postgres
//! --example postgres_sink_payments`.

mod domain;
mod flow;
#[path = "../support/postgres_transport.rs"]
mod postgres_transport;

use anyhow::Result;
use obzenflow::application::FlowApplication;
use obzenflow::sinks::postgres::PostgresConnection;
use std::path::PathBuf;

fn main() -> Result<()> {
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
    FlowApplication::builder().run_blocking(payment_flow)?;
    Ok(())
}
