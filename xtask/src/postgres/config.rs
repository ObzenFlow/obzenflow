// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{error, Result};
use std::path::Path;
use url::Url;

pub(super) const COMPOSE_FILE: &str = "dev/postgres/compose.yml";
pub(super) const SESSION_ROOT: &str = "target/postgres-sessions";
pub(super) const DEVELOPMENT_SESSION: &str = "development";
pub(super) const SESSION_OVERRIDE_ENV: &str = "OBZENFLOW_POSTGRES_XTASK_PROOF_SESSION";
pub(super) const STATE_FILE: &str = "state.tsv";
pub(super) const LOG_FILE: &str = "postgres.log";
pub(super) const IMAGE: &str = "postgres:17";
pub(super) const POSTGRES_USER: &str = "obzenflow";
pub(super) const POSTGRES_DATABASE: &str = "obzenflow";
pub(super) const POSTGRES_PASSWORD: &str = "obzenflow-secret-083c";
pub(super) const DEVELOPMENT_PAYMENT_SCHEMA: &str = "obzenflow_example";
pub(super) const PAYMENT_TEST_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_EXAMPLE_SCHEMA";
pub(super) const INVENTORY_TEST_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_INVENTORY_SCHEMA";
pub(super) const HN_DIGEST_TEST_SCHEMA_ENV: &str = "OBZENFLOW_POSTGRES_HN_DIGEST_SCHEMA";

pub(super) fn payment_test_schema(run_id: &str) -> String {
    format!("obz083c_example_{run_id}")
}

pub(super) fn inventory_test_schema(run_id: &str) -> String {
    format!("obz083c_inventory_{run_id}")
}

pub(super) fn hn_digest_test_schema(run_id: &str) -> String {
    format!("obz010o_hn_digest_{run_id}")
}

pub(super) fn plaintext_url(port: u16) -> String {
    format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{port}/{POSTGRES_DATABASE}?sslmode=disable"
    )
}

pub(super) fn tls_url(port: u16, host: &str) -> String {
    format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DATABASE}?sslmode=verify-full"
    )
}

pub(super) fn verified_tls_url(port: u16, ca_certificate: &Path) -> Result<String> {
    let mut url = Url::parse(&tls_url(port, "localhost"))
        .map_err(|_| error("failed to construct the PostgreSQL TLS URL"))?;
    let ca_certificate = ca_certificate
        .to_str()
        .ok_or_else(|| error("PostgreSQL CA path is not valid Unicode"))?;
    url.query_pairs_mut()
        .append_pair("sslrootcert", ca_certificate);
    Ok(url.into())
}
