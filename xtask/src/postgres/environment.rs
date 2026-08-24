// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    compose::ServiceEvidence,
    config::{
        inventory_test_schema, payment_test_schema, plaintext_url, tls_url, verified_tls_url,
        DEVELOPMENT_PAYMENT_SCHEMA, INVENTORY_TEST_SCHEMA_ENV, PAYMENT_TEST_SCHEMA_ENV,
    },
    state::SessionState,
};
use crate::Result;
use std::{env, path::Path, process::Command};

pub(super) fn configure(
    command: &mut Command,
    directory: &Path,
    state: &SessionState,
    service: &ServiceEvidence,
) -> Result<()> {
    let ca_certificate = directory.join("tls/ca.crt");
    command
        .env(
            "OBZENFLOW_POSTGRES_URL",
            verified_tls_url(state.port, &ca_certificate)?,
        )
        .env("OBZENFLOW_POSTGRES_CA_CERT", &ca_certificate)
        .env("OBZENFLOW_POSTGRES_TEST_URL", plaintext_url(state.port))
        .env("OBZENFLOW_POSTGRES_TEST_RUN_ID", &state.run_id)
        .env("OBZENFLOW_POSTGRES_TEST_PROJECT", &state.project)
        .env("OBZENFLOW_POSTGRES_TEST_PORT", state.port.to_string())
        .env(
            "OBZENFLOW_POSTGRES_TEST_CONTAINER_ID",
            &service.container_id,
        )
        .env("OBZENFLOW_POSTGRES_TEST_HEALTH", &service.health)
        .env("OBZENFLOW_POSTGRES_TEST_TLS_DIR", directory.join("tls"))
        .env(PAYMENT_TEST_SCHEMA_ENV, payment_test_schema(&state.run_id))
        .env(
            INVENTORY_TEST_SCHEMA_ENV,
            inventory_test_schema(&state.run_id),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_TLS_URL",
            tls_url(state.port, "localhost"),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_WRONG_HOST_URL",
            tls_url(state.port, "127.0.0.1"),
        )
        .env("OBZENFLOW_POSTGRES_TEST_CA_CERT", &ca_certificate)
        .env(
            "OBZENFLOW_POSTGRES_TEST_UNTRUSTED_CA_CERT",
            directory.join("tls/untrusted-ca.crt"),
        );
    if env::var_os("OBZENFLOW_POSTGRES_SCHEMA").is_none() {
        command.env("OBZENFLOW_POSTGRES_SCHEMA", DEVELOPMENT_PAYMENT_SCHEMA);
    }
    Ok(())
}
