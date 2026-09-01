// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::sinks::postgres::{PostgresConnection, PostgresTransport};
use sqlx::PgPool;

pub(crate) fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required from the PostgreSQL test environment"))
}

pub(crate) async fn pool() -> PgPool {
    PgPool::connect(&required_env("OBZENFLOW_POSTGRES_TEST_URL"))
        .await
        .expect("connect to the repository PostgreSQL test service")
}

#[allow(dead_code)] // The black-box example target needs only the pool helper.
pub(crate) fn connection() -> PostgresConnection {
    PostgresConnection::deferred_from_env(
        "OBZENFLOW_POSTGRES_TEST_URL",
        PostgresTransport::ExternallyProtectedPlaintext,
    )
}
