// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::effects::postgres::{PostgresConnection, PostgresTransport};

pub fn connection() -> PostgresConnection {
    PostgresConnection::from_env(
        "OBZENFLOW_POSTGRES_TEST_URL",
        PostgresTransport::ExternallyProtectedPlaintext,
    )
    .expect("OBZENFLOW_POSTGRES_TEST_URL names a PostgreSQL connection")
}
