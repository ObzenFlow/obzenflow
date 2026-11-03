// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::stages::sinks::postgres::{PostgresConnection, PostgresTransport};

pub fn connection() -> PostgresConnection {
    PostgresConnection::deferred_from_env(
        "OBZENFLOW_POSTGRES_TEST_URL",
        PostgresTransport::ExternallyProtectedPlaintext,
    )
}
