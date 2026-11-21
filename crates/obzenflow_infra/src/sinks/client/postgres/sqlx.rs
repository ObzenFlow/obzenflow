// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    PostgresBindings, PostgresClient, PostgresClientError, PostgresConfigError, PostgresConnection,
    PostgresStatement,
};
use ::sqlx::postgres::PgPoolOptions;
use ::sqlx::PgPool;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// The framework's PostgreSQL client for `connection`. No connection opens
/// until the first statement runs.
pub fn default_postgres_client(
    connection: PostgresConnection,
) -> Result<Arc<dyn PostgresClient>, PostgresConfigError> {
    if connection.acquire_timeout.is_zero() || connection.operation_timeout.is_zero() {
        return Err(PostgresConfigError::InvalidTimeout);
    }
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .acquire_timeout(connection.acquire_timeout)
        .connect_lazy_with(*connection.options);
    Ok(Arc::new(SqlxPostgresClient {
        pool,
        operation_timeout: connection.operation_timeout,
    }))
}

struct SqlxPostgresClient {
    pool: PgPool,
    operation_timeout: Duration,
}

#[async_trait]
impl PostgresClient for SqlxPostgresClient {
    async fn send(
        &self,
        statement: &PostgresStatement,
        bindings: PostgresBindings,
    ) -> Result<u64, PostgresClientError> {
        let arguments = bindings.into_arguments()?;
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| PostgresClientError::Connection(error.to_string()))?;
        let query = ::sqlx::query_with(statement.sql(), arguments);
        match tokio::time::timeout(self.operation_timeout, query.execute(&mut *connection)).await {
            Ok(Ok(done)) => Ok(done.rows_affected()),
            Ok(Err(error)) => Err(statement_error(error)),
            Err(_) => Err(PostgresClientError::Timeout(format!(
                "statement on {} ran past {:?}",
                statement.destination(),
                self.operation_timeout
            ))),
        }
    }
}

fn statement_error(error: ::sqlx::Error) -> PostgresClientError {
    match error {
        ::sqlx::Error::Database(database) => PostgresClientError::Refused {
            sqlstate: database.code().map(|code| code.into_owned()),
            message: database.message().to_string(),
        },
        ::sqlx::Error::Encode(error) => PostgresClientError::Unencodable(error.to_string()),
        error => PostgresClientError::Transport(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::client::postgres::PostgresTransport;

    #[tokio::test]
    async fn zero_timeout_refused() {
        let connection = PostgresConnection::from_url(
            "postgres://user@localhost/db",
            PostgresTransport::ExternallyProtectedPlaintext,
        )
        .unwrap()
        .with_operation_timeout(Duration::ZERO);
        assert!(matches!(
            default_postgres_client(connection),
            Err(PostgresConfigError::InvalidTimeout)
        ));
    }
}
