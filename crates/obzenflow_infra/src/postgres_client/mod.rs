// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The PostgreSQL client systems speak through.
//!
//! Each call executes one statement in PostgreSQL's autocommit.

mod bindings;
mod client;
mod error;
mod statement;

pub use bindings::PostgresBindings;
pub use client::PostgresClient;
pub use error::{unsent, PostgresClientError};
pub use statement::{PostgresStatement, PostgresStatementError};

use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{ConnectOptions as _, PgPool};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// A connection configuration error, found without opening a socket.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresConfigError {
    #[error("PostgreSQL environment variable '{0}' is missing or is not valid Unicode")]
    MissingEnvironment(String),
    #[error("PostgreSQL connection options are invalid")]
    InvalidConnection,
    #[error("PostgreSQL URL transport mode conflicts with the explicit transport policy")]
    ConflictingTransport,
    #[error("PostgreSQL timeouts must be strictly positive")]
    InvalidTimeout,
}

/// The transport assurance an application states, whatever the URL says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostgresTransport {
    /// Require certificate-chain and hostname verification.
    VerifiedTls,
    /// Plaintext, because another boundary, such as loopback, a Unix socket, a
    /// sidecar or a tunnel, protects the connection.
    ExternallyProtectedPlaintext,
}

impl PostgresTransport {
    fn ssl_mode(self) -> PgSslMode {
        match self {
            Self::VerifiedTls => PgSslMode::VerifyFull,
            Self::ExternallyProtectedPlaintext => PgSslMode::Disable,
        }
    }

    fn agrees_with(self, mode: PgSslMode) -> bool {
        matches!(
            (self, mode),
            (Self::VerifiedTls, PgSslMode::VerifyFull)
                | (Self::ExternallyProtectedPlaintext, PgSslMode::Disable)
        )
    }
}

/// Parsed connection options whose formatting never shows the URL or password.
#[derive(Clone)]
pub struct PostgresConnection {
    options: Box<PgConnectOptions>,
    transport: PostgresTransport,
    acquire_timeout: Duration,
    operation_timeout: Duration,
}

impl PostgresConnection {
    /// Read a PostgreSQL URL from the environment variable `name`.
    pub fn from_env(
        name: impl AsRef<str>,
        transport: PostgresTransport,
    ) -> Result<Self, PostgresConfigError> {
        let name = name.as_ref();
        let value = std::env::var(name)
            .map_err(|_| PostgresConfigError::MissingEnvironment(name.to_string()))?;
        Self::from_url(&value, transport)
    }

    /// Parse a PostgreSQL URL without DNS, authentication or socket I/O. The
    /// transport policy overrides the URL's SSL mode, and a URL that
    /// contradicts it is refused.
    pub fn from_url(url: &str, transport: PostgresTransport) -> Result<Self, PostgresConfigError> {
        preflight_url(url, transport)?;
        let options =
            PgConnectOptions::from_str(url).map_err(|_| PostgresConfigError::InvalidConnection)?;
        Ok(Self {
            options: Box::new(
                options
                    .ssl_mode(transport.ssl_mode())
                    .disable_statement_logging(),
            ),
            transport,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        })
    }

    /// How long a statement waits for its connection.
    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// How long one statement may run once it has its connection.
    pub fn with_operation_timeout(mut self, operation_timeout: Duration) -> Self {
        self.operation_timeout = operation_timeout;
        self
    }
}

impl fmt::Debug for PostgresConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnection")
            .field("transport", &self.transport)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

fn preflight_url(url: &str, transport: PostgresTransport) -> Result<(), PostgresConfigError> {
    let parsed = Url::parse(url).map_err(|_| PostgresConfigError::InvalidConnection)?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql") {
        return Err(PostgresConfigError::InvalidConnection);
    }
    for (key, value) in parsed.query_pairs() {
        let supported = matches!(
            key.as_ref(),
            "sslmode"
                | "ssl-mode"
                | "sslrootcert"
                | "ssl-root-cert"
                | "ssl-ca"
                | "sslcert"
                | "ssl-cert"
                | "sslkey"
                | "ssl-key"
                | "statement-cache-capacity"
                | "host"
                | "hostaddr"
                | "port"
                | "dbname"
                | "user"
                | "password"
                | "application_name"
                | "options"
        ) || (key.starts_with("options[") && key.ends_with(']'));
        if !supported {
            return Err(PostgresConfigError::InvalidConnection);
        }
        if matches!(key.as_ref(), "sslmode" | "ssl-mode") {
            let mode = PgSslMode::from_str(value.as_ref())
                .map_err(|_| PostgresConfigError::InvalidConnection)?;
            if !transport.agrees_with(mode) {
                return Err(PostgresConfigError::ConflictingTransport);
            }
        }
    }
    Ok(())
}

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
        let query = sqlx::query_with(statement.sql(), arguments);
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

fn statement_error(error: sqlx::Error) -> PostgresClientError {
    match error {
        sqlx::Error::Database(database) => PostgresClientError::Refused {
            sqlstate: database.code().map(|code| code.into_owned()),
            message: database.message().to_string(),
        },
        sqlx::Error::Encode(error) => PostgresClientError::Unencodable(error.to_string()),
        error => PostgresClientError::Transport(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_contradicting_transport_refused() {
        assert_eq!(
            PostgresConnection::from_url(
                "postgres://user@localhost/db?sslmode=disable",
                PostgresTransport::VerifiedTls,
            )
            .unwrap_err(),
            PostgresConfigError::ConflictingTransport
        );
    }

    #[test]
    fn unsupported_url_option_refused() {
        assert_eq!(
            PostgresConnection::from_url(
                "postgres://user@localhost/db?unknown=1",
                PostgresTransport::ExternallyProtectedPlaintext,
            )
            .unwrap_err(),
            PostgresConfigError::InvalidConnection
        );
    }

    #[test]
    fn connection_debug_omits_password() {
        let connection = PostgresConnection::from_url(
            "postgres://user:sentinel@localhost/db",
            PostgresTransport::ExternallyProtectedPlaintext,
        )
        .unwrap();
        assert!(!format!("{connection:?}").contains("sentinel"));
    }

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
