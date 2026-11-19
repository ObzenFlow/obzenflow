// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

/// Why a statement didn't complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresClientError {
    /// No connection was made, so the statement never reached the server.
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("statement timed out: {0}")]
    Timeout(String),

    #[error("transport error: {0}")]
    Transport(String),

    /// The server refused the statement, with the SQLSTATE it named.
    #[error("PostgreSQL refused the statement{}: {message}", .sqlstate.as_deref().map(|code| format!(" ({code})")).unwrap_or_default())]
    Refused {
        sqlstate: Option<String>,
        message: String,
    },

    /// A bound value could not be encoded, so nothing was sent.
    #[error("the statement could not be encoded: {0}")]
    Unencodable(String),
}
