// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

/// Why a send got no response.
#[derive(Debug, Clone, thiserror::Error)]
pub enum HttpClientError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("request timed out: {0}")]
    Timeout(String),

    #[error("request cancelled")]
    Cancelled,

    #[error("transport error: {0}")]
    Transport(String),

    /// The request carried a body or header value that could not be encoded,
    /// so nothing was sent.
    #[error("the request could not be encoded: {0}")]
    Unencodable(String),
}
