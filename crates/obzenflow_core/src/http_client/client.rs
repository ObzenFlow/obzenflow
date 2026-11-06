// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::http_client::{HttpClientError, HttpRequest, HttpResponse};
use async_trait::async_trait;

/// What a system speaks through to reach its vendor.
#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    /// Send the request once and bring back the whole response. A client never
    /// retries, and answers [`HttpClientError::Unencodable`] for an unencodable
    /// body without sending.
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError>;
}
