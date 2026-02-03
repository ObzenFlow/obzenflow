// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::http_client::{HttpClientError, HttpResponse, RequestSpec};
use async_trait::async_trait;

#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    async fn execute(&self, request: RequestSpec) -> Result<HttpResponse, HttpClientError>;
}
