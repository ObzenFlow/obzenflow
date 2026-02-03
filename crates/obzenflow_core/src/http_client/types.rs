// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::web::HttpMethod;
use bytes::Bytes;
use http::HeaderMap;
use url::Url;

#[derive(Debug, Clone)]
pub struct RequestSpec {
    pub method: HttpMethod,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
}

impl RequestSpec {
    pub fn new(method: HttpMethod, url: Url) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: None,
        }
    }

    pub fn get(url: Url) -> Self {
        Self::new(HttpMethod::Get, url)
    }

    pub fn post(url: Url) -> Self {
        Self::new(HttpMethod::Post, url)
    }

    pub fn with_header(
        mut self,
        name: http::header::HeaderName,
        value: http::header::HeaderValue,
    ) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub fn with_query(mut self, key: &str, value: &str) -> Self {
        self.url.query_pairs_mut().append_pair(key, value);
        self
    }

    pub fn with_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }
}
