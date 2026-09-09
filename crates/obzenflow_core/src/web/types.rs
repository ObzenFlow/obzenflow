// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core HTTP types used by web abstractions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// HTTP request methods
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

impl HttpMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Head => "HEAD",
            HttpMethod::Options => "OPTIONS",
        }
    }
}

/// Simplified HTTP request representation
///
/// This is framework-agnostic and can be created from any web framework's request type
#[derive(Debug, Clone)]
pub struct Request {
    /// HTTP method
    pub method: HttpMethod,

    /// Request path (without query string)
    pub path: String,

    /// Registered route template matched for this request.
    ///
    /// For exact routes this is typically equal to `path`, but for parameterised
    /// routes this preserves the template (e.g. `/items/:id`) while `path` is
    /// the raw request path (e.g. `/items/123`).
    pub matched_route: String,

    /// Extracted path parameters from the matched route.
    pub path_params: HashMap<String, String>,

    /// HTTP headers
    pub headers: HashMap<String, String>,

    /// Query parameters
    pub query_params: HashMap<String, String>,

    /// Request body as bytes
    pub body: Vec<u8>,
}

impl Request {
    /// Create a new request
    pub fn new(method: HttpMethod, path: String) -> Self {
        Self {
            method,
            matched_route: path.clone(),
            path,
            path_params: HashMap::new(),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body: Vec::new(),
        }
    }

    /// Add a header to the request
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Add a query parameter
    pub fn with_query_param(mut self, key: String, value: String) -> Self {
        self.query_params.insert(key, value);
        self
    }

    /// Set the request body
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
}

/// Simplified HTTP response representation
///
/// This is framework-agnostic and can be converted to any web framework's response type
#[derive(Debug, Clone)]
pub struct Response {
    /// HTTP status code
    pub status: u16,

    /// Response headers
    pub headers: HashMap<String, String>,

    /// Response body as bytes
    pub body: Vec<u8>,
}

impl Response {
    /// Create a new response with status code
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }

    /// Create an OK (200) response
    pub fn ok() -> Self {
        Self::new(200)
    }

    /// Create a Not Found (404) response
    pub fn not_found() -> Self {
        Self::new(404)
    }

    /// Create an Internal Server Error (500) response
    pub fn internal_error() -> Self {
        Self::new(500)
    }

    /// Add a header to the response
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Set the response body from bytes
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// Set the response body from a string
    pub fn with_text(mut self, text: &str) -> Self {
        self.body = text.as_bytes().to_vec();
        self.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        self
    }

    /// Set the response body as JSON
    pub fn with_json<T: Serialize>(mut self, value: &T) -> Result<Self, serde_json::Error> {
        self.body = serde_json::to_vec(value)?;
        self.headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        Ok(self)
    }
}
