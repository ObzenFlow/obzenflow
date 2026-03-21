// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP endpoint abstraction

use super::error::WebError;
use super::managed::ManagedResponse;
use super::types::{HttpMethod, Request};
use async_trait::async_trait;

/// Trait for HTTP endpoints that can be registered with a web server
///
/// This trait is implemented by any component that wants to expose HTTP endpoints.
/// The implementation is framework-agnostic.
#[async_trait]
pub trait HttpEndpoint: Send + Sync {
    /// The path pattern this endpoint handles
    ///
    /// Examples:
    /// - "/metrics" for exact path
    /// - "/api/v1/users" for exact path
    /// - Could support patterns in the future like "/api/v1/users/:id"
    fn path(&self) -> &str;

    /// HTTP methods this endpoint supports
    ///
    /// Return an empty slice to support all methods
    fn methods(&self) -> &[HttpMethod];

    /// Handle an incoming HTTP request
    ///
    /// This method is called when a request matches this endpoint's path and method.
    /// The implementation should process the request and return an appropriate response.
    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError>;

    /// Optional method to check if the endpoint is healthy
    ///
    /// This can be used for health checks and readiness probes.
    /// Default implementation always returns true.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Optional method to get endpoint metadata
    ///
    /// This can be used for documentation, monitoring, etc.
    /// Default implementation returns None.
    fn metadata(&self) -> Option<EndpointMetadata> {
        None
    }
}

/// Metadata about an endpoint
#[derive(Debug, Clone)]
pub struct EndpointMetadata {
    /// Human-readable name for the endpoint
    pub name: String,

    /// Description of what the endpoint does
    pub description: Option<String>,

    /// Version of the endpoint API
    pub version: Option<String>,

    /// Tags for categorization
    pub tags: Vec<String>,
}

impl EndpointMetadata {
    /// Create new endpoint metadata
    pub fn new(name: String) -> Self {
        Self {
            name,
            description: None,
            version: None,
            tags: Vec::new(),
        }
    }

    /// Add a description
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Add a version
    pub fn with_version(mut self, version: String) -> Self {
        self.version = Some(version);
        self
    }

    /// Add tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}
