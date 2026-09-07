// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private configuration for the managed listener.

use obzenflow_core::web::AuthPolicy;

/// Server configuration
#[derive(Debug, Clone)]
pub(crate) struct HostConfig {
    /// Host to bind to (e.g., "0.0.0.0" or "127.0.0.1")
    pub host: String,

    /// Port to listen on
    pub port: u16,

    /// Maximum request body size in bytes (default: 10MB)
    pub max_body_size: Option<usize>,

    /// Request timeout in seconds (default: 30)
    pub request_timeout_secs: Option<u64>,

    /// Cross-origin request configuration (CORS).
    ///
    /// If unset, the web server implementation decides a default.
    pub cors: Option<HostCorsConfig>,

    /// Optional auth policy for framework-owned control-plane routes such as
    /// `/api/flow/*`, `/api/topology`, and `/metrics`.
    pub control_plane_auth: Option<AuthPolicy>,
}

/// CORS configuration.
#[derive(Debug, Clone)]
pub(crate) struct HostCorsConfig {
    pub mode: HostCorsMode,
}

#[derive(Debug, Clone)]
pub(crate) enum HostCorsMode {
    /// Adds permissive CORS headers (`Access-Control-Allow-Origin: *`).
    ///
    /// This is convenient for local development but dangerous for production if the API is
    /// protected only by secrets in headers (e.g. API keys).
    AllowAnyOrigin,
    /// Adds CORS headers for the given origin allow-list.
    AllowList(Vec<String>),
    /// Do not add CORS headers (browser same-origin policy applies).
    SameOrigin,
}

impl Default for HostCorsConfig {
    fn default() -> Self {
        Self {
            mode: HostCorsMode::SameOrigin,
        }
    }
}

impl HostConfig {
    /// Create a new server configuration
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            max_body_size: None,
            request_timeout_secs: None,
            cors: None,
            control_plane_auth: None,
        }
    }

    /// Create a default configuration for localhost
    #[cfg(test)]
    pub fn localhost(port: u16) -> Self {
        Self::new("127.0.0.1".to_string(), port)
    }

    /// Get the full address string
    pub fn address(&self) -> String {
        self.host
            .parse::<std::net::IpAddr>()
            .map(|host| std::net::SocketAddr::new(host, self.port).to_string())
            .unwrap_or_else(|_| format!("{}:{}", self.host, self.port))
    }
}
