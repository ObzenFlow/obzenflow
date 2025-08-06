//! Factory functions for creating web servers
//!
//! Following the same pattern as journal factories, these functions
//! provide simple ways for users to create web servers without
//! dealing with implementation details.

use obzenflow_core::web::{WebServer, WebError};

/// Create a Warp-based web server
/// 
/// # Example
/// ```ignore
/// use obzenflow_infra::web::warp_server;
/// 
/// let mut server = warp_server();
/// server.register_endpoint(Box::new(my_endpoint));
/// server.start(config).await?;
/// ```
#[cfg(feature = "warp-server")]
pub fn warp_server() -> impl WebServer {
    super::warp::WarpServer::new()
}

/// Create an Axum-based web server
/// 
/// # Example
/// ```ignore
/// use obzenflow_infra::web::axum_server;
/// 
/// let mut server = axum_server();
/// server.register_endpoint(Box::new(my_endpoint));
/// server.start(config).await?;
/// ```
#[cfg(feature = "axum-server")]
pub fn axum_server() -> impl WebServer {
    super::axum::AxumServer::new()
}

/// Create a default web server (uses Warp if available, otherwise Axum)
/// 
/// This function automatically selects an available web server implementation.
/// Useful when you don't care which specific framework is used.
#[cfg(any(feature = "warp-server", feature = "axum-server"))]
pub fn default_server() -> Box<dyn WebServer> {
    #[cfg(feature = "warp-server")]
    {
        Box::new(super::warp::WarpServer::new())
    }
    #[cfg(all(not(feature = "warp-server"), feature = "axum-server"))]
    {
        Box::new(super::axum::AxumServer::new())
    }
}