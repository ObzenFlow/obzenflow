//! Factory functions for creating web servers
//!
//! Following the same pattern as journal factories, these functions
//! provide simple ways for users to create web servers without
//! dealing with implementation details.

#[cfg(feature = "warp-server")]
use obzenflow_core::web::WebServer;

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

/// Create a default web server.
///
/// Currently this returns a Warp server when the `warp-server` feature is enabled.
#[cfg(feature = "warp-server")]
pub fn default_server() -> Box<dyn WebServer> {
    Box::new(super::warp::WarpServer::new())
}
