//! Web server abstraction

use async_trait::async_trait;
use super::endpoint::HttpEndpoint;
use super::types::ServerConfig;
use super::error::WebError;

/// Trait for web server implementations
/// 
/// This trait is implemented by concrete web servers (Warp, Axum, Actix, etc.)
/// in the infrastructure layer. It provides a framework-agnostic interface
/// for starting servers and registering endpoints.
#[async_trait]
pub trait WebServer: Send + Sync {
    /// Register an HTTP endpoint with the server
    /// 
    /// This method should be called before starting the server.
    /// Multiple endpoints can be registered.
    fn register_endpoint(&mut self, endpoint: Box<dyn HttpEndpoint>) -> Result<(), WebError>;
    
    /// Start the server with the given configuration
    /// 
    /// This method will start the HTTP server and block until it's shut down.
    /// All endpoints should be registered before calling this method.
    async fn start(self, config: ServerConfig) -> Result<(), WebError>;
    
    /// Create a handle for graceful shutdown
    /// 
    /// This returns a shutdown handle that can be used to stop the server.
    /// Not all implementations may support this.
    fn shutdown_handle(&self) -> Option<Box<dyn ServerShutdownHandle>>;
}

/// Handle for gracefully shutting down a server
#[async_trait]
pub trait ServerShutdownHandle: Send + Sync {
    /// Signal the server to shut down gracefully
    async fn shutdown(&self) -> Result<(), WebError>;
    
    /// Check if the server is still running
    fn is_running(&self) -> bool;
}

/// Builder trait for creating web servers with a fluent API
/// 
/// This is an optional trait that implementations can provide for
/// more ergonomic server creation.
pub trait WebServerBuilder: Sized {
    /// The type of server this builder creates
    type Server: WebServer;
    
    /// Create a new builder with default settings
    fn new() -> Self;
    
    /// Add an endpoint to the server
    fn endpoint(self, endpoint: Box<dyn HttpEndpoint>) -> Self;
    
    /// Add multiple endpoints to the server
    fn endpoints(self, endpoints: Vec<Box<dyn HttpEndpoint>>) -> Self;
    
    /// Build the server
    fn build(self) -> Result<Self::Server, WebError>;
}