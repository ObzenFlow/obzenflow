//! Core web server abstractions
//!
//! This module defines the abstract traits for web servers and HTTP endpoints,
//! following the same pattern as the journal abstractions.
//! Concrete implementations are provided in obzenflow_infra.

pub mod endpoint;
pub mod error;
pub mod server;
pub mod types;

// Re-export core types
pub use endpoint::{EndpointMetadata, HttpEndpoint};
pub use error::WebError;
pub use server::WebServer;
pub use types::{CorsConfig, CorsMode, HttpMethod, Request, Response, ServerConfig};
