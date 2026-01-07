//! Web server implementations
//!
//! This module provides concrete implementations of the core WebServer trait
//! for different web frameworks.

#[cfg(feature = "warp-server")]
pub mod warp;

#[cfg(feature = "axum-server")]
pub mod axum;

pub mod factory;
pub mod endpoints;

#[cfg(feature = "warp-server")]
pub mod web_server;

// Re-export implementations when features are enabled
#[cfg(feature = "warp-server")]
pub use self::warp::WarpServer;

#[cfg(feature = "axum-server")]
pub use self::axum::AxumServer;

// Re-export factory functions
pub use factory::*;

// THE ONE AND ONLY web server function
#[cfg(feature = "warp-server")]
pub use web_server::{start_web_server, start_web_server_with_config};
