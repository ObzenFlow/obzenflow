//! Web server implementations
//!
//! This module provides concrete implementations of the core WebServer trait
//! for different web frameworks.

#[cfg(feature = "warp-server")]
pub mod warp;

pub mod endpoints;
pub mod factory;

#[cfg(feature = "warp-server")]
pub mod web_server;

// Re-export implementations when features are enabled
#[cfg(feature = "warp-server")]
pub use self::warp::WarpServer;

// Re-export factory functions

// THE ONE AND ONLY web server function
#[cfg(feature = "warp-server")]
pub use web_server::{start_web_server, start_web_server_with_config};
