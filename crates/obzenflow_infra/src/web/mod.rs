//! Web server implementations
//!
//! This module provides concrete implementations of the core WebServer trait
//! for different web frameworks.

#[cfg(feature = "warp-server")]
pub mod warp;

#[cfg(feature = "axum-server")]
pub mod axum;

pub mod factory;
pub mod metrics_server;

// Re-export implementations when features are enabled
#[cfg(feature = "warp-server")]
pub use self::warp::WarpServer;

#[cfg(feature = "axum-server")]
pub use self::axum::AxumServer;

// Re-export factory functions
pub use factory::*;

#[cfg(feature = "warp-server")]
pub use metrics_server::start_metrics_server;