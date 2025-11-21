//! Web server error types

use std::fmt;

/// Errors that can occur in web server operations
#[derive(Debug)]
pub enum WebError {
    /// Server failed to start
    StartupFailed {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Server failed to bind to address
    BindFailed {
        address: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Endpoint registration failed
    EndpointRegistrationFailed { path: String, message: String },

    /// Request handling failed
    RequestHandlingFailed {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Server shutdown failed
    ShutdownFailed {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Generic implementation error
    Implementation {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl fmt::Display for WebError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebError::StartupFailed { message, .. } => {
                write!(f, "Server startup failed: {}", message)
            }
            WebError::BindFailed { address, .. } => {
                write!(f, "Failed to bind to address {}", address)
            }
            WebError::EndpointRegistrationFailed { path, message } => {
                write!(f, "Failed to register endpoint at {}: {}", path, message)
            }
            WebError::RequestHandlingFailed { message, .. } => {
                write!(f, "Request handling failed: {}", message)
            }
            WebError::ShutdownFailed { message, .. } => {
                write!(f, "Server shutdown failed: {}", message)
            }
            WebError::Implementation { message, .. } => {
                write!(f, "Implementation error: {}", message)
            }
        }
    }
}

impl std::error::Error for WebError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WebError::StartupFailed { source, .. }
            | WebError::BindFailed { source, .. }
            | WebError::RequestHandlingFailed { source, .. }
            | WebError::ShutdownFailed { source, .. }
            | WebError::Implementation { source, .. } => source
                .as_ref()
                .map(|s| s.as_ref() as &(dyn std::error::Error + 'static)),
            WebError::EndpointRegistrationFailed { .. } => None,
        }
    }
}
