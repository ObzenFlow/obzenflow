use std::error::Error;
use std::fmt;

/// Errors that can occur in the FlowApplication framework
#[derive(Debug)]
pub enum ApplicationError {
    /// Flow failed to build
    FlowBuildFailed(String),

    /// Flow execution failed
    FlowExecutionFailed(String),

    /// Server failed to start
    ServerStartFailed(String),

    /// Runtime creation failed
    RuntimeCreationFailed(String),

    /// Feature not enabled
    FeatureNotEnabled(String),

    /// Invalid runtime configuration
    InvalidConfiguration(String),

    /// IO error
    IoError(std::io::Error),

    /// Other error
    Other(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FlowBuildFailed(msg) => write!(f, "Flow build failed: {}", msg),
            Self::FlowExecutionFailed(msg) => write!(f, "Flow execution failed: {}", msg),
            Self::ServerStartFailed(msg) => write!(f, "Server start failed: {}", msg),
            Self::RuntimeCreationFailed(msg) => write!(f, "Runtime creation failed: {}", msg),
            Self::FeatureNotEnabled(feature) => write!(f, "Feature not enabled: {}", feature),
            Self::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {}", msg),
            Self::IoError(err) => write!(f, "IO error: {}", err),
            Self::Other(err) => write!(f, "Application error: {}", err),
        }
    }
}

impl Error for ApplicationError {}

impl From<std::io::Error> for ApplicationError {
    fn from(err: std::io::Error) -> Self {
        ApplicationError::IoError(err)
    }
}
