// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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

    /// Replay verification finished with a non-zero verdict (FLOWIP-095j):
    /// 1 divergence in the certified region, 2 certified region matched with
    /// uncertified stages, 3 refused or skipped.
    Verification { exit_code: u8, summary: String },

    /// Other error
    Other(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FlowBuildFailed(msg) => write!(f, "Flow build failed: {msg}"),
            Self::FlowExecutionFailed(msg) => write!(f, "Flow execution failed: {msg}"),
            Self::ServerStartFailed(msg) => write!(f, "Server start failed: {msg}"),
            Self::RuntimeCreationFailed(msg) => write!(f, "Runtime creation failed: {msg}"),
            Self::FeatureNotEnabled(feature) => write!(f, "Feature not enabled: {feature}"),
            Self::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {msg}"),
            Self::IoError(err) => write!(f, "IO error: {err}"),
            Self::Verification { exit_code, summary } => {
                write!(f, "replay verification (exit code {exit_code}): {summary}")
            }
            Self::Other(err) => write!(f, "Application error: {err}"),
        }
    }
}

impl Error for ApplicationError {}

impl ApplicationError {
    /// Map to a process exit code. Verification verdicts carry their
    /// FLOWIP-095j contract code (1 divergence, 2 uncertified remainder,
    /// 3 refused/skipped); every other error is 1.
    pub fn process_exit_code(&self) -> u8 {
        match self {
            Self::Verification { exit_code, .. } => *exit_code,
            _ => 1,
        }
    }
}

impl From<std::io::Error> for ApplicationError {
    fn from(err: std::io::Error) -> Self {
        ApplicationError::IoError(err)
    }
}
