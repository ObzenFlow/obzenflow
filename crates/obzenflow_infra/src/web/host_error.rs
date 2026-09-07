// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Failures of the private managed host, never endpoint responses.

#[derive(Debug, thiserror::Error)]
pub(crate) enum ManagedWebHostError {
    #[error("Managed web host did not start: {message}")]
    StartupFailed {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("Managed web host did not bind {address}")]
    BindFailed {
        address: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("Failed to register endpoint at {path}: {message}")]
    EndpointRegistrationFailed { path: String, message: String },
    #[error("Managed web host preparation failed: {message}")]
    Implementation {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("Managed web host listener failed: {0}")]
    Accept(#[source] std::io::Error),
    #[error("Managed web host terminated before shutdown")]
    PrematureCompletion,
    #[error("Managed web host task failed: {0}")]
    Task(#[source] tokio::task::JoinError),
    #[error("Managed web host close deadline expired")]
    CloseTimeout,
}
