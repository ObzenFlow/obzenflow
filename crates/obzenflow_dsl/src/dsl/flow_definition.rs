// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use obzenflow_runtime::journal::RunSubstrateState;
use obzenflow_runtime::prelude::FlowHandle;

use super::FlowBuildError;

/// A failed flow build paired with the substrate state known at the failure
/// point (FLOWIP-120u F2).
///
/// The error is the only channel left when the build fails, so it carries the
/// run state the host still needs: a build that failed after substrate
/// selection has partial journals on disk worth naming in the failure footer.
#[derive(Debug)]
pub struct FlowBuildFailure {
    pub error: FlowBuildError,
    /// None: the build failed before substrate selection; no run directory exists.
    pub run: Option<RunSubstrateState>,
}

impl fmt::Display for FlowBuildFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for FlowBuildFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Pre-substrate failures carry no run state; the factory seam pairs the rest.
impl From<FlowBuildError> for FlowBuildFailure {
    fn from(error: FlowBuildError) -> Self {
        Self { error, run: None }
    }
}

/// A declarative flow definition produced by the `flow!` macro.
///
/// This is intentionally a distinct type (not a generic `Future`) so application runners
/// can be opinionated about accepting flows from the DSL rather than arbitrary async code.
pub struct FlowDefinition {
    inner: Pin<Box<dyn Future<Output = Result<FlowHandle, FlowBuildFailure>> + Send + 'static>>,
}

impl FlowDefinition {
    #[doc(hidden)]
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = Result<FlowHandle, FlowBuildFailure>> + Send + 'static,
    {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl Future for FlowDefinition {
    type Output = Result<FlowHandle, FlowBuildFailure>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
