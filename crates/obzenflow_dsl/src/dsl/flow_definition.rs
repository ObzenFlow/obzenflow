// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use obzenflow_runtime::journal::RunSubstrateState;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::run_context::FlowBuildContext;

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

type BuildFuture =
    Pin<Box<dyn Future<Output = Result<FlowHandle, FlowBuildFailure>> + Send + 'static>>;

/// A declarative flow definition produced by the `flow!` macro.
///
/// This is intentionally a distinct type (not a generic `Future`) so application runners
/// can be opinionated about accepting flows from the DSL rather than arbitrary async code.
///
/// FLOWIP-010 §7: the build is deferred until a [`FlowBuildContext`] is
/// supplied, so a flow cannot be built without the resolved config snapshot.
/// Hosts call [`FlowDefinition::build`]; tests without a host use
/// `FlowBuildContext::for_tests()`.
pub struct FlowDefinition {
    build: Box<dyn FnOnce(FlowBuildContext) -> BuildFuture + Send + 'static>,
}

impl FlowDefinition {
    #[doc(hidden)]
    pub fn new<F, Fut>(build: F) -> Self
    where
        F: FnOnce(FlowBuildContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<FlowHandle, FlowBuildFailure>> + Send + 'static,
    {
        Self {
            build: Box::new(move |ctx| Box::pin(build(ctx))),
        }
    }

    /// Build the flow against an explicit per-run context.
    pub fn build(self, ctx: FlowBuildContext) -> BuildFuture {
        (self.build)(ctx)
    }
}
