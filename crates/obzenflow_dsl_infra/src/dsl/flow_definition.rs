use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use obzenflow_runtime_services::prelude::FlowHandle;

use super::FlowBuildError;

/// A declarative flow definition produced by the `flow!` macro.
///
/// This is intentionally a distinct type (not a generic `Future`) so application runners
/// can be opinionated about accepting flows from the DSL rather than arbitrary async code.
pub struct FlowDefinition {
    inner: Pin<Box<dyn Future<Output = Result<FlowHandle, FlowBuildError>> + Send + 'static>>,
}

impl FlowDefinition {
    #[doc(hidden)]
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = Result<FlowHandle, FlowBuildError>> + Send + 'static,
    {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl Future for FlowDefinition {
    type Output = Result<FlowHandle, FlowBuildError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

