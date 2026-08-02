// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use obzenflow_runtime::journal::RunSubstrateState;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::ResolvedRuntimeConfig;

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

/// A declarative flow definition produced by the `flow!` macro or by wrapping
/// one in [`FlowDefinition::materialize`].
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

    /// Defer flow materialisation until the host has resolved the run's
    /// immutable runtime configuration snapshot.
    ///
    /// The factory runs exactly once from inside [`Self::build`]. It receives
    /// the same snapshot carried by the [`FlowBuildContext`] used to build the
    /// returned definition. Configuration-derived contracts, handlers,
    /// sources, sinks, middleware, roles, and effect-port registries belong in
    /// this factory rather than in the calling host:
    ///
    /// ```rust,ignore
    /// FlowDefinition::materialize(move |runtime_config| {
    ///     let binding = build_binding(runtime_config)?;
    ///     let handler = build_handler(&binding)?;
    ///
    ///     Ok(flow! {
    ///         // `binding` and `handler` are in ordinary Rust scope here.
    ///     })
    /// })
    /// ```
    ///
    /// Owned application inputs and explicitly injected dependencies may be
    /// captured by the factory. Ephemeral host-resource guards should remain
    /// in the calling future across the application run.
    pub fn materialize<F>(factory: F) -> Self
    where
        F: FnOnce(&ResolvedRuntimeConfig) -> Result<FlowDefinition, FlowBuildError>
            + Send
            + 'static,
    {
        Self::new(move |ctx| async move {
            let flow = factory(ctx.runtime_config().as_ref()).map_err(FlowBuildFailure::from)?;
            flow.build(ctx).await
        })
    }

    /// Build the flow against an explicit per-run context.
    pub fn build(self, ctx: FlowBuildContext) -> BuildFuture {
        (self.build)(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use obzenflow_runtime::run_context::FlowBuildContext;
    use obzenflow_runtime::runtime_config::ResolvedRuntimeConfig;

    use super::{FlowBuildFailure, FlowDefinition};
    use crate::dsl::FlowBuildError;

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    #[test]
    fn materialize_factory_stays_cold_until_the_build_future_is_polled() {
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&factory_calls);

        let flow = FlowDefinition::materialize(move |_| {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Err(FlowBuildError::StageResourcesFailed(
                "first-poll sentinel".to_string(),
            ))
        });

        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);

        let mut build = flow.build(FlowBuildContext::for_tests());
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);

        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let result = Future::poll(build.as_mut(), &mut context);

        assert!(matches!(result, Poll::Ready(Err(_))));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropping_an_unbuilt_definition_or_unpolled_build_future_does_not_run_the_factory() {
        let definition_calls = Arc::new(AtomicUsize::new(0));
        let observed_definition_calls = Arc::clone(&definition_calls);
        let unbuilt = FlowDefinition::materialize(move |_| {
            observed_definition_calls.fetch_add(1, Ordering::SeqCst);
            Err(FlowBuildError::StageResourcesFailed(
                "unbuilt sentinel".to_string(),
            ))
        });
        drop(unbuilt);
        assert_eq!(definition_calls.load(Ordering::SeqCst), 0);

        let future_calls = Arc::new(AtomicUsize::new(0));
        let observed_future_calls = Arc::clone(&future_calls);
        let flow = FlowDefinition::materialize(move |_| {
            observed_future_calls.fetch_add(1, Ordering::SeqCst);
            Err(FlowBuildError::StageResourcesFailed(
                "unpolled sentinel".to_string(),
            ))
        });
        let build = flow.build(FlowBuildContext::for_tests());
        drop(build);
        assert_eq!(future_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn materialize_invokes_factory_once_and_forwards_the_same_snapshot() {
        let snapshot = Arc::new(ResolvedRuntimeConfig::builtin_defaults());
        let factory_snapshot = Arc::clone(&snapshot);
        let inner_snapshot = Arc::clone(&snapshot);
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&factory_calls);

        let flow = FlowDefinition::materialize(move |runtime_config| {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            assert!(ptr::eq(runtime_config, factory_snapshot.as_ref()));

            Ok(FlowDefinition::new(move |ctx| async move {
                assert!(Arc::ptr_eq(ctx.runtime_config(), &inner_snapshot));
                Err(FlowBuildFailure::from(
                    FlowBuildError::StageResourcesFailed("inner sentinel".to_string()),
                ))
            }))
        });

        let failure = match flow.build(FlowBuildContext::new(snapshot)).await {
            Ok(_) => panic!("the inner sentinel must end the focused build"),
            Err(failure) => failure,
        };

        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
        assert!(failure.run.is_none());
    }

    #[tokio::test]
    async fn materialize_maps_factory_errors_to_pre_substrate_failures() {
        let flow = FlowDefinition::materialize(|_| {
            Err(FlowBuildError::BindingConfiguration {
                binding: "chat".to_string(),
                detail: "missing model".to_string(),
            })
        });

        let failure = match flow.build(FlowBuildContext::for_tests()).await {
            Ok(_) => panic!("factory error must fail the build"),
            Err(failure) => failure,
        };

        assert!(failure.run.is_none());
        assert!(matches!(
            failure.error,
            FlowBuildError::BindingConfiguration { binding, detail }
                if binding == "chat" && detail == "missing model"
        ));
    }
}
