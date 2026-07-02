// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware adapter for Source handlers
//!
//! This module provides middleware capabilities for both FiniteSourceHandler
//! and InfiniteSourceHandler implementations.

use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase};
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

const DEFAULT_ASYNC_POLL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct ExponentialBackoff {
    base_ms: u64,
    max_ms: u64,
    next_ms: u64,
}

impl ExponentialBackoff {
    fn new(base: Duration, max: Duration) -> Self {
        let base_ms = base.as_millis().max(1) as u64;
        let max_ms = max.as_millis().max(base_ms as u128) as u64;
        Self {
            base_ms,
            max_ms,
            next_ms: base_ms,
        }
    }

    fn next_backoff(&mut self) -> Duration {
        let backoff_ms = self.next_ms.max(1).min(self.max_ms);
        let next = backoff_ms.saturating_mul(2).min(self.max_ms);
        self.next_ms = next.max(1);
        Duration::from_millis(backoff_ms)
    }

    fn reset(&mut self) {
        self.next_ms = self.base_ms;
    }
}

fn source_error_kind(err: &SourceError) -> ErrorKind {
    match err {
        SourceError::Timeout(_) => ErrorKind::Timeout,
        SourceError::Transport(_) => ErrorKind::Remote,
        SourceError::Deserialization(_) => ErrorKind::Deserialization,
        SourceError::Other(_) => ErrorKind::Unknown,
    }
}

fn source_error_event(
    writer_id: WriterId,
    source_type: &'static str,
    err: &SourceError,
) -> ChainEvent {
    let kind = source_error_kind(err);
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    ChainEventFactory::observability_event(
        writer_id,
        ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
            name: "source.poll_error".to_string(),
            value: json!({
                "source_type": source_type,
                "error_type": format!("{err:?}").split('(').next().unwrap_or("unknown"),
                "message": err.to_string(),
                "timestamp_ms": timestamp_ms,
            }),
            tags: None,
        }),
    )
    .mark_as_error(err.to_string(), kind)
}

/// An AsyncFiniteSourceHandler wrapper that applies middleware.
#[derive(Clone)]
pub struct MiddlewareAsyncFiniteSource<H: AsyncFiniteSourceHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    writer_id: WriterId, // Sources need a writer ID for synthetic events
    poll_timeout: Option<Duration>,
    empty_poll_backoff: ExponentialBackoff,
}

impl<H: AsyncFiniteSourceHandler> std::fmt::Debug for MiddlewareAsyncFiniteSource<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareAsyncFiniteSource")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .field("writer_id", &self.writer_id)
            .field("poll_timeout", &self.poll_timeout)
            .finish()
    }
}

impl<H: AsyncFiniteSourceHandler> MiddlewareAsyncFiniteSource<H> {
    /// Create a new middleware-wrapped async finite source handler.
    pub fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            writer_id,
            poll_timeout: Some(DEFAULT_ASYNC_POLL_TIMEOUT),
            empty_poll_backoff: ExponentialBackoff::new(
                Duration::from_millis(1),
                Duration::from_millis(50),
            ),
        }
    }

    /// Override the poll timeout used to bound `inner.next().await`.
    ///
    /// - `Some(d)` enforces a timeout
    /// - `None` disables the timeout (handler is responsible)
    pub fn with_poll_timeout(mut self, poll_timeout: Option<Duration>) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }
}

#[async_trait]
impl<H: AsyncFiniteSourceHandler> AsyncFiniteSourceHandler for MiddlewareAsyncFiniteSource<H> {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
        self.inner.bind_writer_id(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let synthetic_event = ChainEventFactory::data_event(
            self.writer_id,
            "system.source.next",
            json!({
                "source_type": "async_finite",
                "timestamp_ms": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            }),
        );

        let mut ctx = MiddlewareContext::live_handler();

        // Poll the inner source with an optional timeout.
        let inner_result = match self.poll_timeout {
            Some(poll_timeout) => match timeout(poll_timeout, self.inner.next()).await {
                Ok(result) => result,
                Err(_elapsed) => Err(SourceError::Timeout(format!(
                    "poll timeout exceeded ({}s)",
                    poll_timeout.as_secs()
                ))),
            },
            None => self.inner.next().await,
        };

        // Completion short-circuit: do not run handler middleware on the final completion poll.
        if matches!(inner_result, Ok(None)) {
            return Ok(None);
        }

        // Handler middleware remains ordinary-only. Source policies are composed
        // by the adapter-owned source boundary outside this wrapper.
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }

            let action = middleware.pre_handle(&synthetic_event, &mut ctx);
            if let Some(message) =
                crate::middleware::observation_short_circuit(middleware.as_ref(), &action)
            {
                return Err(SourceError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { mut results, .. } => {
                    // Pre-write enrichment for skip results
                    for result in &mut results {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(result, &ctx);
                        }
                    }

                    // Append control events
                    let mut control_events = ctx.take_control_events();
                    for control_event in &mut control_events {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return Ok(Some(results));
                }
                MiddlewareAction::Abort { .. } => return Ok(Some(Vec::new())),
            }
        }

        let mut results = match inner_result {
            Ok(Some(events)) => events,
            Ok(None) => unreachable!("handled in completion short-circuit above"),
            Err(err) => vec![source_error_event(self.writer_id, "async_finite", &err)],
        };

        // Post-processing phase (observation)
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }
            middleware.post_handle(&synthetic_event, &results, &mut ctx);
        }

        // Pre-write phase: enrich each result event
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                middleware.pre_write(result, &ctx);
            }
        }

        // Append control events after all middleware runs
        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for mw in self.middleware_chain.iter() {
                if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                mw.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        // Empty-poll backoff to prevent hot loops when no data is produced.
        if !results.iter().any(|e| e.is_data()) {
            let backoff = self.empty_poll_backoff.next_backoff();
            tokio::time::sleep(backoff).await;
        } else {
            self.empty_poll_backoff.reset();
        }

        Ok(Some(results))
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.inner.drain().await
    }
}

/// An AsyncInfiniteSourceHandler wrapper that applies middleware.
#[derive(Clone)]
pub struct MiddlewareAsyncInfiniteSource<H: AsyncInfiniteSourceHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    writer_id: WriterId, // Sources need a writer ID for synthetic events
    poll_timeout: Option<Duration>,
    empty_poll_backoff: ExponentialBackoff,
}

impl<H: AsyncInfiniteSourceHandler> std::fmt::Debug for MiddlewareAsyncInfiniteSource<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareAsyncInfiniteSource")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .field("writer_id", &self.writer_id)
            .field("poll_timeout", &self.poll_timeout)
            .finish()
    }
}

impl<H: AsyncInfiniteSourceHandler> MiddlewareAsyncInfiniteSource<H> {
    /// Create a new middleware-wrapped async infinite source handler.
    pub fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            writer_id,
            // Infinite sources should block efficiently by default (e.g. recv().await),
            // so do not enforce a timeout unless the user opts in.
            poll_timeout: None,
            empty_poll_backoff: ExponentialBackoff::new(
                Duration::from_millis(1),
                Duration::from_millis(50),
            ),
        }
    }

    /// Override the poll timeout used to bound `inner.next().await`.
    ///
    /// - `Some(d)` enforces a timeout
    /// - `None` disables the timeout (handler is responsible)
    pub fn with_poll_timeout(mut self, poll_timeout: Option<Duration>) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }
}

#[async_trait]
impl<H: AsyncInfiniteSourceHandler> AsyncInfiniteSourceHandler
    for MiddlewareAsyncInfiniteSource<H>
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
        self.inner.bind_writer_id(id);
    }

    // Forwarded so the supervisor's resume-live flip reaches the hosted
    // surface; the trait default is None and would swallow it (FLOWIP-120n F12).
    fn hosted_ingress_slot(&self) -> Option<obzenflow_core::ingress::HostedIngressBindingSlot> {
        self.inner.hosted_ingress_slot()
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let synthetic_event = ChainEventFactory::data_event(
            self.writer_id,
            "system.source.next",
            json!({
                "source_type": "async_infinite",
                "timestamp_ms": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            }),
        );

        let mut ctx = MiddlewareContext::live_handler();

        // Poll the inner source with an optional timeout.
        let inner_result = match self.poll_timeout {
            Some(poll_timeout) => match timeout(poll_timeout, self.inner.next()).await {
                Ok(result) => result,
                Err(_elapsed) => Err(SourceError::Timeout(format!(
                    "poll timeout exceeded ({}s)",
                    poll_timeout.as_secs()
                ))),
            },
            None => self.inner.next().await,
        };

        // Handler middleware remains ordinary-only. Source policies are composed
        // by the adapter-owned source boundary outside this wrapper.
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }

            let action = middleware.pre_handle(&synthetic_event, &mut ctx);
            if let Some(message) =
                crate::middleware::observation_short_circuit(middleware.as_ref(), &action)
            {
                return Err(SourceError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { mut results, .. } => {
                    // Pre-write enrichment for skip results
                    for result in &mut results {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(result, &ctx);
                        }
                    }

                    // Append control events
                    let mut control_events = ctx.take_control_events();
                    for control_event in &mut control_events {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return Ok(results);
                }
                MiddlewareAction::Abort { .. } => return Ok(Vec::new()),
            }
        }

        let mut results = match inner_result {
            Ok(events) => events,
            Err(err) => vec![source_error_event(self.writer_id, "async_infinite", &err)],
        };

        // Post-processing phase (observation)
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }
            middleware.post_handle(&synthetic_event, &results, &mut ctx);
        }

        // Pre-write phase: enrich each result event
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                middleware.pre_write(result, &ctx);
            }
        }

        // Append control events after all middleware runs
        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for mw in self.middleware_chain.iter() {
                if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                mw.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        // Empty-poll backoff to prevent hot loops when no data is produced.
        if !results.iter().any(|e| e.is_data()) {
            let backoff = self.empty_poll_backoff.next_backoff();
            tokio::time::sleep(backoff).await;
        } else {
            self.empty_poll_backoff.reset();
        }

        Ok(results)
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.inner.drain().await
    }
}

/// A FiniteSourceHandler wrapper that applies middleware
#[derive(Clone)]
pub struct MiddlewareFiniteSource<H: FiniteSourceHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    writer_id: WriterId, // Sources need a writer ID for synthetic events
}

impl<H: FiniteSourceHandler> std::fmt::Debug for MiddlewareFiniteSource<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareFiniteSource")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<H: FiniteSourceHandler> MiddlewareFiniteSource<H> {
    /// Create a new middleware-wrapped finite source handler
    pub fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            writer_id,
        }
    }

    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }
}

impl<H: FiniteSourceHandler> FiniteSourceHandler for MiddlewareFiniteSource<H> {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
        self.inner.bind_writer_id(id);
    }

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        // Create a synthetic event for middleware to process.
        //
        // Important: finite sources may complete via `Ok(None)`. For that final completion
        // poll, we intentionally avoid running non-CB middleware (especially rate limiting)
        // so we don't delay shutdown or skew counters (see metrics exporter integration tests).
        let synthetic_event = ChainEventFactory::data_event(
            self.writer_id,
            "system.source.next",
            json!({
                "source_type": "finite",
                "timestamp_ms": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            }),
        );

        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::live_handler();

        // Poll the inner source.
        let inner_result = self.inner.next();

        // Completion short-circuit: no handler middleware on the final poll.
        if matches!(inner_result, Ok(None)) {
            return Ok(None);
        }

        // Handler middleware remains ordinary-only. Source policies are composed
        // by the adapter-owned source boundary outside this wrapper.
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }
            let action = middleware.pre_handle(&synthetic_event, &mut ctx);
            if let Some(message) =
                crate::middleware::observation_short_circuit(middleware.as_ref(), &action)
            {
                return Err(SourceError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { mut results, .. } => {
                    for result in &mut results {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(result, &ctx);
                        }
                    }

                    let mut control_events = ctx.take_control_events();
                    for control_event in &mut control_events {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return Ok(Some(results));
                }
                MiddlewareAction::Abort { .. } => return Ok(Some(Vec::new())),
            }
        }

        let mut results = match inner_result {
            Ok(Some(events)) => events,
            Ok(None) => unreachable!("handled above"),
            Err(err) => vec![source_error_event(self.writer_id, "finite", &err)],
        };

        // Post-processing phase (observation)
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }
            middleware.post_handle(&synthetic_event, &results, &mut ctx);
        }

        // Pre-write phase: enrich each result event
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                middleware.pre_write(result, &ctx);
            }
        }

        // Append control events after all middleware runs
        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        Ok(Some(results))
    }
}

/// An InfiniteSourceHandler wrapper that applies middleware
#[derive(Clone)]
pub struct MiddlewareInfiniteSource<H: InfiniteSourceHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    writer_id: WriterId,
}

impl<H: InfiniteSourceHandler> std::fmt::Debug for MiddlewareInfiniteSource<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareInfiniteSource")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .field("writer_id", &self.writer_id)
            .finish()
    }
}

impl<H: InfiniteSourceHandler> MiddlewareInfiniteSource<H> {
    /// Create a new middleware-wrapped infinite source handler
    pub fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            writer_id,
        }
    }

    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }
}

impl<H: InfiniteSourceHandler> InfiniteSourceHandler for MiddlewareInfiniteSource<H> {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
        self.inner.bind_writer_id(id);
    }

    fn next(
        &mut self,
    ) -> Result<
        Vec<ChainEvent>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        // Create a synthetic event for middleware to process
        let synthetic_event = ChainEventFactory::data_event(
            self.writer_id,
            "system.source.next",
            json!({
                "source_type": "infinite",
                "timestamp_ms": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            }),
        );

        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::live_handler();

        // Get next batch from inner source
        let inner_result = self.inner.next();

        // Handler middleware remains ordinary-only. Source policies are composed
        // by the adapter-owned source boundary outside this wrapper.
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }

            let action = middleware.pre_handle(&synthetic_event, &mut ctx);
            if let Some(message) =
                crate::middleware::observation_short_circuit(middleware.as_ref(), &action)
            {
                return Err(SourceError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { mut results, .. } => {
                    // Pre-write phase for skip results
                    for result in &mut results {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(result, &ctx);
                        }
                    }

                    // Append any control events emitted during skip
                    let mut control_events = ctx.take_control_events();
                    for control_event in &mut control_events {
                        for mw in self.middleware_chain.iter() {
                            if mw.source_phase() != SourceMiddlewarePhase::Ordinary {
                                continue;
                            }
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return Ok(results);
                }
                MiddlewareAction::Abort { .. } => return Ok(Vec::new()),
            }
        }

        let mut results = match inner_result {
            Ok(events) => events,
            Err(err) => vec![source_error_event(self.writer_id, "infinite", &err)],
        };

        // Post-processing phase (observation)
        for middleware in self.middleware_chain.iter() {
            if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                continue;
            }
            middleware.post_handle(&synthetic_event, &results, &mut ctx);
        }

        // Pre-write phase: enrich each result event
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                middleware.pre_write(result, &ctx);
            }
        }

        // Append control events after all middleware runs
        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                if middleware.source_phase() != SourceMiddlewarePhase::Ordinary {
                    continue;
                }
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        Ok(results)
    }
}

/// Extension trait for finite sources
pub trait FiniteSourceHandlerExt: FiniteSourceHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self, writer_id: WriterId) -> FiniteSourceMiddlewareBuilder<Self> {
        FiniteSourceMiddlewareBuilder::new(self, writer_id)
    }
}

impl<T: FiniteSourceHandler> FiniteSourceHandlerExt for T {}

/// Extension trait for async finite sources.
pub trait AsyncFiniteSourceHandlerExt: AsyncFiniteSourceHandler + Sized {
    /// Start building a middleware chain for this handler.
    fn middleware(self, writer_id: WriterId) -> AsyncFiniteSourceMiddlewareBuilder<Self> {
        AsyncFiniteSourceMiddlewareBuilder::new(self, writer_id)
    }
}

impl<T: AsyncFiniteSourceHandler> AsyncFiniteSourceHandlerExt for T {}

/// Extension trait for async infinite sources.
pub trait AsyncInfiniteSourceHandlerExt: AsyncInfiniteSourceHandler + Sized {
    /// Start building a middleware chain for this handler.
    fn middleware(self, writer_id: WriterId) -> AsyncInfiniteSourceMiddlewareBuilder<Self> {
        AsyncInfiniteSourceMiddlewareBuilder::new(self, writer_id)
    }
}

impl<T: AsyncInfiniteSourceHandler> AsyncInfiniteSourceHandlerExt for T {}

/// Extension trait for infinite sources
pub trait InfiniteSourceHandlerExt: InfiniteSourceHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self, writer_id: WriterId) -> InfiniteSourceMiddlewareBuilder<Self> {
        InfiniteSourceMiddlewareBuilder::new(self, writer_id)
    }
}

impl<T: InfiniteSourceHandler> InfiniteSourceHandlerExt for T {}

/// Builder for finite source middleware chains
pub struct FiniteSourceMiddlewareBuilder<H: FiniteSourceHandler> {
    handler: MiddlewareFiniteSource<H>,
}

impl<H: FiniteSourceHandler> FiniteSourceMiddlewareBuilder<H> {
    fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            handler: MiddlewareFiniteSource::new(inner, writer_id),
        }
    }

    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Add an already-boxed middleware without re-boxing.
    ///
    /// FLOWIP-114o: the generic `with` boxes its argument, so passing a
    /// `Box<dyn Middleware>` through it double-boxes and the chain element
    /// dispatches through the `Box` blanket impl. This preserves the concrete
    /// vtable for boxed middleware supplied by the DSL.
    pub fn with_boxed(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.handler = self.handler.with_middleware(middleware);
        self
    }

    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareFiniteSource<H> {
        self.handler
    }
}

/// Builder for async finite source middleware chains.
pub struct AsyncFiniteSourceMiddlewareBuilder<H: AsyncFiniteSourceHandler> {
    handler: MiddlewareAsyncFiniteSource<H>,
}

impl<H: AsyncFiniteSourceHandler> AsyncFiniteSourceMiddlewareBuilder<H> {
    fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            handler: MiddlewareAsyncFiniteSource::new(inner, writer_id),
        }
    }

    pub fn with_poll_timeout(mut self, poll_timeout: Option<Duration>) -> Self {
        self.handler = self.handler.with_poll_timeout(poll_timeout);
        self
    }

    /// Add a middleware to the chain.
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Add an already-boxed middleware without re-boxing (FLOWIP-115a; see
    /// `FiniteSourceMiddlewareBuilder::with_boxed`).
    pub fn with_boxed(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.handler = self.handler.with_middleware(middleware);
        self
    }

    /// Build the final middleware-wrapped handler.
    pub fn build(self) -> MiddlewareAsyncFiniteSource<H> {
        self.handler
    }
}

/// Builder for async infinite source middleware chains.
pub struct AsyncInfiniteSourceMiddlewareBuilder<H: AsyncInfiniteSourceHandler> {
    handler: MiddlewareAsyncInfiniteSource<H>,
}

impl<H: AsyncInfiniteSourceHandler> AsyncInfiniteSourceMiddlewareBuilder<H> {
    fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            handler: MiddlewareAsyncInfiniteSource::new(inner, writer_id),
        }
    }

    pub fn with_poll_timeout(mut self, poll_timeout: Option<Duration>) -> Self {
        self.handler = self.handler.with_poll_timeout(poll_timeout);
        self
    }

    /// Add a middleware to the chain.
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Add an already-boxed middleware without re-boxing (FLOWIP-115a; see
    /// `FiniteSourceMiddlewareBuilder::with_boxed`).
    pub fn with_boxed(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.handler = self.handler.with_middleware(middleware);
        self
    }

    /// Build the final middleware-wrapped handler.
    pub fn build(self) -> MiddlewareAsyncInfiniteSource<H> {
        self.handler
    }
}

/// Builder for infinite source middleware chains
pub struct InfiniteSourceMiddlewareBuilder<H: InfiniteSourceHandler> {
    handler: MiddlewareInfiniteSource<H>,
}

impl<H: InfiniteSourceHandler> InfiniteSourceMiddlewareBuilder<H> {
    fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            handler: MiddlewareInfiniteSource::new(inner, writer_id),
        }
    }

    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Add an already-boxed middleware without re-boxing (FLOWIP-115a; see
    /// `FiniteSourceMiddlewareBuilder::with_boxed`).
    pub fn with_boxed(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.handler = self.handler.with_middleware(middleware);
        self
    }

    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareInfiniteSource<H> {
        self.handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::circuit_breaker::CircuitBreakerMiddleware;
    use async_trait::async_trait;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::status::processing_status::ProcessingStatus;
    use obzenflow_core::StageId;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    struct ErringFiniteSource {
        calls: Arc<AtomicUsize>,
    }

    impl FiniteSourceHandler for ErringFiniteSource {
        fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(SourceError::Timeout("boom".to_string()))
        }
    }

    #[test]
    fn finite_source_policy_middleware_is_not_driven_by_handler_wrapper() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = ErringFiniteSource {
            calls: calls.clone(),
        };

        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);

        // Policy middleware is intentionally inert in the handler wrapper. Source
        // policies are driven by PerSourcePolicyBoundary instead.
        let cb = CircuitBreakerMiddleware::with_cooldown(1, Duration::from_millis(50));

        let mut wrapped =
            MiddlewareFiniteSource::new(inner, writer_id).with_middleware(Box::new(cb));

        let first = wrapped
            .next()
            .expect("finite source wrapper should not propagate SourceError");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let results = first.expect("first call should return error event(s)");
        let error_events: Vec<&ChainEvent> = results
            .iter()
            .filter(|e| matches!(e.processing_info.status, ProcessingStatus::Error { .. }))
            .collect();
        assert_eq!(
            error_events.len(),
            1,
            "expected exactly one source error event"
        );
        assert!(
            error_events[0].is_lifecycle(),
            "source errors should be lifecycle events (not data)"
        );

        match &error_events[0].processing_info.status {
            ProcessingStatus::Error { kind, .. } => {
                assert_eq!(
                    kind.clone().unwrap_or(ErrorKind::Unknown),
                    ErrorKind::Timeout
                );
            }
            _ => unreachable!("filtered to Error events"),
        }

        // A policy inserted directly into the wrapper does not gate the poll.
        let _ = wrapped
            .next()
            .expect("finite source wrapper should not propagate SourceError");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "source policy middleware must not be driven by the handler wrapper"
        );
    }

    #[derive(Clone, Debug)]
    struct ErringInfiniteSource {
        calls: Arc<AtomicUsize>,
    }

    impl InfiniteSourceHandler for ErringInfiniteSource {
        fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(SourceError::Transport("boom".to_string()))
        }
    }

    #[test]
    fn infinite_source_policy_middleware_is_not_driven_by_handler_wrapper() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = ErringInfiniteSource {
            calls: calls.clone(),
        };

        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);

        // Policy middleware is intentionally inert in the handler wrapper. Source
        // policies are driven by PerSourcePolicyBoundary instead.
        let cb = CircuitBreakerMiddleware::with_cooldown(1, Duration::from_millis(50));

        let mut wrapped =
            MiddlewareInfiniteSource::new(inner, writer_id).with_middleware(Box::new(cb));

        let first = wrapped
            .next()
            .expect("infinite source wrapper should not propagate SourceError");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            first
                .iter()
                .any(|e| matches!(e.processing_info.status, ProcessingStatus::Error { .. })),
            "expected an error-marked event"
        );

        // A policy inserted directly into the wrapper does not gate the poll.
        let _ = wrapped
            .next()
            .expect("infinite source wrapper should not propagate SourceError");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "source policy middleware must not be driven by the handler wrapper"
        );
    }

    #[derive(Clone, Debug)]
    struct ErringAsyncFiniteSource {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AsyncFiniteSourceHandler for ErringAsyncFiniteSource {
        async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(SourceError::Timeout("boom".to_string()))
        }
    }

    #[tokio::test]
    async fn async_finite_source_policy_middleware_is_not_driven_by_handler_wrapper() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = ErringAsyncFiniteSource {
            calls: calls.clone(),
        };

        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);

        // Policy middleware is intentionally inert in the handler wrapper. Source
        // policies are driven by PerSourcePolicyBoundary instead.
        let cb = CircuitBreakerMiddleware::with_cooldown(1, Duration::from_millis(50));

        let mut wrapped =
            MiddlewareAsyncFiniteSource::new(inner, writer_id).with_middleware(Box::new(cb));

        let first = wrapped
            .next()
            .await
            .expect("async finite source wrapper should not propagate SourceError");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let results = first.expect("first call should return error event(s)");
        assert!(
            results
                .iter()
                .any(|e| matches!(e.processing_info.status, ProcessingStatus::Error { .. })),
            "expected an error-marked event"
        );

        // A policy inserted directly into the wrapper does not gate the poll.
        let _ = wrapped
            .next()
            .await
            .expect("async finite source wrapper should not propagate SourceError");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "source policy middleware must not be driven by the handler wrapper"
        );
    }

    #[derive(Clone, Debug)]
    struct SingleEventAsyncFiniteSource {
        calls: Arc<AtomicUsize>,
        writer_id: WriterId,
    }

    #[async_trait]
    impl AsyncFiniteSourceHandler for SingleEventAsyncFiniteSource {
        fn bind_writer_id(&mut self, id: WriterId) {
            self.writer_id = id;
        }

        async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Ok(Some(vec![ChainEventFactory::data_event(
                    self.writer_id,
                    "test.event",
                    json!({ "call": call }),
                )]));
            }

            Ok(None)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn async_finite_source_completion_short_circuits_handler_middleware() {
        let calls = Arc::new(AtomicUsize::new(0));
        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);

        let inner = SingleEventAsyncFiniteSource {
            calls: calls.clone(),
            writer_id,
        };

        let mut wrapped = MiddlewareAsyncFiniteSource::new(inner, writer_id);

        let first = wrapped
            .next()
            .await
            .expect("first async finite source poll should succeed");
        assert!(
            first
                .as_ref()
                .is_some_and(|events| events.iter().any(ChainEvent::is_data)),
            "first poll should produce a data event"
        );

        let second = tokio::time::timeout(Duration::from_secs(1), wrapped.next())
            .await
            .expect("completion poll should not hang")
            .expect("completion poll should not propagate SourceError");
        assert!(
            second.is_none(),
            "completion poll should return None without running handler middleware"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "expected exactly one data poll followed by one completion poll"
        );
    }

    #[derive(Clone, Debug)]
    struct ErringAsyncInfiniteSource {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AsyncInfiniteSourceHandler for ErringAsyncInfiniteSource {
        async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(SourceError::Timeout("boom".to_string()))
        }
    }

    #[tokio::test]
    async fn async_infinite_source_policy_middleware_is_not_driven_by_handler_wrapper() {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = ErringAsyncInfiniteSource {
            calls: calls.clone(),
        };

        let stage_id = StageId::new();
        let writer_id = WriterId::from(stage_id);

        // Policy middleware is intentionally inert in the handler wrapper. Source
        // policies are driven by PerSourcePolicyBoundary instead.
        let cb = CircuitBreakerMiddleware::with_cooldown(1, Duration::from_millis(50));

        let mut wrapped =
            MiddlewareAsyncInfiniteSource::new(inner, writer_id).with_middleware(Box::new(cb));

        let first = wrapped
            .next()
            .await
            .expect("async infinite source wrapper should not propagate SourceError");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            first
                .iter()
                .any(|e| matches!(e.processing_info.status, ProcessingStatus::Error { .. })),
            "expected an error-marked event"
        );

        // A policy inserted directly into the wrapper does not gate the poll.
        let _ = wrapped
            .next()
            .await
            .expect("async infinite source wrapper should not propagate SourceError");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "source policy middleware must not be driven by the handler wrapper"
        );
    }
}
