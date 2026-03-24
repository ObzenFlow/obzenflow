// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use crate::dsl::typing::StageTypingMetadata;
use crate::stage_handle_adapter::StageHandleAdapter;
use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    validate_middleware_safety, AsyncFiniteSourceHandlerExt, AsyncInfiniteSourceHandlerExt,
    AsyncTransformHandlerExt, FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    JoinHandlerMiddlewareExt, Middleware, MiddlewareFactory, OutcomeEnrichmentMiddleware,
    SinkHandlerExt, StatefulHandlerMiddlewareExt, SystemEnrichmentMiddleware, TimingMiddleware,
    TransformHandlerExt,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::{
    metrics::instrumentation::{InstrumentationConfig, StageInstrumentation},
    stages::StageResources,
};
use obzenflow_runtime::{
    pipeline::config::StageConfig,
    stages::{
        common::{
            control_strategies::{CompositeStrategy, ControlEventStrategy, JonestownStrategy},
            handlers::{
                AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
                FiniteSourceHandler, InfiniteSourceHandler, JoinHandler, SinkHandler,
                StatefulHandler, TransformHandler,
            },
            stage_handle::{BoxedStageHandle, StageEvent, FORCE_SHUTDOWN_MESSAGE},
        },
        join::{JoinBuilder, JoinConfig, JoinEvent, JoinState},
        sink::journal_sink::{
            JournalSinkBuilder, JournalSinkConfig, JournalSinkEvent, JournalSinkState,
        },
        source::{
            finite::{
                AsyncFiniteSourceBuilder, FiniteSourceBuilder, FiniteSourceConfig,
                FiniteSourceEvent, FiniteSourceState,
            },
            infinite::{
                AsyncInfiniteSourceBuilder, InfiniteSourceBuilder, InfiniteSourceConfig,
                InfiniteSourceEvent, InfiniteSourceState,
            },
            strategies::CircuitBreakerSourceStrategy,
        },
        stateful::{StatefulBuilder, StatefulConfig, StatefulEvent, StatefulState},
        transform::{
            AsyncTransformBuilder, TransformBuilder, TransformConfig, TransformEvent,
            TransformState,
        },
    },
    supervised_base::SupervisorBuilder as SupervisorBuilderTrait,
};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Marker name used by stage macros when the runtime name should be derived from the enclosing
/// `flow!` binding.
///
/// This is intentionally a weird, non-user-facing value. `flow!` resolves it to the left-hand
/// binding before any uniqueness checks or topology build steps run.
#[doc(hidden)]
pub const BINDING_DERIVED_NAME_SENTINEL: &str = "__obzenflow_binding_derived_name__";

/// Create system middleware for a stage
fn create_system_middleware(
    config: &StageConfig,
    stage_type: StageType,
) -> Vec<Box<dyn Middleware>> {
    tracing::info!(
        "Creating system middleware for stage '{}' of type {:?}",
        config.name,
        stage_type
    );
    vec![
        Box::new(TimingMiddleware::new(&config.name)),
        Box::new(SystemEnrichmentMiddleware::new(
            config.flow_name.clone(),
            config.flow_name.clone(), // flow_id same as flow_name for now
            config.name.clone(),
            config.stage_id,
            stage_type,
        )),
        Box::new(OutcomeEnrichmentMiddleware::new(&config.name)),
    ]
}

/// Helper function to create a control strategy from resolved middleware.
fn create_control_strategy_from_middleware_specs(
    middleware: &[crate::middleware_resolution::MiddlewareSpec],
) -> Arc<dyn ControlEventStrategy> {
    let strategies: Vec<Box<dyn ControlEventStrategy>> = middleware
        .iter()
        .filter_map(|spec| spec.factory.create_control_strategy())
        .collect();

    match strategies.len() {
        0 => Arc::new(JonestownStrategy), // Default
        1 => {
            // Convert Box<dyn> to Arc<dyn> for single strategy
            let boxed = strategies.into_iter().next().unwrap();
            Arc::from(boxed)
        }
        _ => {
            // Multiple strategies - compose them
            Arc::new(CompositeStrategy::new(strategies))
        }
    }
}

/// Trait for stage descriptors that know how to create their supervisors
#[async_trait]
pub trait StageDescriptor: Send + Sync {
    /// Get the stage name
    fn name(&self) -> &str;

    /// Update the stage's runtime name.
    ///
    /// Implementations that carry an internal name field should override this.
    /// Default: no-op (for descriptors that do not own a mutable name).
    fn set_name(&mut self, _name: String) {
        // Default: no-op
    }

    /// Get the stage type
    fn stage_type(&self) -> StageType;

    /// Get the reference stage ID (only for join stages)
    /// Returns None for non-join stages
    fn reference_stage_id(&self) -> Option<StageId> {
        None
    }

    /// Get the reference stage name for DSL resolution (only for join stages)
    /// Returns None for non-join stages or programmatic join stages
    fn reference_stage_name(&self) -> Option<&str> {
        None
    }

    /// Set the reference stage ID (only for join stages, used by DSL)
    fn set_reference_stage_id(&mut self, _id: StageId) {
        // Default: no-op for non-join stages
    }

    /// Create the handle for this stage
    async fn create_handle(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
    ) -> Result<BoxedStageHandle, String> {
        // Default implementation without flow middleware
        self.create_handle_with_flow_middleware(
            config,
            resources,
            vec![],
            Arc::new(ControlMiddlewareAggregator::new()),
        )
        .await
    }

    /// Create the handle for this stage with flow-level middleware
    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String>;

    /// Structural: return configured stage-level middleware names (for topology)
    ///
    /// Default implementation returns an empty list; concrete descriptors that
    /// carry middleware should override this.
    fn stage_middleware_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Structural: return configured stage-level middleware factories (for topology config extraction)
    ///
    /// Default implementation returns an empty slice; concrete descriptors that
    /// carry middleware should override this to expose their factories for `config_snapshot()`.
    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &[]
    }

    /// Get a debug representation
    fn debug_info(&self) -> String {
        format!("Stage[{}]", self.name())
    }

    /// Optional types-first metadata captured by typed stage macros.
    fn typing_metadata(&self) -> Option<&StageTypingMetadata> {
        None
    }

    /// Whether this descriptor is a composite that must be lowered during `flow!` materialisation.
    ///
    /// Default: `false` for ordinary stages.
    fn is_composite(&self) -> bool {
        false
    }

    /// Lower a composite descriptor into concrete stages + edges.
    ///
    /// Default: returns `Ok(None)` for non-composite descriptors.
    #[allow(clippy::result_large_err)]
    fn try_lower_composite(
        self: Box<Self>,
        _binding: &str,
    ) -> Result<Option<crate::dsl::composites::CompositeLowering>, crate::dsl::FlowBuildError> {
        Ok(None)
    }
}

/// Descriptor for finite source stages
pub struct FiniteSourceDescriptor<H: FiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for FiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::from(config.stage_id);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(&config, StageType::FiniteSource);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware, tracking whether circuit_breaker is present.
        //
        // Note: circuit_breaker middleware MUST be attached for sources so it can:
        // - observe SourceError-derived error events
        // - trip/open and prevent hammering upstream dependencies
        // - export correct per-stage breaker metrics
        let mut has_circuit_breaker = false;
        let mut user_middleware: Vec<Box<dyn Middleware>> = Vec::new();
        for spec in resolved.middleware.into_iter() {
            if spec.factory.name() == "circuit_breaker" {
                has_circuit_breaker = true;
                user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
                continue;
            }
            user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
        }
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        // Apply all middleware
        let mut builder = handler.middleware(writer_id);
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let source_config = FiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: if has_circuit_breaker {
                Some(Arc::new(
                    CircuitBreakerSourceStrategy::try_new(config.stage_id, &control_provider)
                        .map_err(|e| e.to_string())?,
                ))
            } else {
                None
            },
        };

        // Use the builder to create the handle
        let handle = FiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build finite source: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::FiniteSource,
            translate_stage_event_to_finite_source,
            check_finite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for async finite source stages.
pub struct AsyncFiniteSourceDescriptor<H: AsyncFiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub poll_timeout: Option<Duration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncFiniteSourceDescriptor<H>
{
    /// Create a new async finite source descriptor with a default 30s poll timeout.
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        let poll_timeout = handler
            .suggested_poll_timeout()
            .or(Some(DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT));
        Self {
            name: name.into(),
            handler,
            poll_timeout,
            middleware: Vec::new(),
        }
    }

    /// Override poll timeout. `None` disables enforcement (handler manages its own).
    pub fn with_poll_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    /// Build into a boxed StageDescriptor for DSL compatibility.
    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: AsyncFiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for AsyncFiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::from(config.stage_id);
        let poll_timeout = self.poll_timeout;

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        let mut all_middleware = create_system_middleware(&config, StageType::FiniteSource);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        let mut has_circuit_breaker = false;
        let mut user_middleware: Vec<Box<dyn Middleware>> = Vec::new();
        for spec in resolved.middleware.into_iter() {
            if spec.factory.name() == "circuit_breaker" {
                has_circuit_breaker = true;
                user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
                continue;
            }
            user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
        }
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        // Apply all middleware.
        let mut builder = handler
            .middleware(writer_id)
            .with_poll_timeout(poll_timeout);
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let source_config = FiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: if has_circuit_breaker {
                Some(Arc::new(
                    CircuitBreakerSourceStrategy::try_new(config.stage_id, &control_provider)
                        .map_err(|e| e.to_string())?,
                ))
            } else {
                None
            },
        };

        // Use the builder to create the handle
        let handle =
            AsyncFiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
                .with_instrumentation(instrumentation)
                .build()
                .await
                .map_err(|e| format!("Failed to build async finite source: {e:?}"))?;

        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::FiniteSource,
            translate_stage_event_to_finite_source,
            check_finite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for infinite source stages
pub struct InfiniteSourceDescriptor<H: InfiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for InfiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::from(config.stage_id);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(&config, StageType::InfiniteSource);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware, tracking whether circuit_breaker is present.
        //
        // Note: circuit_breaker middleware MUST be attached for sources so it can:
        // - observe SourceError-derived error events
        // - trip/open and prevent hammering upstream dependencies
        // - export correct per-stage breaker metrics
        let mut has_circuit_breaker = false;
        let mut user_middleware: Vec<Box<dyn Middleware>> = Vec::new();
        for spec in resolved.middleware.into_iter() {
            if spec.factory.name() == "circuit_breaker" {
                has_circuit_breaker = true;
                user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
                continue;
            }
            user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
        }
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081d).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        // Apply all middleware
        let mut builder = handler.middleware(writer_id);
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let source_config = InfiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: if has_circuit_breaker {
                Some(Arc::new(
                    CircuitBreakerSourceStrategy::try_new(config.stage_id, &control_provider)
                        .map_err(|e| e.to_string())?,
                ))
            } else {
                None
            },
        };

        // Use the builder to create the handle
        let handle = InfiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build infinite source: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::InfiniteSource,
            translate_stage_event_to_infinite_source,
            check_infinite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for async infinite source stages.
pub struct AsyncInfiniteSourceDescriptor<H: AsyncInfiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub poll_timeout: Option<Duration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    AsyncInfiniteSourceDescriptor<H>
{
    /// Create a new async infinite source descriptor.
    ///
    /// Defaults to no poll timeout so push sources can block efficiently (e.g. `recv().await`).
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        let poll_timeout = handler.suggested_poll_timeout();
        Self {
            name: name.into(),
            handler,
            poll_timeout,
            middleware: Vec::new(),
        }
    }

    /// Override poll timeout. `None` disables enforcement (handler manages its own).
    pub fn with_poll_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    /// Build into a boxed StageDescriptor for DSL compatibility.
    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: AsyncInfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    StageDescriptor for AsyncInfiniteSourceDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::from(config.stage_id);
        let poll_timeout = self.poll_timeout;

        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);

        let mut all_middleware = create_system_middleware(&config, StageType::InfiniteSource);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        let mut has_circuit_breaker = false;
        let mut user_middleware: Vec<Box<dyn Middleware>> = Vec::new();
        for spec in resolved.middleware.into_iter() {
            if spec.factory.name() == "circuit_breaker" {
                has_circuit_breaker = true;
                user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
                continue;
            }
            user_middleware.push(spec.factory.create(&config, control_middleware.clone()));
        }
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Inject stage writer id into the handler before wrapping with middleware (FLOWIP-081d).
        let mut handler = self.handler;
        handler.bind_writer_id(writer_id);

        let mut builder = handler
            .middleware(writer_id)
            .with_poll_timeout(poll_timeout);
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        let source_config = InfiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: if has_circuit_breaker {
                Some(Arc::new(
                    CircuitBreakerSourceStrategy::try_new(config.stage_id, &control_provider)
                        .map_err(|e| e.to_string())?,
                ))
            } else {
                None
            },
        };

        let handle =
            AsyncInfiniteSourceBuilder::new(handler_with_middleware, source_config, resources)
                .with_instrumentation(instrumentation)
                .build()
                .await
                .map_err(|e| format!("Failed to build async infinite source: {e:?}"))?;

        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::InfiniteSource,
            translate_stage_event_to_infinite_source,
            check_infinite_source_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for transform stages
pub struct TransformDescriptor<H: TransformHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for TransformDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
        for factory in &self.middleware {
            // Validate safety
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Transform, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
                // Could choose to panic here for critical errors
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_control_strategy_from_middleware_specs(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(&config, StageType::Transform);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved
            .middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config, control_middleware.clone()))
            .collect();
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let transform_config = TransformConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
            cycle_guard: config.cycle_guard,
        };

        // Use the builder to create the handle
        let handle = TransformBuilder::new(handler_with_middleware, transform_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build transform: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Transform,
            translate_stage_event_to_transform,
            check_transform_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for async transform stages.
pub struct AsyncTransformDescriptor<H: AsyncTransformHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: AsyncTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for AsyncTransformDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Transform
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Transform, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_control_strategy_from_middleware_specs(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(&config, StageType::Transform);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved
            .middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config, control_middleware.clone()))
            .collect();
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let transform_config = TransformConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
            cycle_guard: config.cycle_guard,
        };

        // Use the builder to create the handle
        let handle =
            AsyncTransformBuilder::new(handler_with_middleware, transform_config, resources)
                .with_instrumentation(instrumentation)
                .build()
                .await
                .map_err(|e| format!("Failed to build async transform: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Transform,
            translate_stage_event_to_transform,
            check_transform_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

/// Descriptor for sink stages
pub struct SinkDescriptor<H: SinkHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for SinkDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Sink
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
        for factory in &self.middleware {
            // Validate safety
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Sink, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
                // Could choose to panic here for critical errors
            }
        }

        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_control_strategy_from_middleware_specs(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(&config, StageType::Sink);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved
            .middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config, control_middleware.clone()))
            .collect();
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let sink_config = JournalSinkConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            upstream_stages: resources.upstream_stages.clone(),
            buffer_size: None,
            flush_interval_ms: None,
            control_strategy: Some(control_strategy),
        };

        // Use the builder to create the handle
        let handle = JournalSinkBuilder::new(handler_with_middleware, sink_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build sink: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Sink,
            translate_stage_event_to_sink,
            check_sink_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

// Event translation functions

fn translate_stage_event_to_finite_source<H>(
    event: StageEvent,
) -> Result<FiniteSourceEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(FiniteSourceEvent::Initialize),
        StageEvent::Ready => Ok(FiniteSourceEvent::Ready),
        StageEvent::Start => Ok(FiniteSourceEvent::Start),
        StageEvent::BeginDrain => Ok(FiniteSourceEvent::BeginDrain),
        StageEvent::ForceShutdown => {
            Ok(FiniteSourceEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string()))
        }
        _ => Err(format!(
            "Unsupported stage event for finite source: {event:?}"
        )),
    }
}

fn check_finite_source_state<H>(
    state: &FiniteSourceState<H>,
) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        FiniteSourceState::Created => StageStatus::Created,
        FiniteSourceState::Initialized | FiniteSourceState::WaitingForGun => StageStatus::Ready,
        FiniteSourceState::Running => StageStatus::Running,
        FiniteSourceState::Draining => StageStatus::Draining,
        FiniteSourceState::Drained => StageStatus::Drained,
        FiniteSourceState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

fn translate_stage_event_to_infinite_source<H>(
    event: StageEvent,
) -> Result<InfiniteSourceEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(InfiniteSourceEvent::Initialize),
        StageEvent::Ready => Ok(InfiniteSourceEvent::Ready),
        StageEvent::Start => Ok(InfiniteSourceEvent::Start),
        StageEvent::BeginDrain => Ok(InfiniteSourceEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(InfiniteSourceEvent::Error(
            FORCE_SHUTDOWN_MESSAGE.to_string(),
        )),
        _ => Err(format!(
            "Unsupported stage event for infinite source: {event:?}"
        )),
    }
}

fn check_infinite_source_state<H>(
    state: &InfiniteSourceState<H>,
) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        InfiniteSourceState::Created => StageStatus::Created,
        InfiniteSourceState::Initialized | InfiniteSourceState::WaitingForGun => StageStatus::Ready,
        InfiniteSourceState::Running => StageStatus::Running,
        InfiniteSourceState::Draining => StageStatus::Draining,
        InfiniteSourceState::Drained => StageStatus::Drained,
        InfiniteSourceState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

fn translate_stage_event_to_transform<H>(event: StageEvent) -> Result<TransformEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(TransformEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(TransformEvent::Ready), // Transforms don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(TransformEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(TransformEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string())),
        _ => Err(format!("Unsupported stage event for transform: {event:?}")),
    }
}

fn check_transform_state<H>(state: &TransformState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        TransformState::Created => StageStatus::Created,
        TransformState::Initialized => StageStatus::Ready,
        TransformState::Running => StageStatus::Running,
        TransformState::Draining => StageStatus::Draining,
        TransformState::Drained => StageStatus::Drained,
        TransformState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

fn translate_stage_event_to_sink<H>(event: StageEvent) -> Result<JournalSinkEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(JournalSinkEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(JournalSinkEvent::Ready), // Sinks don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(JournalSinkEvent::BeginDrain),
        StageEvent::ForceShutdown => {
            Ok(JournalSinkEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string()))
        }
        _ => Err(format!("Unsupported stage event for sink: {event:?}")),
    }
}

fn check_sink_state<H>(state: &JournalSinkState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        JournalSinkState::Created => StageStatus::Created,
        JournalSinkState::Initialized => StageStatus::Ready,
        JournalSinkState::Running => StageStatus::Running,
        JournalSinkState::Flushing | JournalSinkState::Draining => StageStatus::Draining,
        JournalSinkState::Drained => StageStatus::Drained,
        JournalSinkState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

// ============================================================================
// Stateful Descriptor (FLOWIP-080b)
// ============================================================================

/// Descriptor for stateful transform stages
pub struct StatefulDescriptor<H: StatefulHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub emit_interval: Option<Duration>,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StatefulDescriptor<H> {
    /// Create a new stateful descriptor with no emit interval.
    pub fn new(name: impl Into<String>, handler: H) -> Self {
        Self {
            name: name.into(),
            handler,
            emit_interval: None,
            middleware: Vec::new(),
        }
    }

    /// Configure a supervisor-driven emit interval for timer-driven emission while idle.
    pub fn with_emit_interval(mut self, emit_interval: Duration) -> Self {
        self.emit_interval = Some(emit_interval);
        self
    }

    /// Add middleware to the chain.
    pub fn with_middleware<M: MiddlewareFactory + 'static>(mut self, mw: M) -> Self {
        self.middleware.push(Box::new(mw));
        self
    }

    /// Build into a boxed StageDescriptor for DSL compatibility.
    pub fn build(self) -> Box<dyn StageDescriptor> {
        Box::new(self)
    }
}

#[async_trait]
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for StatefulDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Stateful
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Stateful, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_control_strategy_from_middleware_specs(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Create system middleware with instrumentation (FLOWIP-080o-part-2)
        let mut all_middleware = create_system_middleware(&config, StageType::Stateful);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved
            .middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config, control_middleware.clone()))
            .collect();
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware (FLOWIP-080o-part-2: MiddlewareStateful now exists)
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Create the stage configuration
        let stateful_config = StatefulConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            emit_interval: self.emit_interval,
            control_strategy: Some(control_strategy),
            upstream_stages: resources.upstream_stages.clone(),
        };

        // Use the builder to create the handle
        let handle = StatefulBuilder::new(handler_with_middleware, stateful_config, resources)
            .with_instrumentation(instrumentation)
            .build()
            .await
            .map_err(|e| format!("Failed to build stateful stage: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Stateful,
            translate_stage_event_to_stateful,
            check_stateful_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

fn translate_stage_event_to_stateful<H>(event: StageEvent) -> Result<StatefulEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(StatefulEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(StatefulEvent::Ready), // Stateful stages use Ready like transforms
        StageEvent::BeginDrain => Ok(StatefulEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(StatefulEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string())),
        _ => Err(format!("Unsupported stage event for stateful: {event:?}")),
    }
}

fn check_stateful_state<H>(state: &StatefulState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        StatefulState::Created => StageStatus::Created,
        StatefulState::Initialized => StageStatus::Ready,
        StatefulState::Accumulating | StatefulState::Emitting => StageStatus::Running,
        StatefulState::Draining => StageStatus::Draining,
        StatefulState::Drained => StageStatus::Drained,
        StatefulState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

// ============================================================================
// Join Descriptor (FLOWIP-080l)
// ============================================================================

/// Descriptor for join stages
pub struct JoinDescriptor<H: JoinHandler + 'static> {
    pub name: String,
    pub reference_stage_id: StageId,
    pub reference_stage_var: Option<&'static str>, // For DSL resolution - stage variable name
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor
    for JoinDescriptor<H>
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> StageType {
        StageType::Join
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.middleware
            .iter()
            .map(|f| f.name().to_string())
            .collect()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        &self.middleware
    }

    fn reference_stage_id(&self) -> Option<StageId> {
        Some(self.reference_stage_id)
    }

    fn reference_stage_name(&self) -> Option<&str> {
        self.reference_stage_var
    }

    fn set_reference_stage_id(&mut self, id: StageId) {
        self.reference_stage_id = id;
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
        for factory in &self.middleware {
            let validation_result =
                validate_middleware_safety(factory.as_ref(), StageType::Join, &self.name);

            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
            }
        }

        // Create control strategy before moving middleware
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name,
        );

        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(&config.name, &resolved);
        let control_strategy = create_control_strategy_from_middleware_specs(&resolved.middleware);

        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let mut instrumentation = StageInstrumentation::new_with_config(instrumentation_config);
        let control_provider: Arc<dyn obzenflow_core::ControlMiddlewareProvider> =
            control_middleware.clone();

        // Create system middleware with instrumentation (FLOWIP-080o-part-2)
        let mut all_middleware = create_system_middleware(&config, StageType::Join);

        let expects_circuit_breaker = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "circuit_breaker");
        let expects_rate_limiter = resolved
            .middleware
            .iter()
            .any(|spec| spec.factory.name() == "rate_limiter");

        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved
            .middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config, control_middleware.clone()))
            .collect();
        all_middleware.extend(user_middleware);

        instrumentation
            .bind_control_middleware(
                &config.stage_id,
                &control_provider,
                expects_circuit_breaker,
                expects_rate_limiter,
            )
            .map_err(|e| e.to_string())?;
        let instrumentation = Arc::new(instrumentation);

        // Apply all middleware (FLOWIP-080o-part-2: MiddlewareJoin now exists)
        // Same middleware is applied to both reference and stream sides
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();

        // Extract join-mode configuration from the handler before moving it into the runtime.
        let reference_mode = handler_with_middleware.reference_mode();
        let reference_batch_cap = handler_with_middleware.reference_batch_cap();

        // Create the stage configuration
        // reference_stage_id comes from the builder (stored in self)
        // Stream stages come from topology (in upstream_stages, after DSL adds reference)
        let reference_source_id = self.reference_stage_id;

        // Get stream sources - all upstreams after the reference (which DSL prepended)
        let stream_sources: Vec<StageId> = resources
            .upstream_stages
            .iter()
            .skip(1) // Skip reference which is at index 0
            .copied()
            .collect();

        // For now, we support single stream source
        let stream_source_id = stream_sources
            .first()
            .copied()
            .ok_or_else(|| "Join stage requires at least one stream source".to_string())?;

        let join_config = JoinConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            reference_source_id,
            stream_source_id,
            reference_mode,
            reference_batch_cap,
            control_strategy: Some(control_strategy.clone()),
            upstream_stages: resources.upstream_stages.clone(),
        };

        // Separate reference and stream journals
        // First upstream is reference, rest are streams
        let (reference_journal, stream_journals) =
            if let Some((first, rest)) = resources.upstream_journals.split_first() {
                (first.1.clone(), rest.to_vec())
            } else {
                return Err("Join stage requires at least one upstream journal".to_string());
            };

        // Use the builder to create the handle
        // NOTE: For join stages, the pre-built subscription in resources is stale
        // because DSL mutates upstream_journals AFTER subscription was built
        let handle = JoinBuilder::new(
            handler_with_middleware,
            join_config,
            resources,
            reference_journal,
            stream_journals,
            control_strategy,
        )
        .map_err(|e| format!("Failed to create join builder: {e}"))?
        .with_instrumentation(instrumentation)
        .build()
        .await
        .map_err(|e| format!("Failed to build join stage: {e:?}"))?;

        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Join,
            translate_stage_event_to_join,
            check_join_state,
        );

        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

fn translate_stage_event_to_join<H>(event: StageEvent) -> Result<JoinEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(JoinEvent::Initialize),
        StageEvent::Ready | StageEvent::Start => Ok(JoinEvent::Ready), // Join stages use Ready like transforms
        StageEvent::BeginDrain => Ok(JoinEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(JoinEvent::Error(FORCE_SHUTDOWN_MESSAGE.to_string())),
        _ => Err(format!("Unsupported stage event for join: {event:?}")),
    }
}

fn check_join_state<H>(state: &JoinState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        JoinState::Created => StageStatus::Created,
        JoinState::Initialized => StageStatus::Ready,
        JoinState::Hydrating | JoinState::Live | JoinState::Enriching => StageStatus::Running,
        JoinState::Draining => StageStatus::Draining,
        JoinState::Drained => StageStatus::Drained,
        JoinState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_adapters::middleware::control::circuit_breaker::circuit_breaker;
    use obzenflow_core::event::{JournalEvent, SystemEvent};
    use obzenflow_core::ControlMiddlewareProvider;
    use obzenflow_core::{ChainEvent, EventEnvelope, FlowId};
    use obzenflow_runtime::message_bus::FsmMessageBus;
    use obzenflow_runtime::stages::resources_builder::SubscriptionFactory;

    #[derive(Clone, Debug)]
    struct DummyFiniteSource;

    impl FiniteSourceHandler for DummyFiniteSource {
        fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            // This dummy source never emits data; it's used only to verify that the
            // circuit breaker middleware wires up a CircuitBreakerSourceStrategy.
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncFiniteSourceNoHint;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceNoHint {
        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncFiniteSourceWithHint;

    #[async_trait]
    impl AsyncFiniteSourceHandler for DummyAsyncFiniteSourceWithHint {
        fn suggested_poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(123))
        }

        async fn next(
            &mut self,
        ) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime::stages::SourceError> {
            Ok(None)
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncInfiniteSourceNoHint;

    #[async_trait]
    impl AsyncInfiniteSourceHandler for DummyAsyncInfiniteSourceNoHint {
        async fn next(
            &mut self,
        ) -> Result<Vec<ChainEvent>, obzenflow_runtime::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Debug)]
    struct DummyAsyncInfiniteSourceWithHint;

    #[async_trait]
    impl AsyncInfiniteSourceHandler for DummyAsyncInfiniteSourceWithHint {
        fn suggested_poll_timeout(&self) -> Option<Duration> {
            Some(Duration::from_secs(7))
        }

        async fn next(
            &mut self,
        ) -> Result<Vec<ChainEvent>, obzenflow_runtime::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn async_finite_source_descriptor_seeds_poll_timeout_from_handler_hint() {
        let descriptor = AsyncFiniteSourceDescriptor::new("hinted", DummyAsyncFiniteSourceWithHint);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(123)));
    }

    #[test]
    fn async_finite_source_descriptor_uses_default_poll_timeout_without_hint() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("defaulted", DummyAsyncFiniteSourceNoHint);
        assert_eq!(
            descriptor.poll_timeout,
            Some(DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT)
        );
    }

    #[test]
    fn async_finite_source_descriptor_explicit_poll_timeout_override_wins() {
        let descriptor =
            AsyncFiniteSourceDescriptor::new("override", DummyAsyncFiniteSourceWithHint)
                .with_poll_timeout(Some(Duration::from_secs(1)));
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(1)));
    }

    #[test]
    fn async_infinite_source_descriptor_seeds_poll_timeout_from_handler_hint() {
        let descriptor =
            AsyncInfiniteSourceDescriptor::new("hinted", DummyAsyncInfiniteSourceWithHint);
        assert_eq!(descriptor.poll_timeout, Some(Duration::from_secs(7)));
    }

    #[test]
    fn async_infinite_source_descriptor_keeps_default_poll_timeout_without_hint() {
        let descriptor =
            AsyncInfiniteSourceDescriptor::new("defaulted", DummyAsyncInfiniteSourceNoHint);
        assert_eq!(descriptor.poll_timeout, None);
    }

    #[tokio::test]
    async fn finite_source_with_circuit_breaker_uses_cb_strategy() {
        let stage_id = StageId::new();
        let config = StageConfig {
            stage_id,
            name: "cb_source".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
        };

        // Minimal StageResources: journals are never actually written in this unit test.
        use obzenflow_core::id::JournalId;
        use obzenflow_core::journal::journal_error::JournalError;
        use obzenflow_core::journal::journal_owner::JournalOwner;
        use obzenflow_core::journal::journal_reader::JournalReader;
        use obzenflow_core::journal::Journal;

        struct NoopJournal<T: JournalEvent> {
            id: JournalId,
            owner: Option<JournalOwner>,
            _marker: std::marker::PhantomData<T>,
        }

        impl<T: JournalEvent> NoopJournal<T> {
            fn new(owner: JournalOwner) -> Self {
                Self {
                    id: JournalId::new(),
                    owner: Some(owner),
                    _marker: std::marker::PhantomData,
                }
            }
        }

        struct NoopReader;

        #[async_trait]
        impl<T: JournalEvent + 'static> Journal<T> for NoopJournal<T> {
            fn id(&self) -> &JournalId {
                &self.id
            }

            fn owner(&self) -> Option<&JournalOwner> {
                self.owner.as_ref()
            }

            async fn append(
                &self,
                _event: T,
                _parent: Option<&EventEnvelope<T>>,
            ) -> Result<EventEnvelope<T>, JournalError> {
                Err(JournalError::Implementation {
                    message: "noop journal".to_string(),
                    source: "noop".into(),
                })
            }

            async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_causally_after(
                &self,
                _after_event_id: &obzenflow_core::EventId,
            ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_event(
                &self,
                _event_id: &obzenflow_core::EventId,
            ) -> Result<Option<EventEnvelope<T>>, JournalError> {
                Ok(None)
            }

            async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
                Ok(Box::new(NoopReader))
            }

            async fn reader_from(
                &self,
                _position: u64,
            ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
                Ok(Box::new(NoopReader))
            }

            async fn read_last_n(
                &self,
                _count: usize,
            ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
                // NoopJournal never stores events; always return empty.
                Ok(Vec::new())
            }
        }

        #[async_trait]
        impl<T: JournalEvent + 'static> JournalReader<T> for NoopReader {
            async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
                Ok(None)
            }

            async fn skip(&mut self, _n: u64) -> Result<u64, JournalError> {
                Ok(0)
            }

            fn position(&self) -> u64 {
                0
            }

            fn is_at_end(&self) -> bool {
                true
            }
        }

        let system_owner = JournalOwner::system(obzenflow_core::SystemId::new());
        let stage_owner = JournalOwner::stage(stage_id);

        let data_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(NoopJournal::new(stage_owner.clone()));
        let error_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(NoopJournal::new(stage_owner.clone()));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(NoopJournal::new(system_owner));

        let topology_stage_id = obzenflow_topology::StageId::from_ulid(stage_id.as_ulid());
        let topology_stage = obzenflow_topology::StageInfo::new(
            topology_stage_id,
            "cb_source",
            obzenflow_topology::StageType::FiniteSource,
        );
        let dummy_stage_id =
            obzenflow_topology::StageId::from_ulid(obzenflow_core::StageId::new().as_ulid());
        let dummy_stage = obzenflow_topology::StageInfo::new(
            dummy_stage_id,
            "dummy",
            obzenflow_topology::StageType::Sink,
        );
        let topology = obzenflow_topology::Topology::new_unvalidated(
            vec![topology_stage, dummy_stage],
            vec![obzenflow_topology::DirectedEdge::new(
                topology_stage_id,
                dummy_stage_id,
                obzenflow_topology::EdgeKind::Forward,
            )],
        )
        .expect("topology");
        let backpressure_registry =
            std::sync::Arc::new(obzenflow_runtime::backpressure::BackpressureRegistry::new(
                &topology,
                &obzenflow_runtime::backpressure::BackpressurePlan::disabled(),
            ));

        let resources = StageResources {
            flow_id: FlowId::new(),
            data_journal,
            error_journal,
            system_journal,
            upstream_journals: Vec::new(),
            upstream_stage_names: std::collections::HashMap::new(),
            subscription_factory: SubscriptionFactory::new(std::collections::HashMap::new()),
            upstream_subscription_factory: SubscriptionFactory::new(
                std::collections::HashMap::new(),
            )
            .bind(&[]),
            message_bus: Arc::new(FsmMessageBus::new()),
            upstream_stages: Vec::new(),
            error_journals: Vec::new(),
            backpressure_writer: Default::default(),
            backpressure_readers: Default::default(),
            backpressure_registry,
            replay_archive: None,
        };

        let descriptor = FiniteSourceDescriptor {
            name: "cb_source".to_string(),
            handler: DummyFiniteSource,
            middleware: vec![circuit_breaker(1)],
        };

        let control_middleware = Arc::new(ControlMiddlewareAggregator::new());
        let boxed: Box<dyn StageDescriptor> = Box::new(descriptor);
        let handle = boxed
            .create_handle_with_flow_middleware(
                config,
                resources,
                vec![],
                control_middleware.clone(),
            )
            .await
            .expect("handle creation should succeed");

        // Ensure the breaker state is registered via the flow-scoped provider.
        let cb_state = control_middleware.circuit_breaker_state(&stage_id);
        assert!(
            cb_state.is_some(),
            "circuit breaker state should be registered for source with circuit_breaker middleware"
        );

        // Avoid unused variable warning
        drop(handle);
    }
}

// ============================================================================
// ErrorSink Descriptor (FLOWIP-082e)
// ============================================================================
