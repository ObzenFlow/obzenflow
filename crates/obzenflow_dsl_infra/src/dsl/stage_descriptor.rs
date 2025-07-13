//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use obzenflow_runtime_services::{
    stages::{
        common::{
            stage_handle::{BoxedStageHandle, StageEvent, StageType},
            handlers::{
                FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
            },
            control_strategies::{
                ControlEventStrategy, ControlEventAction, ProcessingContext,
                JonestownStrategy, RetryStrategy, WindowingStrategy,
                CompositeStrategy, BackoffStrategy,
            },
        },
        source::{
            finite::{FiniteSourceBuilder, FiniteSourceConfig, FiniteSourceEvent, FiniteSourceState},
            infinite::{InfiniteSourceBuilder, InfiniteSourceConfig, InfiniteSourceEvent, InfiniteSourceState},
        },
        transform::{TransformBuilder, TransformConfig, TransformEvent, TransformState},
        sink::{SinkBuilder, SinkConfig, SinkEvent, SinkState},
    },
    pipeline::config::StageConfig,
    supervised_base::{SupervisorBuilder as SupervisorBuilderTrait},
};
use obzenflow_adapters::middleware::{
    Middleware, MiddlewareFactory, FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    TransformHandlerExt, SinkHandlerExt, ControlStrategyRequirement, BackoffConfig,
    validate_middleware_safety, TimingMiddleware, SystemEnrichmentMiddleware,
    OutcomeEnrichmentMiddleware,
};
use obzenflow_core::{journal::writer_id::WriterId, event::event_envelope::EventEnvelope};
use obzenflow_core::StageId;
use obzenflow_runtime_services::{
    messaging::reactive_journal::ReactiveJournal,
    message_bus::FsmMessageBus,
    stages::StageResources,
};
use crate::stage_handle_adapter::StageHandleAdapter;
use std::sync::Arc;
use async_trait::async_trait;

/// Create system middleware for a stage
fn create_system_middleware(config: &StageConfig, stage_type: obzenflow_core::event::flow_context::StageType) -> Vec<Box<dyn Middleware>> {
    vec![
        Box::new(TimingMiddleware::new(&config.name)),
        Box::new(SystemEnrichmentMiddleware::new(
            config.flow_name.clone(),
            config.flow_name.clone(), // flow_id same as flow_name for now
            config.name.clone(),
            stage_type,
        )),
        Box::new(OutcomeEnrichmentMiddleware::new(&config.name)),
    ]
}

/// Wrapper to convert Arc to Box for CompositeStrategy
struct ArcStrategyWrapper(Arc<dyn ControlEventStrategy>);

impl ControlEventStrategy for ArcStrategyWrapper {
    fn handle_eof(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        self.0.handle_eof(envelope, ctx)
    }
    
    fn handle_watermark(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        self.0.handle_watermark(envelope, ctx)
    }
    
    fn handle_checkpoint(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        self.0.handle_checkpoint(envelope, ctx)
    }
    
    fn handle_drain(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        self.0.handle_drain(envelope, ctx)
    }
}

/// Helper function to create a control strategy from middleware requirements
fn create_control_strategy_from_requirements(
    requirements: Vec<ControlStrategyRequirement>,
    stage_name: &str,
) -> Arc<dyn ControlEventStrategy> {
    match requirements.len() {
        0 => Arc::new(JonestownStrategy), // Default
        1 => create_strategy_for(&requirements[0], stage_name),
        _ => {
            // Multiple requirements - compose them
            let strategies: Vec<Box<dyn ControlEventStrategy>> = requirements
                .iter()
                .map(|req| {
                    let arc_strategy = create_strategy_for(req, stage_name);
                    Box::new(ArcStrategyWrapper(arc_strategy)) as Box<dyn ControlEventStrategy>
                })
                .collect();
            Arc::new(CompositeStrategy::new(strategies))
        }
    }
}

/// Create a strategy for a single requirement
fn create_strategy_for(
    requirement: &ControlStrategyRequirement,
    stage_name: &str,
) -> Arc<dyn ControlEventStrategy> {
    match requirement {
        ControlStrategyRequirement::Retry { max_attempts, backoff } => {
            let backoff_strategy = match backoff {
                BackoffConfig::Fixed { delay } => BackoffStrategy::Fixed { delay: *delay },
                BackoffConfig::Exponential { initial, max, factor, jitter } => {
                    BackoffStrategy::Exponential {
                        initial: *initial,
                        max: *max,
                        factor: *factor,
                        jitter: *jitter,
                    }
                }
            };
            Arc::new(RetryStrategy {
                max_attempts: *max_attempts,
                backoff: backoff_strategy,
            })
        }
        ControlStrategyRequirement::Windowing { window_duration } => {
            Arc::new(WindowingStrategy::new(*window_duration))
        }
        ControlStrategyRequirement::Custom { strategy_id, config: _ } => {
            // For now, log a warning and use default
            tracing::warn!(
                "Custom control strategy '{}' requested for stage '{}', using default",
                strategy_id, stage_name
            );
            Arc::new(JonestownStrategy)
        }
    }
}

/// Trait for stage descriptors that know how to create their supervisors
#[async_trait]
pub trait StageDescriptor: Send + Sync {
    /// Get the stage name
    fn name(&self) -> &str;
    
    /// Create the handle for this stage
    async fn create_handle(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources
    ) -> Result<BoxedStageHandle, String>;
    
    /// Get a debug representation
    fn debug_info(&self) -> String {
        format!("Stage[{}]", self.name())
    }
}

/// Descriptor for finite source stages
pub struct FiniteSourceDescriptor<H: FiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

#[async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor for FiniteSourceDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    async fn create_handle(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::new();
        
        // Create system middleware
        let mut all_middleware = create_system_middleware(&config, obzenflow_core::event::flow_context::StageType::Source);
        
        // Add user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = self.middleware
            .into_iter()
            .map(|factory| factory.create(&config))
            .collect();
        all_middleware.extend(user_middleware);
        
        // Apply all middleware
        let mut builder = self.handler.middleware(writer_id.clone());
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();
        
        // Create the stage configuration
        let source_config = FiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
        };
        
        // Use the builder to create the handle
        let handle = FiniteSourceBuilder::new(
            handler_with_middleware,
            source_config,
            resources.journal,
            resources.message_bus,
        )
        .build()
        .await
        .map_err(|e| format!("Failed to build finite source: {:?}", e))?;
        
        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::InfiniteSource,
            move |event| translate_stage_event_to_finite_source(event),
            |state| check_finite_source_state(state),
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
impl<H: InfiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor for InfiniteSourceDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    async fn create_handle(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::new();
        
        // Create system middleware
        let mut all_middleware = create_system_middleware(&config, obzenflow_core::event::flow_context::StageType::Source);
        
        // Add user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = self.middleware
            .into_iter()
            .map(|factory| factory.create(&config))
            .collect();
        all_middleware.extend(user_middleware);
        
        // Apply all middleware
        let mut builder = self.handler.middleware(writer_id.clone());
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();
        
        // Create the stage configuration
        let source_config = InfiniteSourceConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
        };
        
        // Use the builder to create the handle
        let handle = InfiniteSourceBuilder::new(
            handler_with_middleware,
            source_config,
            resources.journal,
            resources.message_bus,
        )
        .build()
        .await
        .map_err(|e| format!("Failed to build infinite source: {:?}", e))?;
        
        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::InfiniteSource,
            move |event| translate_stage_event_to_infinite_source(event),
            |state| check_infinite_source_state(state),
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
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor for TransformDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    async fn create_handle(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety and collect strategy requirements
        let mut strategy_requirements = Vec::new();
        
        for factory in &self.middleware {
            // Validate safety
            let validation_result = validate_middleware_safety(
                factory.as_ref(),
                StageType::Transform,
                &self.name,
            );
            
            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
                // Could choose to panic here for critical errors
            }
            
            // Collect strategy requirements
            if let Some(requirement) = factory.required_control_strategy() {
                strategy_requirements.push(requirement);
            }
        }
        
        // Create control strategy from requirements
        let control_strategy = create_control_strategy_from_requirements(
            strategy_requirements,
            &self.name,
        );
        
        // Create system middleware
        let mut all_middleware = create_system_middleware(&config, obzenflow_core::event::flow_context::StageType::Transform);
        
        // Add user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = self.middleware
            .into_iter()
            .map(|factory| factory.create(&config))
            .collect();
        all_middleware.extend(user_middleware);
        
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
        };
        
        // Use the builder to create the handle
        let handle = TransformBuilder::new(
            handler_with_middleware,
            transform_config,
            resources.journal,
            resources.message_bus,
        )
        .build()
        .await
        .map_err(|e| format!("Failed to build transform: {:?}", e))?;
        
        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Transform,
            move |event| translate_stage_event_to_transform(event),
            |state| check_transform_state(state),
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
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StageDescriptor for SinkDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    async fn create_handle(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety and collect strategy requirements
        let mut strategy_requirements = Vec::new();
        
        for factory in &self.middleware {
            // Validate safety
            let validation_result = validate_middleware_safety(
                factory.as_ref(),
                StageType::Sink,
                &self.name,
            );
            
            if !validation_result.is_ok() {
                for error in &validation_result.errors {
                    tracing::error!("{}", error);
                }
                // Could choose to panic here for critical errors
            }
            
            // Collect strategy requirements
            if let Some(requirement) = factory.required_control_strategy() {
                strategy_requirements.push(requirement);
            }
        }
        
        // Create control strategy from requirements
        let control_strategy = create_control_strategy_from_requirements(
            strategy_requirements,
            &self.name,
        );
        
        // Create system middleware
        let mut all_middleware = create_system_middleware(&config, obzenflow_core::event::flow_context::StageType::Sink);
        
        // Add user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = self.middleware
            .into_iter()
            .map(|factory| factory.create(&config))
            .collect();
        all_middleware.extend(user_middleware);
        
        // Apply all middleware
        let mut builder = self.handler.middleware();
        for mw in all_middleware {
            builder = builder.with(mw);
        }
        let handler_with_middleware = builder.build();
        
        // Create the stage configuration
        let sink_config = SinkConfig {
            stage_id: config.stage_id,
            stage_name: config.name.clone(),
            flow_name: config.flow_name.clone(),
            upstream_stages: resources.upstream_stages.clone(),
            buffer_size: None,
            flush_interval_ms: None,
        };
        
        // Use the builder to create the handle
        let handle = SinkBuilder::new(
            handler_with_middleware,
            sink_config,
            resources.journal,
            resources.message_bus,
        )
        .build()
        .await
        .map_err(|e| format!("Failed to build sink: {:?}", e))?;
        
        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::Sink,
            move |event| translate_stage_event_to_sink(event),
            |state| check_sink_state(state),
        );
        
        Ok(Box::new(adapter) as BoxedStageHandle)
    }
}

// Event translation functions

fn translate_stage_event_to_finite_source<H>(event: StageEvent) -> Result<FiniteSourceEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(FiniteSourceEvent::Initialize),
        StageEvent::Start => Ok(FiniteSourceEvent::Start),
        StageEvent::BeginDrain => Ok(FiniteSourceEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(FiniteSourceEvent::Error("Force shutdown requested".to_string())),
        _ => Err(format!("Unsupported stage event for finite source: {:?}", event)),
    }
}

fn check_finite_source_state<H>(state: &FiniteSourceState<H>) -> crate::stage_handle_adapter::StageStatus {
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

fn translate_stage_event_to_infinite_source<H>(event: StageEvent) -> Result<InfiniteSourceEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(InfiniteSourceEvent::Initialize),
        StageEvent::Start => Ok(InfiniteSourceEvent::Start),
        StageEvent::BeginDrain => Ok(InfiniteSourceEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(InfiniteSourceEvent::Error("Force shutdown requested".to_string())),
        _ => Err(format!("Unsupported stage event for infinite source: {:?}", event)),
    }
}

fn check_infinite_source_state<H>(state: &InfiniteSourceState<H>) -> crate::stage_handle_adapter::StageStatus {
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
        StageEvent::Start => Ok(TransformEvent::Ready), // Transforms don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(TransformEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(TransformEvent::Error("Force shutdown requested".to_string())),
        _ => Err(format!("Unsupported stage event for transform: {:?}", event)),
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

fn translate_stage_event_to_sink<H>(event: StageEvent) -> Result<SinkEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(SinkEvent::Initialize),
        StageEvent::Start => Ok(SinkEvent::Ready), // Sinks don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(SinkEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(SinkEvent::Error("Force shutdown requested".to_string())),
        _ => Err(format!("Unsupported stage event for sink: {:?}", event)),
    }
}

fn check_sink_state<H>(state: &SinkState<H>) -> crate::stage_handle_adapter::StageStatus {
    use crate::stage_handle_adapter::StageStatus;
    match state {
        SinkState::Created => StageStatus::Created,
        SinkState::Initialized => StageStatus::Ready,
        SinkState::Running => StageStatus::Running,
        SinkState::Flushing | SinkState::Draining => StageStatus::Draining,
        SinkState::Drained => StageStatus::Drained,
        SinkState::Failed(_) => StageStatus::Failed,
        _ => StageStatus::Created,
    }
}