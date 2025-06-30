//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use obzenflow_runtime_services::control_plane::stages::{
    BoxedStageHandle,
    supervisors::{
        StageConfig, FiniteSourceSupervisor, InfiniteSourceSupervisor,
        TransformSupervisor, SinkSupervisor,
        stage_handle::StageType,
    },
    handler_traits::{
        FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
    },
    control_strategy::{
        ControlEventStrategy, ControlEventAction, ProcessingContext,
        JonestownStrategy, RetryStrategy, WindowingStrategy,
        CompositeStrategy, BackoffStrategy,
    },
};
use obzenflow_adapters::middleware::{
    Middleware, MiddlewareFactory, FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    TransformHandlerExt, SinkHandlerExt, ControlStrategyRequirement, BackoffConfig,
    validate_middleware_safety,
};
use obzenflow_core::{journal::writer_id::WriterId, event::event_envelope::EventEnvelope};
use std::sync::Arc;

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
pub trait StageDescriptor: Send + Sync {
    /// Get the stage name
    fn name(&self) -> &str;
    
    /// Create the supervisor for this stage
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle;
    
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

impl<H: FiniteSourceHandler + 'static> StageDescriptor for FiniteSourceDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
        let writer_id = WriterId::new();
        
        // Create middleware instances from factories
        if self.middleware.is_empty() {
            let supervisor = FiniteSourceSupervisor::new(self.handler, config);
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let middleware_instances: Vec<Box<dyn Middleware>> = self.middleware
                .into_iter()
                .map(|factory| factory.create(&config))
                .collect();
            
            let mut builder = self.handler.middleware(writer_id.clone());
            for mw in middleware_instances {
                builder = builder.with(mw);
            }
            let handler_with_middleware = builder.build();
            let supervisor = FiniteSourceSupervisor::new(handler_with_middleware, config);
            Box::new(supervisor) as BoxedStageHandle
        }
    }
}

/// Descriptor for infinite source stages
pub struct InfiniteSourceDescriptor<H: InfiniteSourceHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: InfiniteSourceHandler + 'static> StageDescriptor for InfiniteSourceDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
        let writer_id = WriterId::new();
        
        // Create middleware instances from factories
        if self.middleware.is_empty() {
            let supervisor = InfiniteSourceSupervisor::new(self.handler, config);
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let middleware_instances: Vec<Box<dyn Middleware>> = self.middleware
                .into_iter()
                .map(|factory| factory.create(&config))
                .collect();
            
            let mut builder = self.handler.middleware(writer_id.clone());
            for mw in middleware_instances {
                builder = builder.with(mw);
            }
            let handler_with_middleware = builder.build();
            let supervisor = InfiniteSourceSupervisor::new(handler_with_middleware, config);
            Box::new(supervisor) as BoxedStageHandle
        }
    }
}

/// Descriptor for transform stages
pub struct TransformDescriptor<H: TransformHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: TransformHandler + 'static> StageDescriptor for TransformDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
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
        
        // Create middleware instances from factories
        if self.middleware.is_empty() {
            let supervisor = TransformSupervisor::with_strategy(
                self.handler,
                config,
                control_strategy,
            );
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let middleware_instances: Vec<Box<dyn Middleware>> = self.middleware
                .into_iter()
                .map(|factory| factory.create(&config))
                .collect();
            
            let mut builder = self.handler.middleware();
            for mw in middleware_instances {
                builder = builder.with(mw);
            }
            let handler_with_middleware = builder.build();
            let supervisor = TransformSupervisor::with_strategy(
                handler_with_middleware,
                config,
                control_strategy,
            );
            Box::new(supervisor) as BoxedStageHandle
        }
    }
}

/// Descriptor for sink stages
pub struct SinkDescriptor<H: SinkHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<H: SinkHandler + 'static> StageDescriptor for SinkDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
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
        
        // Create middleware instances from factories
        if self.middleware.is_empty() {
            let supervisor = SinkSupervisor::with_strategy(
                self.handler,
                config,
                control_strategy,
            );
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let middleware_instances: Vec<Box<dyn Middleware>> = self.middleware
                .into_iter()
                .map(|factory| factory.create(&config))
                .collect();
            
            let mut builder = self.handler.middleware();
            for mw in middleware_instances {
                builder = builder.with(mw);
            }
            let handler_with_middleware = builder.build();
            let supervisor = SinkSupervisor::with_strategy(
                handler_with_middleware,
                config,
                control_strategy,
            );
            Box::new(supervisor) as BoxedStageHandle
        }
    }
}