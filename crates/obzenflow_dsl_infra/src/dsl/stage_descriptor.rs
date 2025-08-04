//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use obzenflow_runtime_services::{
    stages::{
        common::{
            stage_handle::{BoxedStageHandle, StageEvent},
            handlers::{
                FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
            },
            control_strategies::{
                ControlEventStrategy,
                JonestownStrategy,
                CompositeStrategy,
            },
        },
        source::{
            finite::{FiniteSourceBuilder, FiniteSourceConfig, FiniteSourceEvent, FiniteSourceState},
            infinite::{InfiniteSourceBuilder, InfiniteSourceConfig, InfiniteSourceEvent, InfiniteSourceState},
        },
        transform::{TransformBuilder, TransformConfig, TransformEvent, TransformState},
        sink::{
            journal_sink::{JournalSinkBuilder, JournalSinkConfig, JournalSinkEvent, JournalSinkState},
        },
    },
    pipeline::config::StageConfig,
    supervised_base::{SupervisorBuilder as SupervisorBuilderTrait},
};
use obzenflow_adapters::middleware::{
    Middleware, MiddlewareFactory, FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    TransformHandlerExt, SinkHandlerExt,
    TimingMiddleware, SystemEnrichmentMiddleware, OutcomeEnrichmentMiddleware,
    validate_middleware_safety
};
use obzenflow_core::WriterId;
use obzenflow_runtime_services::{
    stages::StageResources,
    metrics::instrumentation::{StageInstrumentation, InstrumentationConfig},
};
use crate::stage_handle_adapter::StageHandleAdapter;
use std::sync::Arc;
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;

/// Create system middleware for a stage
fn create_system_middleware(
    config: &StageConfig, 
    stage_type: StageType,
) -> Vec<Box<dyn Middleware>> {
    tracing::info!("Creating system middleware for stage '{}' of type {:?}", config.name, stage_type);
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


/// Helper function to create a control strategy from middleware factories
fn create_control_strategy_from_factories(
    factories: &[Box<dyn MiddlewareFactory>],
    _stage_name: &str,
) -> Arc<dyn ControlEventStrategy> {
    let strategies: Vec<Box<dyn ControlEventStrategy>> = factories
        .iter()
        .filter_map(|factory| factory.create_control_strategy())
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
    
    /// Get the stage type
    fn stage_type(&self) -> StageType;
    
    /// Create the handle for this stage
    async fn create_handle(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources
    ) -> Result<BoxedStageHandle, String> {
        // Default implementation without flow middleware
        self.create_handle_with_flow_middleware(config, resources, vec![]).await
    }
    
    /// Create the handle for this stage with flow-level middleware
    async fn create_handle_with_flow_middleware(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>
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
    
    fn stage_type(&self) -> StageType {
        StageType::FiniteSource
    }
    
    async fn create_handle_with_flow_middleware(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::from(config.stage_id);
        
        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let instrumentation = Arc::new(StageInstrumentation::new_with_config(instrumentation_config));
        
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name
        );
        
        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(
            &config.name,
            &resolved
        );
        
        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(
            &config, 
            StageType::FiniteSource
        );
        
        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved.middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config))
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
            resources,
        )
        .with_instrumentation(instrumentation)
        .build()
        .await
        .map_err(|e| format!("Failed to build finite source: {:?}", e))?;
        
        // Create adapter to bridge to StageHandle
        let adapter = StageHandleAdapter::new(
            handle,
            config.stage_id,
            config.name,
            StageType::FiniteSource,
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
    
    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }
    
    async fn create_handle_with_flow_middleware(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>
    ) -> Result<BoxedStageHandle, String> {
        let writer_id = WriterId::from(config.stage_id);
        
        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let instrumentation = Arc::new(StageInstrumentation::new_with_config(instrumentation_config));
        
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name
        );
        
        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(
            &config.name,
            &resolved
        );
        
        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(
            &config, 
            StageType::InfiniteSource
        );
        
        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved.middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config))
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
            resources,
        )
        .with_instrumentation(instrumentation)
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
    
    fn stage_type(&self) -> StageType {
        StageType::Transform
    }
    
    async fn create_handle_with_flow_middleware(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
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
        }
        
        // Create control strategy before moving middleware
        let control_strategy = create_control_strategy_from_factories(
            &self.middleware,
            &self.name,
        );
        
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name
        );
        
        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(
            &config.name,
            &resolved
        );
        
        tracing::warn!(
            "Control strategy created from stage middleware only - flow middleware not included for stage '{}'",
            &config.name
        );
        
        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let instrumentation = Arc::new(StageInstrumentation::new_with_config(instrumentation_config));
        
        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(
            &config, 
            StageType::Transform
        );
        
        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved.middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config))
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
            resources.flow_id,
            resources.data_journal,
            resources.error_journal,
            resources.system_journal,
            resources.upstream_journals,
            resources.message_bus,
        )
        .with_instrumentation(instrumentation)
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
    
    fn stage_type(&self) -> StageType {
        StageType::Sink
    }
    
    async fn create_handle_with_flow_middleware(
        self: Box<Self>, 
        config: StageConfig, 
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>
    ) -> Result<BoxedStageHandle, String> {
        // Validate middleware safety
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
        }
        
        // Create control strategy before moving middleware
        let _control_strategy = create_control_strategy_from_factories(
            &self.middleware,  // TODO: Use resolved middleware once MiddlewareFactory supports clone
            &self.name,
        );
        
        // Resolve flow and stage middleware
        let resolved = crate::middleware_resolution::resolve_middleware(
            flow_middleware,
            self.middleware,
            &config.name
        );
        
        // Log the resolution
        crate::middleware_resolution::log_resolved_middleware(
            &config.name,
            &resolved
        );
        
        // Create instrumentation configuration
        let instrumentation_config = InstrumentationConfig::default();
        let instrumentation = Arc::new(StageInstrumentation::new_with_config(instrumentation_config));
        
        // Create system middleware with instrumentation
        let mut all_middleware = create_system_middleware(
            &config, 
            StageType::Sink
        );
        
        // Add resolved user middleware
        let user_middleware: Vec<Box<dyn Middleware>> = resolved.middleware
            .into_iter()
            .map(|spec| spec.factory.create(&config))
            .collect();
        all_middleware.extend(user_middleware);
        
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
        };
        
        // Use the builder to create the handle
        let handle = JournalSinkBuilder::new(
            handler_with_middleware,
            sink_config,
            resources,
        )
        .with_instrumentation(instrumentation)
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

fn translate_stage_event_to_sink<H>(event: StageEvent) -> Result<JournalSinkEvent<H>, String> {
    match event {
        StageEvent::Initialize => Ok(JournalSinkEvent::Initialize),
        StageEvent::Start => Ok(JournalSinkEvent::Ready), // Sinks don't have Start, they use Ready
        StageEvent::BeginDrain => Ok(JournalSinkEvent::BeginDrain),
        StageEvent::ForceShutdown => Ok(JournalSinkEvent::Error("Force shutdown requested".to_string())),
        _ => Err(format!("Unsupported stage event for sink: {:?}", event)),
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
// ErrorSink Descriptor (FLOWIP-082e)
// ============================================================================

