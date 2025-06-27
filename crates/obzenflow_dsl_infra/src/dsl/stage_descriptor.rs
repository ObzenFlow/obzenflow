//! Stage descriptors that carry type information and know how to create supervisors
//!
//! This is the core of the let bindings approach - each stage macro creates a
//! descriptor that encapsulates both the handler and how to create its supervisor.

use obzenflow_runtime_services::control_plane::stages::{
    BoxedStageHandle,
    supervisors::{
        StageConfig, FiniteSourceSupervisor, InfiniteSourceSupervisor,
        TransformSupervisor, SinkSupervisor
    },
    handler_traits::{
        FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
    }
};
use obzenflow_adapters::middleware::{
    Middleware, FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    TransformHandlerExt, SinkHandlerExt
};
use obzenflow_core::journal::writer_id::WriterId;

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
    pub middleware: Vec<Box<dyn Middleware>>,
}

impl<H: FiniteSourceHandler + 'static> StageDescriptor for FiniteSourceDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
        let writer_id = WriterId::new();
        
        // Apply middleware and create supervisor
        if self.middleware.is_empty() {
            let supervisor = FiniteSourceSupervisor::new(self.handler, config);
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let mut builder = self.handler.middleware(writer_id.clone());
            for mw in self.middleware {
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
    pub middleware: Vec<Box<dyn Middleware>>,
}

impl<H: InfiniteSourceHandler + 'static> StageDescriptor for InfiniteSourceDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
        let writer_id = WriterId::new();
        
        // Apply middleware and create supervisor
        if self.middleware.is_empty() {
            let supervisor = InfiniteSourceSupervisor::new(self.handler, config);
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let mut builder = self.handler.middleware(writer_id.clone());
            for mw in self.middleware {
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
    pub middleware: Vec<Box<dyn Middleware>>,
}

impl<H: TransformHandler + 'static> StageDescriptor for TransformDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
        // Apply middleware and create supervisor
        if self.middleware.is_empty() {
            let supervisor = TransformSupervisor::new(self.handler, config);
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let mut builder = self.handler.middleware();
            for mw in self.middleware {
                builder = builder.with(mw);
            }
            let handler_with_middleware = builder.build();
            let supervisor = TransformSupervisor::new(handler_with_middleware, config);
            Box::new(supervisor) as BoxedStageHandle
        }
    }
}

/// Descriptor for sink stages
pub struct SinkDescriptor<H: SinkHandler + 'static> {
    pub name: String,
    pub handler: H,
    pub middleware: Vec<Box<dyn Middleware>>,
}

impl<H: SinkHandler + 'static> StageDescriptor for SinkDescriptor<H> {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn create_supervisor(self: Box<Self>, config: StageConfig) -> BoxedStageHandle {
        // Apply middleware and create supervisor
        if self.middleware.is_empty() {
            let supervisor = SinkSupervisor::new(self.handler, config);
            Box::new(supervisor) as BoxedStageHandle
        } else {
            let mut builder = self.handler.middleware();
            for mw in self.middleware {
                builder = builder.with(mw);
            }
            let handler_with_middleware = builder.build();
            let supervisor = SinkSupervisor::new(handler_with_middleware, config);
            Box::new(supervisor) as BoxedStageHandle
        }
    }
}