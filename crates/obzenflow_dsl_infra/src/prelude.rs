//! Prelude for ObzenFlow DSL - imports needed by the flow! macro
//!
//! Users typically won't import this directly, but the flow! macro uses it internally.

// Core types
pub use obzenflow_core::event::chain_event::ChainEvent;

// Supervision
pub use obzenflow_runtime_services::supervisor::{PipelineSupervisor, FlowHandle};

// Core journal interface
pub use obzenflow_core::journal::journal::Journal;

// Topology types
pub use obzenflow_topology_services::builder::TopologyBuilder;
pub use obzenflow_topology_services::stages::{StageId, StageInfo};
pub use obzenflow_topology_services::topology::Topology;

// Middleware
pub use obzenflow_adapters::middleware::{
    EventHandlerExt, MonitoringMiddleware,
    FlowObserver, apply_middleware_vec
};

// Re-export EventHandler for convenience
pub use obzenflow_runtime_services::control_plane::stage_supervisor::event_handler::EventHandler;

// Monitoring
pub use obzenflow_adapters::monitoring::{Taxonomy, TaxonomyMetrics};

// Pipeline
pub use obzenflow_runtime_services::control_plane::pipeline_supervisor::pipeline::pipeline::Pipeline;