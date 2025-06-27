//! Prelude for ObzenFlow DSL - imports needed by the flow! macro
//!
//! Users typically won't import this directly, but the flow! macro uses it internally.

// Core types
pub use obzenflow_core::event::chain_event::ChainEvent;
pub use obzenflow_core::journal::writer_id::WriterId;

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
    MonitoringMiddleware,
    TransformHandlerExt, FiniteSourceHandlerExt, InfiniteSourceHandlerExt, SinkHandlerExt,
};

// Handler traits
pub use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
};

// Supervisor config
pub use obzenflow_runtime_services::control_plane::stages::supervisors::StageConfig;

// Monitoring
pub use obzenflow_adapters::monitoring::{Taxonomy, TaxonomyMetrics};

// Pipeline
pub use obzenflow_runtime_services::control_plane::pipeline::Pipeline;