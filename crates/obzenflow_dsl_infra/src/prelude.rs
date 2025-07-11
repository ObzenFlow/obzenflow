//! Prelude for ObzenFlow DSL - imports needed by the flow! macro
//!
//! Users typically won't import this directly, but the flow! macro uses it internally.

// Core types
pub use obzenflow_core::event::chain_event::ChainEvent;
pub use obzenflow_core::journal::writer_id::WriterId;

// Supervision
pub use obzenflow_runtime_services::pipeline::{FlowHandle, PipelineBuilder};

// Core journal interface
pub use obzenflow_core::journal::journal::Journal;

// Topology types
pub use obzenflow_topology_services::builder::TopologyBuilder;
pub use obzenflow_topology_services::stages::{StageId, StageInfo};
pub use obzenflow_topology_services::topology::Topology;

// Middleware
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
pub use obzenflow_adapters::middleware::{
    TransformHandlerExt, FiniteSourceHandlerExt, InfiniteSourceHandlerExt, SinkHandlerExt,
};

// Handler traits
pub use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, TransformHandler, SinkHandler
};

// Supervisor config
pub use obzenflow_runtime_services::pipeline::config::StageConfig;

// Monitoring
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
// pub use obzenflow_adapters::monitoring::{Taxonomy, TaxonomyMetrics};

