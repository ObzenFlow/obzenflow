//! Prelude for ObzenFlow DSL - imports needed by the flow! macro
//!
//! Users typically won't import this directly, but the flow! macro uses it internally.

// Core types
pub use obzenflow_core::event::chain_event::ChainEvent;
pub use obzenflow_core::WriterId;

// Supervision
pub use obzenflow_runtime_services::pipeline::{FlowHandle, PipelineBuilder};

// Core journal interface
pub use obzenflow_core::journal::journal::Journal;

// Topology types
pub use obzenflow_core::id::StageId;
pub use obzenflow_topology::{StageInfo, Topology, TopologyBuilder};

// Middleware
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
pub use obzenflow_adapters::middleware::{
    FiniteSourceHandlerExt, InfiniteSourceHandlerExt, SinkHandlerExt, TransformHandlerExt,
};

// Handler traits
pub use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, SinkHandler, TransformHandler,
};

// Supervisor config
pub use obzenflow_runtime_services::pipeline::config::StageConfig;

// Monitoring
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
// pub use obzenflow_adapters::monitoring::{Taxonomy, TaxonomyMetrics};
