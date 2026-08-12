// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prelude for ObzenFlow DSL - imports needed by the flow! macro
//!
//! Users typically won't import this directly, but the flow! macro uses it internally.

// Core types
pub use obzenflow_core::event::chain_event::ChainEvent;
pub use obzenflow_core::WriterId;

// Supervision
pub use obzenflow_runtime::pipeline::{FlowHandle, PipelineBuilder};

// Core journal interface
pub use obzenflow_core::journal::Journal;

// Topology types
pub use obzenflow_core::id::StageId;
pub use obzenflow_topology::{StageInfo, Topology, TopologyBuilder};

// Handler traits
pub use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkConnector, SinkDescription, SinkWriter, TypedTransformHandler,
};

// Supervisor config
pub use obzenflow_runtime::pipeline::config::StageConfig;

// Monitoring
// FLOWIP-056-666: Monitoring middleware temporarily disabled pending redesign
// pub use obzenflow_adapters::monitoring::{Taxonomy, TaxonomyMetrics};
