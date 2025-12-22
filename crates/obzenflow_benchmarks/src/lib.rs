//! # ObzenFlow Benchmarks
//!
//! This crate contains performance benchmarks for the ObzenFlow event streaming framework.
//!
//! ## Benchmark Categories
//!
//! - **Latency benchmarks**: Measure per-event processing time through pipelines of various depths
//! - **Throughput benchmarks**: Measure sustained event processing rates
//! - **Runtime benchmarks**: Test runtime characteristics like idle CPU usage and threading behavior
//! - **Integration benchmarks**: End-to-end pipeline execution tests
//!
//! ## Running Benchmarks
//!
//! Run all benchmarks:
//! ```bash
//! cargo bench -p obzenflow_benchmarks
//! ```
//!
//! Run specific benchmark category:
//! ```bash
//! cargo bench -p obzenflow_benchmarks latency
//! ```
//!
//! Run individual benchmark:
//! ```bash
//! cargo bench -p obzenflow_benchmarks per_event_latency_3_stage
//! ```

// This is primarily a benchmark crate, but we can expose some common utilities
// that benchmarks might share

/// Re-export commonly used types for benchmarks
pub mod prelude {
    // Core types
    pub use obzenflow_core::ChainEvent as Event;
    pub use obzenflow_core::EventId as Id;
    pub use obzenflow_core::WriterId as Writer;
    pub use obzenflow_core::{ChainEvent, EventId, Result, WriterId};

    // Runtime services
    pub use obzenflow_runtime_services::prelude::*;

    // DSL
    pub use obzenflow_dsl_infra::prelude::*;

    // Journal
    pub use obzenflow_infra::journal::*;

    // Monitoring
    pub use obzenflow_adapters::monitoring::*;
}

// Any benchmark-specific utilities can be added here
// For example, common benchmark fixtures, measurement helpers, etc.
