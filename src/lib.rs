// src/lib.rs
pub mod event_types;
pub mod chain_event;
pub mod step;
pub mod monitoring;
pub mod stages;
pub mod event_sourcing;
pub mod event_store;
pub mod topology;


// Include macros
#[macro_use]
pub mod dsl;

// Re-export macros at crate level (they're already at crate root due to #[macro_export])
// Note: flow_stages and count_stages are already available at crate root

// Re-export commonly used types
pub mod prelude {
    pub use crate::chain_event::ChainEvent;
    pub use crate::step::{Step, StepType, Result};
    pub use crate::monitoring::{Taxonomy, TaxonomyMetrics, RED, USE, GoldenSignals, SAAFE};
    pub use crate::stages::{Source, Stage, Sink, monitored::Monitor};
    pub use crate::event_store::{EventStore, EventStoreConfig};
    pub use crate::event_sourcing::EventSourcedStage;

    // Common imports users will need
    pub use async_trait::async_trait;
    pub use std::time::Duration;
    pub use serde_json::{json, Value};
}
