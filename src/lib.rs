// src/lib.rs
pub mod step;
pub mod runtime;
pub mod metrics;
pub mod io;
pub mod stages;

// Include macros
#[macro_use]
pub mod dsl;

// Re-export commonly used types
pub mod prelude {
    pub use crate::step::{ChainEvent, PipelineStep, Result, StepMetrics, MetricsSnapshot};
    pub use crate::runtime::{Pipeline, PipelineBuilder, PipelineReport};
    pub use crate::io::{LogSource, LogSink};

    // Common imports users will need
    pub use async_trait::async_trait;
    pub use tokio::sync::mpsc::{channel, Receiver, Sender};
    pub use std::time::Duration;
    pub use serde_json::{json, Value};
}
