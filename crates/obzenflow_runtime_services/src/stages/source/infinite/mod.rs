//! Infinite source stage implementation
//!
//! Infinite sources never complete naturally (Kafka, WebSocket, etc).

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

// Re-export public API
pub use builder::InfiniteSourceBuilder;
pub use config::InfiniteSourceConfig;
pub use handle::{InfiniteSourceHandle, InfiniteSourceHandleExt};

// Re-export FSM types for users who need them
pub use fsm::{InfiniteSourceEvent, InfiniteSourceState};
