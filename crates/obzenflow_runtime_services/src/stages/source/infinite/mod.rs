//! Infinite source stage implementation
//!
//! Infinite sources never complete naturally (Kafka, WebSocket, etc).

pub mod fsm;
pub mod supervisor;
pub mod builder;
pub mod handle;
pub mod config;

// Re-export public API
pub use builder::InfiniteSourceBuilder;
pub use handle::{InfiniteSourceHandle, InfiniteSourceHandleExt};
pub use config::InfiniteSourceConfig;

// Re-export FSM types for users who need them
pub use fsm::{
    InfiniteSourceState,
    InfiniteSourceEvent,
};