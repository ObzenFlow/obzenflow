//! Finite source stage implementation
//!
//! Finite sources eventually complete (files, bounded collections).

pub mod async_builder;
pub mod async_supervisor;
pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

// Re-export public API
pub use async_builder::AsyncFiniteSourceBuilder;
pub use builder::FiniteSourceBuilder;
pub use config::FiniteSourceConfig;
pub use handle::{FiniteSourceHandle, FiniteSourceHandleExt};

// Re-export FSM types for users who need them
pub use fsm::{FiniteSourceEvent, FiniteSourceState};
