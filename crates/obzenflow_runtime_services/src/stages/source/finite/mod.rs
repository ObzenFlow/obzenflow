//! Finite source stage implementation
//!
//! Finite sources eventually complete (files, bounded collections).

pub mod fsm;
pub mod supervisor;
pub mod builder;
pub mod handle;
pub mod config;

// Re-export public API
pub use builder::FiniteSourceBuilder;
pub use handle::{FiniteSourceHandle, FiniteSourceHandleExt};
pub use config::FiniteSourceConfig;

// Re-export FSM types for users who need them
pub use fsm::{
    FiniteSourceState,
    FiniteSourceEvent,
};