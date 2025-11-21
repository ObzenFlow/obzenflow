//! Stateful strategies
//!
//! Composable primitives for stateful event processing.
//! These are NOT handlers themselves but building blocks that compose to implement StatefulHandler.

pub mod accumulators;
pub mod emissions;

// Re-export for convenience
pub use accumulators::*;
pub use emissions::*;
