//! Stateful stage implementation
//!
//! Stateful stages maintain state across events, enabling aggregations,
//! windowing operations, and session tracking without Arc<Mutex> anti-patterns.
//!
//! Key features:
//! - Functional state updates (handler returns new state)
//! - Type-safe state management (State: Clone + Send + Sync)
//! - Proper lifecycle with Accumulating → Draining → Drained states
//! - Control event strategies for customizing behavior
//! - Foundation for FLOWIP-080c primitives (GroupBy, Reduce, Conflate)
//!
//! # Example
//!
//! ```ignore,ignore
//! use obzenflow_runtime_services::stages::common::handlers::StatefulHandler;
//! use obzenflow_core::{ChainEvent, Result};
//!
//! #[derive(Clone, Debug)]
//! struct CounterHandler;
//!
//! impl StatefulHandler for CounterHandler {
//!     type State = u64;
//!
//!     fn process(&self, state: &Self::State, _event: ChainEvent) -> (Self::State, Vec<ChainEvent>) {
//!         (*state + 1, vec![])  // Accumulate count, emit on drain
//!     }
//!
//!     fn initial_state(&self) -> Self::State {
//!         0
//!     }
//!
//!     async fn drain(&mut self, state: &Self::State) -> Result<Vec<ChainEvent>> {
//!         Ok(vec![ChainEvent::data(
//!             EventId::new(),
//!             WriterId::new(),
//!             "count",
//!             json!({ "total": *state })
//!         )])
//!     }
//! }
//! ```

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

// FLOWIP-080c: Composable primitives
pub mod strategies;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::StatefulHandler;
pub use builder::StatefulBuilder;
pub use config::StatefulConfig;
pub use fsm::{StatefulEvent, StatefulState};
pub use handle::{StatefulHandle, StatefulHandleExt};

// FLOWIP-080c: Re-export commonly used strategies for convenience
pub use strategies::accumulators::{Accumulator, Conflate, GroupBy, Reduce, StatefulWithEmission};
pub use strategies::emissions::{EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow};

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    BackoffStrategy, CompositeStrategy, ControlEventAction, ControlEventStrategy,
    JonestownStrategy, RetryStrategy, WindowingStrategy,
};

// Note: StatefulSupervisor is NOT exported! It's an implementation detail.
