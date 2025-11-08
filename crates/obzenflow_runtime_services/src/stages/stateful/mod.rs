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
//! ```rust,ignore
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
pub mod accumulators;
pub mod emission;

// Public API - only expose builder, handle, and essential types
pub use builder::StatefulBuilder;
pub use config::StatefulConfig;
pub use handle::{StatefulHandle, StatefulHandleExt};
pub use fsm::{StatefulState, StatefulEvent};
pub use crate::stages::common::handlers::StatefulHandler;

// FLOWIP-080c: Re-export commonly used primitives for convenience
pub use accumulators::{Accumulator, GroupBy, Reduce, Conflate, StatefulWithEmission};
pub use emission::{EmissionStrategy, OnEOF, EveryN, TimeWindow, EmitAlways};

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    ControlEventStrategy, ControlEventAction,
    JonestownStrategy, RetryStrategy, BackoffStrategy,
    WindowingStrategy, CompositeStrategy,
};

// Note: StatefulSupervisor is NOT exported! It's an implementation detail.
