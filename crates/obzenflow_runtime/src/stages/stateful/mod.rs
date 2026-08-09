// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful stage implementation
//!
//! Stateful stages maintain state across events, enabling aggregations,
//! windowing operations, and session tracking without `Arc<Mutex>` anti-patterns.
//!
//! Key features:
//! - Functional state updates (handler returns new state)
//! - Type-safe state management (State: Clone + Send + Sync)
//! - Proper lifecycle with Accumulating → Draining → Drained states
//! - Control event strategies for customizing behavior
//! - First-class typed accumulation and emission strategies
//!
//! # Example
//!
//! ```ignore
//! use obzenflow_runtime::stages::{StatefulEmission, TypedStatefulHandler};
//!
//! #[derive(Clone, Debug)]
//! struct CounterHandler;
//!
//! impl TypedStatefulHandler for CounterHandler {
//!     type State = u64;
//!     type Input = CountedFact;
//!     type Output = CountSnapshot;
//!
//!     fn accumulate(&self, state: &mut Self::State, _input: CountedFact) {
//!         *state += 1;
//!     }
//!
//!     fn initial_state(&self) -> Self::State {
//!         0
//!     }
//!
//!     fn emit(&self, state: &Self::State) -> Result<StatefulEmission<u64, CountSnapshot>, HandlerError> {
//!         Ok(StatefulEmission::RetainEpoch {
//!             next_state: *state,
//!             outputs: vec![CountSnapshot { total: *state }],
//!         })
//!     }
//! }
//! ```

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

// FLOWIP-080c: Composable primitives
pub mod ai_map_reduce;
pub mod strategies;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::{StatefulEmission, TypedStatefulHandler};
pub use builder::StatefulBuilder;
pub use config::StatefulConfig;
pub use fsm::{StatefulEvent, StatefulState};
pub use handle::{StatefulHandle, StatefulHandleExt};

// FLOWIP-080c/FLOWIP-134e: first-class accumulation strategies.
pub use strategies::accumulators::{
    Accumulator, Conflate, ConflateState, ConflateTyped, GroupBy, GroupByState, GroupByTyped,
    Reduce, ReduceState, ReduceTyped, StatefulWithEmission, TopN, TopNBy, TopNByEntry,
    TopNBySnapshot, TopNByState, TopNByTyped, TopNEntry, TopNSnapshot, TopNState, TopNTyped,
    WrapperState,
};
pub use strategies::emissions::{EmissionStrategy, EmitAlways, EveryN, OnEOF, TimeWindow};

// FLOWIP-128g: sealed seeded AI map-reduce collector
#[doc(hidden)]
pub use ai_map_reduce::SeededCollectByInput;

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    BackoffStrategy, CompositeStrategy, JonestownSignalStrategy, SignalDecision, SignalGate,
};

// Note: StatefulSupervisor is NOT exported! It's an implementation detail.
