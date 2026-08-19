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

use obzenflow_core::{OneFactStageOutput, TypedPayload};
use std::fmt::Debug;
use std::hash::Hash;

/// Construct a typed fold over an initial accumulator value.
pub fn reduce<T, S, F>(initial: S, reduce_fn: F) -> Reduce<T, S, F>
where
    T: TypedPayload + Send + Sync,
    S: Clone + Send + Sync + Debug + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    Reduce::new(initial, reduce_fn)
}

/// Construct a latest-value-per-key accumulator.
pub fn conflate<T, K, FKey>(key_fn: FKey) -> Conflate<T, K, FKey>
where
    T: Send + Sync + Clone + Debug + TypedPayload,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
{
    Conflate::new(key_fn)
}

/// Construct a typed keyed aggregation.
pub fn group_by<T, K, S, O, FKey, FUpdate, FOutput>(
    key_fn: FKey,
    update_fn: FUpdate,
    output_fn: FOutput,
) -> GroupBy<T, K, S, O, FKey, FUpdate, FOutput>
where
    T: TypedPayload + Send + Sync,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    S: Default + Send + Sync + Debug + Clone,
    O: OneFactStageOutput + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone,
    FOutput: Fn(&K, &S) -> O + Send + Sync + Clone,
{
    GroupBy::new(key_fn, update_fn, output_fn)
}

/// Construct a top-N accumulator using each key's latest score.
pub fn top_n<T, K, O, FKey, FScore, FOutput>(
    n: usize,
    key_fn: FKey,
    score_fn: FScore,
    output_fn: FOutput,
) -> TopN<T, K, O, FKey, FScore, FOutput>
where
    T: Clone + Debug + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Send + Sync + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
    FOutput: Fn(TopNSnapshot<K, T>) -> O + Send + Sync + Clone + 'static,
{
    TopN::new(n, key_fn, score_fn, output_fn)
}

/// Construct a top-N accumulator using aggregate scores per key.
pub fn top_n_by<T, K, O, FKey, FScore, FOutput>(
    n: usize,
    key_fn: FKey,
    score_fn: FScore,
    output_fn: FOutput,
) -> TopNBy<T, K, O, FKey, FScore, FOutput>
where
    T: Clone + Debug + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Send + Sync + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
    FOutput: Fn(TopNBySnapshot<K, T>) -> O + Send + Sync + Clone + 'static,
{
    TopNBy::new(n, key_fn, score_fn, output_fn)
}

// FLOWIP-128g: sealed seeded AI map-reduce collector
#[doc(hidden)]
pub use ai_map_reduce::SeededCollectByInput;

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    BackoffStrategy, CompositeStrategy, JonestownSignalStrategy, SignalDecision, SignalGate,
};

// Note: StatefulSupervisor is NOT exported! It's an implementation detail.

#[cfg(test)]
mod helper_tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Input {
        key: u32,
        score: f64,
    }

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "stateful.helper.input";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct State(u64);

    impl TypedPayload for State {
        const EVENT_TYPE: &'static str = "stateful.helper.state";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Output(u64);

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "stateful.helper.output";
    }

    #[test]
    fn owner_helpers_construct_every_supported_accumulator() {
        let _reduce = reduce(State(0), |state: &mut State, _input: &Input| state.0 += 1);
        let _conflate = conflate(|input: &Input| input.key);
        let _grouped = group_by(
            |input: &Input| input.key,
            |count: &mut u64, _input: &Input| *count += 1,
            |_key: &u32, count: &u64| Output(*count),
        );
        let _top = top_n(
            3,
            |input: &Input| input.key,
            |input: &Input| input.score,
            |snapshot: TopNSnapshot<u32, Input>| Output(snapshot.top_n.len() as u64),
        );
        let _top_by = top_n_by(
            3,
            |input: &Input| input.key,
            |input: &Input| input.score,
            |snapshot: TopNBySnapshot<u32, Input>| Output(snapshot.top_n.len() as u64),
        );
    }
}
