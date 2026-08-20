// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform stage implementation
//!
//! Transforms are the workhorses of the pipeline - they process events
//! from upstream stages and emit transformed events downstream.
//!
//! Key features:
//! - Start processing immediately (no waiting)
//! - Ordered sequential processing (one event at a time)
//! - Owned handler storage (no stage-level locks)
//! - Control event strategies for customizing behavior
//! - Can filter (0 outputs), pass through (1 output), or expand (N outputs)

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod strategies;
pub mod supervisor;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::{EffectfulTransformHandler, TypedTransformHandler};
pub use builder::TransformBuilder;
pub use config::TransformConfig;
pub use fsm::{TransformEvent, TransformState};
pub use handle::{TransformHandle, TransformHandleExt};

// Re-export transform strategies for ergonomic imports (FLOWIP-080h)
pub use strategies::{
    ChunkByBudgetBuilder, ChunkByBudgetTyped, FilterMapTyped, FilterTyped, MapTyped, TryMapTyped,
};

use obzenflow_core::TypedPayload;
use std::fmt;

/// Construct a pure typed one-to-one mapping.
pub fn map<T, O, F>(mapper: F) -> MapTyped<T, O, F>
where
    T: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    F: Fn(T) -> O + Send + Sync + Clone,
{
    MapTyped::new(mapper)
}

/// Construct a typed mapping that deliberately emits zero or one output fact.
pub fn filter_map<T, O, F>(mapper: F) -> FilterMapTyped<T, O, F>
where
    T: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    FilterMapTyped::new(mapper)
}

/// Construct a typed pass-or-drop predicate.
pub fn filter<T, F>(predicate: F) -> FilterTyped<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) -> bool + Send + Sync + Clone,
{
    FilterTyped::new(predicate)
}

/// Construct a typed fallible mapping with fixed terminal-error semantics.
pub fn try_map<I, O, E, F>(converter: F) -> TryMapTyped<I, O, E, F>
where
    I: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    E: fmt::Display + 'static,
    F: Fn(I) -> Result<O, E> + Send + Sync + Clone,
{
    TryMapTyped::new(converter)
}

// Re-export control strategies for convenience
pub use crate::stages::common::control_strategies::{
    BackoffStrategy, CompositeStrategy, JonestownSignalStrategy, SignalDecision, SignalGate,
};

// Note: TransformSupervisor is NOT exported! It's an implementation detail.

#[cfg(test)]
mod helper_tests {
    use super::*;
    use crate::stages::common::handler_error::HandlerError;
    use obzenflow_core::TypedFactSet;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Input(u32);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "transform.helper.input";
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Output(u32);

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "transform.helper.output";
    }

    #[test]
    fn owner_helpers_construct_every_supported_transform() {
        let mapped = map(|Input(value)| Output(value + 1))
            .process(Input(1))
            .expect("map succeeds");
        assert_eq!(mapped, Output(2));

        let emitted = filter_map(|Input(value)| (value > 0).then_some(Output(value)))
            .process(Input(1))
            .expect("filter-map succeeds")
            .into_facts()
            .expect("filter-map lowers");
        assert_eq!(emitted.len(), 1);

        let filtered = filter(|input: &Input| input.0 > 0)
            .process(Input(0))
            .expect("filter succeeds")
            .into_facts()
            .expect("filter lowers");
        assert!(filtered.is_empty());

        let error = try_map(|Input(_)| Err::<Output, _>("invalid"))
            .process(Input(1))
            .expect_err("try-map fails");
        assert!(matches!(
            error,
            HandlerError::Other(message) if message == "typed try-map failed: invalid"
        ));
    }
}
