// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed optional mapping for pure transform stages.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TypedTransformHandler;
use obzenflow_core::{StageOutputs, TypedPayload};
use std::fmt;
use std::marker::PhantomData;

/// A typed mapping that deliberately emits zero or one output fact.
pub struct FilterMapTyped<I, O, F> {
    mapper: F,
    _types: PhantomData<fn(I) -> O>,
}

impl<I, O, F> FilterMapTyped<I, O, F> {
    pub fn new(mapper: F) -> Self {
        Self {
            mapper,
            _types: PhantomData,
        }
    }
}

impl<I, O, F> Clone for FilterMapTyped<I, O, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.mapper.clone())
    }
}

impl<I, O, F> fmt::Debug for FilterMapTyped<I, O, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FilterMapTyped")
            .field("input_type", &std::any::type_name::<I>())
            .field("output_type", &std::any::type_name::<O>())
            .finish()
    }
}

impl<I, O, F> TypedTransformHandler for FilterMapTyped<I, O, F>
where
    I: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    F: Fn(I) -> Option<O> + Send + Sync,
{
    type Input = I;
    type Output = StageOutputs<O>;

    fn process(&self, input: I) -> Result<Self::Output, HandlerError> {
        Ok(match (self.mapper)(input) {
            Some(output) => StageOutputs::one(output),
            None => StageOutputs::none(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::TypedFactSet;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Input(u32);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "filter_map.input";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Output(u32);

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "filter_map.output";
    }

    #[test]
    fn maps_some_or_deliberately_emits_no_fact() {
        let mapper = FilterMapTyped::new(|Input(value)| (value > 3).then_some(Output(value * 2)));
        let emitted = mapper
            .process(Input(4))
            .expect("maps")
            .into_facts()
            .expect("lowers");
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].payload, serde_json::json!(8));
        assert!(mapper
            .process(Input(2))
            .expect("filters")
            .into_facts()
            .expect("lowers")
            .is_empty());
    }
}
