// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed one-to-one mapping for pure transform stages.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TypedTransformHandler;
use obzenflow_core::TypedPayload;
use std::fmt;
use std::marker::PhantomData;

/// A pure typed one-to-one mapping.
pub struct MapTyped<I, O, F> {
    mapper: F,
    _types: PhantomData<fn(I) -> O>,
}

impl<I, O, F> MapTyped<I, O, F> {
    pub fn new(mapper: F) -> Self {
        Self {
            mapper,
            _types: PhantomData,
        }
    }
}

impl<I, O, F> Clone for MapTyped<I, O, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.mapper.clone())
    }
}

impl<I, O, F> fmt::Debug for MapTyped<I, O, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MapTyped")
            .field("input_type", &std::any::type_name::<I>())
            .field("output_type", &std::any::type_name::<O>())
            .finish()
    }
}

impl<I, O, F> TypedTransformHandler for MapTyped<I, O, F>
where
    I: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    F: Fn(I) -> O + Send + Sync,
{
    type Input = I;
    type Output = O;

    fn process(&self, input: I) -> Result<O, HandlerError> {
        Ok((self.mapper)(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Input(u32);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "map.input";
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Output(u32);

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "map.output";
    }

    #[test]
    fn maps_typed_values() {
        let mapper = MapTyped::new(|Input(value)| Output(value * 2));
        assert_eq!(mapper.process(Input(4)).expect("maps"), Output(8));
    }
}
