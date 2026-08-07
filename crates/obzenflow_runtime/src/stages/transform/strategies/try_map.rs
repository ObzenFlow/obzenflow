// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed fallible one-to-one mapping with fixed terminal-error semantics.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TypedTransformHandler;
use obzenflow_core::TypedPayload;
use std::fmt;
use std::marker::PhantomData;

/// A typed fallible mapping.
///
/// Converter errors become `HandlerError::Other`; the transform supervisor
/// owns error marking and error-journal routing. This helper has no envelope or
/// error-policy surface.
pub struct TryMapTyped<I, O, E, F> {
    converter: F,
    _types: PhantomData<fn(I) -> Result<O, E>>,
}

impl<I, O, E, F> TryMapTyped<I, O, E, F> {
    pub fn new(converter: F) -> Self {
        Self {
            converter,
            _types: PhantomData,
        }
    }
}

impl<I, O, E, F> Clone for TryMapTyped<I, O, E, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.converter.clone())
    }
}

impl<I, O, E, F> fmt::Debug for TryMapTyped<I, O, E, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TryMapTyped")
            .field("input_type", &std::any::type_name::<I>())
            .field("output_type", &std::any::type_name::<O>())
            .field("error_type", &std::any::type_name::<E>())
            .finish()
    }
}

impl<I, O, E, F> TypedTransformHandler for TryMapTyped<I, O, E, F>
where
    I: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    E: fmt::Display + 'static,
    F: Fn(I) -> Result<O, E> + Send + Sync,
{
    type Input = I;
    type Output = O;

    fn process(&self, input: I) -> Result<O, HandlerError> {
        (self.converter)(input)
            .map_err(|error| HandlerError::Other(format!("typed try-map failed: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Input(u32);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "try_map.input";
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Output(u32);

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "try_map.output";
    }

    #[test]
    fn success_returns_typed_output() {
        let mapper = TryMapTyped::new(|Input(value)| Ok::<_, &'static str>(Output(value + 1)));
        assert_eq!(mapper.process(Input(1)).expect("maps"), Output(2));
    }

    #[test]
    fn converter_error_has_fixed_handler_error() {
        let mapper = TryMapTyped::new(|Input(_)| Err::<Output, _>("invalid"));
        let error = mapper.process(Input(1)).expect_err("fails");
        assert!(matches!(
            error,
            HandlerError::Other(message) if message == "typed try-map failed: invalid"
        ));
    }
}
