// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed predicate filtering for pure transform stages.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TypedTransformHandler;
use obzenflow_core::{StageOutputs, TypedPayload};
use std::fmt;
use std::marker::PhantomData;

/// A typed pass-or-drop predicate.
pub struct FilterTyped<T, F> {
    predicate: F,
    _type: PhantomData<fn(T) -> T>,
}

impl<T, F> FilterTyped<T, F> {
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _type: PhantomData,
        }
    }
}

impl<T, F> Clone for FilterTyped<T, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.predicate.clone())
    }
}

impl<T, F> fmt::Debug for FilterTyped<T, F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FilterTyped")
            .field("input_type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T, F> TypedTransformHandler for FilterTyped<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) -> bool + Send + Sync,
{
    type Input = T;
    type Output = StageOutputs<T>;

    fn process(&self, input: T) -> Result<Self::Output, HandlerError> {
        Ok(if (self.predicate)(&input) {
            StageOutputs::one(input)
        } else {
            StageOutputs::none()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::TypedFactSet;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Item(u32);

    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "filter.item";
    }

    #[test]
    fn passes_or_drops_typed_values() {
        let filter = FilterTyped::new(|item: &Item| item.0 > 3);
        assert_eq!(
            filter
                .process(Item(4))
                .expect("passes")
                .into_facts()
                .expect("lowers")
                .len(),
            1
        );
        assert!(filter
            .process(Item(2))
            .expect("drops")
            .into_facts()
            .expect("lowers")
            .is_empty());
    }
}
