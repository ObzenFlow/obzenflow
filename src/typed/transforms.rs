// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed transform helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::transform::{FilterMapTyped, FilterTyped, MapTyped, TryMapTyped};
use std::fmt;

pub fn map<T, O, F>(mapper: F) -> MapTyped<T, O, F>
where
    T: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    F: Fn(T) -> O + Send + Sync + Clone,
{
    MapTyped::new(mapper)
}

pub fn filter_map<T, O, F>(mapper: F) -> FilterMapTyped<T, O, F>
where
    T: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    FilterMapTyped::new(mapper)
}

pub fn filter<T, F>(predicate: F) -> FilterTyped<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) -> bool + Send + Sync + Clone,
{
    FilterTyped::new(predicate)
}

pub fn try_map<I, O, E, F>(converter: F) -> TryMapTyped<I, O, E, F>
where
    I: TypedPayload + Send + Sync + 'static,
    O: TypedPayload + Send + Sync + 'static,
    E: fmt::Display + 'static,
    F: Fn(I) -> Result<O, E> + Send + Sync + Clone,
{
    TryMapTyped::new(converter)
}
