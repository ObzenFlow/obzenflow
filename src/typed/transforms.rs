// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed transform helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::transform::{FilterMapTyped, MapTyped, TryMapWithTyped};
use serde::{de::DeserializeOwned, Serialize};

pub fn map<T, O, F>(mapper: F) -> MapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> O + Send + Sync + Clone,
{
    MapTyped::new(mapper)
}

pub fn filter_map<T, O, F>(mapper: F) -> FilterMapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    FilterMapTyped::new(mapper)
}

pub fn try_map_with<T, O, F>(converter: F) -> TryMapWithTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Result<O, String> + Send + Sync + Clone,
{
    TryMapWithTyped::new(converter)
}
