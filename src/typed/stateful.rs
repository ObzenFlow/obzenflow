// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed stateful helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::stateful::strategies::accumulators::{ConflateTyped, ReduceTyped};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::hash::Hash;

pub fn reduce<T, S, F>(initial: S, reduce_fn: F) -> ReduceTyped<T, S, F>
where
    T: DeserializeOwned + Send + Sync,
    S: Clone + Send + Sync + Debug + Serialize + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    ReduceTyped::new(initial, reduce_fn)
}

pub fn conflate<T, K, FKey>(key_fn: FKey) -> ConflateTyped<T, K, FKey>
where
    T: DeserializeOwned + Serialize + Send + Sync + Clone + Debug + TypedPayload,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
{
    ConflateTyped::new(key_fn)
}

