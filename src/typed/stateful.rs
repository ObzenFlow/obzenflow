// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed stateful helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::stateful::strategies::accumulators::{
    ConflateTyped, GroupByTyped, ReduceTyped, TopNByTyped,
};
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

pub fn group_by<T, K, S, FKey, FUpdate>(
    key_fn: FKey,
    update_fn: FUpdate,
) -> GroupByTyped<T, K, S, FKey, FUpdate>
where
    T: DeserializeOwned + Send + Sync,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    S: Default + Serialize + TypedPayload + Send + Sync + Debug + Clone,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone,
{
    GroupByTyped::new(key_fn, update_fn)
}

pub fn top_n_by<T, K, FKey, FScore>(
    n: usize,
    key_fn: FKey,
    score_fn: FScore,
) -> TopNByTyped<T, K, FKey, FScore>
where
    T: DeserializeOwned + Serialize + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
{
    TopNByTyped::new(n, key_fn, score_fn)
}
