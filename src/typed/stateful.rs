// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed stateful helper facades.

use obzenflow_core::{OneFactStageOutput, TypedPayload};
use obzenflow_runtime::stages::stateful::strategies::accumulators::{
    ConflateTyped, GroupByTyped, ReduceTyped, TopNByTyped,
};
use std::fmt::Debug;
use std::hash::Hash;

pub fn reduce<T, S, F>(initial: S, reduce_fn: F) -> ReduceTyped<T, S, F>
where
    T: TypedPayload + Send + Sync,
    S: Clone + Send + Sync + Debug + TypedPayload,
    F: Fn(&mut S, &T) + Send + Sync + Clone,
{
    ReduceTyped::new(initial, reduce_fn)
}

pub fn conflate<T, K, FKey>(key_fn: FKey) -> ConflateTyped<T, K, FKey>
where
    T: Send + Sync + Clone + Debug + TypedPayload,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
{
    ConflateTyped::new(key_fn)
}

pub fn group_by<T, K, S, O, FKey, FUpdate, FOutput>(
    key_fn: FKey,
    update_fn: FUpdate,
    output_fn: FOutput,
) -> GroupByTyped<T, K, S, O, FKey, FUpdate, FOutput>
where
    T: TypedPayload + Send + Sync,
    K: Hash + Eq + Clone + Debug + Send + Sync,
    S: Default + Send + Sync + Debug + Clone,
    O: OneFactStageOutput + Send + Sync,
    FKey: Fn(&T) -> K + Send + Sync + Clone,
    FUpdate: Fn(&mut S, &T) + Send + Sync + Clone,
    FOutput: Fn(&K, &S) -> O + Send + Sync + Clone,
{
    GroupByTyped::new(key_fn, update_fn, output_fn)
}

pub fn top_n_by<T, K, O, FKey, FScore, FOutput>(
    n: usize,
    key_fn: FKey,
    score_fn: FScore,
    output_fn: FOutput,
) -> TopNByTyped<T, K, O, FKey, FScore, FOutput>
where
    T: Clone + Debug + Send + Sync + TypedPayload + 'static,
    K: Hash + Eq + Clone + Debug + Send + Sync + 'static,
    O: OneFactStageOutput + Send + Sync + 'static,
    FKey: Fn(&T) -> K + Send + Sync + Clone + 'static,
    FScore: Fn(&T) -> f64 + Send + Sync + Clone + 'static,
    FOutput: Fn(obzenflow_runtime::stages::stateful::strategies::accumulators::TopNBySnapshot<K, T>) -> O
        + Send
        + Sync
        + Clone
        + 'static,
{
    TopNByTyped::new(n, key_fn, score_fn, output_fn)
}

pub use obzenflow_runtime::stages::stateful::strategies::accumulators::{
    TopNByEntry, TopNBySnapshot,
};
