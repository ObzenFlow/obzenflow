// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed join helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handlers::JoinHandler;
use obzenflow_runtime::stages::join::{InnerJoinBuilder, LeftJoinBuilder, StrictJoinBuilder};
use obzenflow_runtime::typing::JoinTyping;
use std::hash::Hash;

pub fn inner<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl JoinHandler + JoinTyping<Reference = C, Stream = S, Output = E> + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    JoinFn: Fn(C, S) -> E + Send + Sync + Clone,
{
    InnerJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .stream_key(stream_key)
        .build(join_fn)
}

/// Inner join with live reference updates enabled.
///
/// This is the right choice when your reference side is unbounded (e.g. HTTP ingestion),
/// or when you want reference updates to be applied concurrently with stream processing.
pub fn inner_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl JoinHandler + JoinTyping<Reference = C, Stream = S, Output = E> + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    JoinFn: Fn(C, S) -> E + Send + Sync + Clone,
{
    InnerJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .stream_key(stream_key)
        .live()
        .build(join_fn)
}

pub fn left<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl JoinHandler + JoinTyping<Reference = C, Stream = S, Output = E> + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    JoinFn: Fn(Option<C>, S) -> E + Send + Sync + Clone,
{
    LeftJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .stream_key(stream_key)
        .build(join_fn)
}

/// Left join with live reference updates enabled.
pub fn left_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl JoinHandler + JoinTyping<Reference = C, Stream = S, Output = E> + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    JoinFn: Fn(Option<C>, S) -> E + Send + Sync + Clone,
{
    LeftJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .stream_key(stream_key)
        .live()
        .build(join_fn)
}

pub fn strict<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl JoinHandler + JoinTyping<Reference = C, Stream = S, Output = E> + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    JoinFn: Fn(C, S) -> E + Send + Sync + Clone,
{
    StrictJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .stream_key(stream_key)
        .build(join_fn)
}

/// Strict join with live reference updates enabled.
pub fn strict_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl JoinHandler + JoinTyping<Reference = C, Stream = S, Output = E> + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    JoinFn: Fn(C, S) -> E + Send + Sync + Clone,
{
    StrictJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .stream_key(stream_key)
        .live()
        .build(join_fn)
}
