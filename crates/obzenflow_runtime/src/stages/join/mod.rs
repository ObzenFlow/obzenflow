// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join stage implementation
//!
//! Joins combine events from two upstream sources (reference and stream) to produce
//! enriched output events.
//!
//! By default (`JoinReferenceMode::FiniteEof`), joins follow a hydrate-then-enrich model:
//! reference loads to EOF before stream processing begins. In `JoinReferenceMode::Live`,
//! the join processes stream events continuously while the reference side can keep
//! receiving updates.
//!
//! # Key Features
//! - Reference-first convention (no left/right confusion)
//! - Optional live reference updates (`JoinReferenceMode::Live`)
//! - Per-source EOF handling
//! - 3 join strategies: InnerJoin, LeftJoin, StrictJoin
//! - Type-safe key extraction via closures
//! - In-memory HashMap catalogs (<1GB per side)
//!
//! # Architecture
//! Joins are implemented as a dedicated stage type with custom FSM, not built on
//! top of StatefulHandler or TransformHandler, because joins have fundamentally
//! different semantics (immediate 1:1 emission, per-source EOF, distinct upstream roles).

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod strategies;
pub mod supervisor;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::{JoinReferenceView, TypedJoinHandler};
pub use builder::JoinBuilder;
pub use config::{JoinConfig, JoinReferenceMode};
pub use fsm::{JoinEvent, JoinState};
pub use handle::JoinHandle;

// Re-export join strategies
pub use strategies::{
    InnerJoin, InnerJoinBuilder, LeftJoin, LeftJoinBuilder, StrictJoin, StrictJoinBuilder,
};

use obzenflow_core::TypedPayload;
use std::hash::Hash;

/// Construct an inner join with a finite reference side.
pub fn inner<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl TypedJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
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

/// Construct an inner join with live reference updates enabled.
pub fn inner_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl TypedJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
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

/// Construct a left join with a finite reference side.
pub fn left<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl TypedJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
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

/// Construct a left join with live reference updates enabled.
pub fn left_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl TypedJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
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

/// Construct a strict join with a finite reference side.
pub fn strict<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl TypedJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
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

/// Construct a strict join with live reference updates enabled.
pub fn strict_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    stream_key: StreamKeyFn,
    join_fn: JoinFn,
) -> impl TypedJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
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

// Note: JoinSupervisor is NOT exported! It's an implementation detail.

#[cfg(test)]
mod helper_tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Reference {
        key: u32,
    }

    impl TypedPayload for Reference {
        const EVENT_TYPE: &'static str = "join.helper.reference";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Stream {
        key: u32,
    }

    impl TypedPayload for Stream {
        const EVENT_TYPE: &'static str = "join.helper.stream";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Output;

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "join.helper.output";
    }

    fn assert_reference_mode<H>(handler: &H, expected: JoinReferenceMode)
    where
        H: TypedJoinHandler,
    {
        assert_eq!(handler.reference_mode(), expected);
    }

    #[test]
    fn owner_helpers_preserve_finite_and_live_reference_modes() {
        let inner = inner(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Reference, _stream: Stream| Output,
        );
        assert_reference_mode(&inner, JoinReferenceMode::FiniteEof);

        let inner_live = inner_live(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Reference, _stream: Stream| Output,
        );
        assert_reference_mode(&inner_live, JoinReferenceMode::Live);

        let left = left(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Option<Reference>, _stream: Stream| Output,
        );
        assert_reference_mode(&left, JoinReferenceMode::FiniteEof);

        let left_live = left_live(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Option<Reference>, _stream: Stream| Output,
        );
        assert_reference_mode(&left_live, JoinReferenceMode::Live);

        let strict = strict(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Reference, _stream: Stream| Output,
        );
        assert_reference_mode(&strict, JoinReferenceMode::FiniteEof);

        let strict_live = strict_live(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Reference, _stream: Stream| Output,
        );
        assert_reference_mode(&strict_live, JoinReferenceMode::Live);
    }
}
