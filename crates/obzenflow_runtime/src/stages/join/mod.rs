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
//! receiving updates. `JoinReferenceMode::FiniteThenLive` hydrates the batch catalog,
//! enriches the batch stream, then runs live over every input.
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
pub(crate) mod phase;
pub mod strategies;
pub mod supervisor;
#[cfg(test)]
mod transition_tests;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::{
    AsOf, AsOfJoin, AsOfReferenceView, JoinReferenceView, TypedAsOfJoinHandler, TypedJoinHandler,
};
pub use builder::JoinBuilder;
pub use config::{
    JoinCatalogKind, JoinConfig, JoinReferenceMode, LiveStream, PhaseWitness, PhaseWitnesses,
};
pub use fsm::{JoinEvent, JoinState};
pub use handle::JoinHandle;

// Re-export join strategies
pub use strategies::{
    InnerJoin, InnerJoinBuilder, LeftJoin, LeftJoinBuilder, StrictJoin, StrictJoinBuilder,
};

use obzenflow_core::{OneFactStageOutput, TypedPayload};
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

/// Construct an inner join that hydrates the batch catalog, enriches the batch
/// stream, then runs live over every input.
pub fn inner_then_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
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
        .then_live()
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

/// Construct a left join that hydrates the batch catalog, enriches the batch
/// stream, then runs live over every input.
pub fn left_then_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
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
        .then_live()
        .build(join_fn)
}

/// Construct a point-in-time left join over a versioned catalog.
///
/// The catalog keeps every version of each key, by the time it took effect,
/// and loads them all before the first stream row. Each stream row is joined
/// with the version of its key whose effective time is the latest at or before
/// the row's `as_of` time, and the join function receives that selection as an
/// [`AsOf`] and returns exactly one output.
///
/// Both time accessors return one type, which can be any totally ordered `Copy`
/// type, such as a `chrono::NaiveDateTime`, a date or a sequence number. Text
/// does not qualify, because timestamps written in two formats compare as text
/// and mis-order. Whether a found version is too old is the join function's
/// check:
///
/// ```
/// # use obzenflow_core::{StageOutputFacts, TypedPayload};
/// # use serde::{Deserialize, Serialize};
/// use chrono::{NaiveDateTime, TimeDelta};
/// use obzenflow_runtime::stages::join::{left_as_of, AsOf};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct FxRate { currency: String, effective_at: NaiveDateTime, rate: f64 }
/// # impl TypedPayload for FxRate { const EVENT_TYPE: &'static str = "doc.fx_rate"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Payment { currency: String, booked_at: NaiveDateTime }
/// # impl TypedPayload for Payment { const EVENT_TYPE: &'static str = "doc.payment"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Priced { rate: f64 }
/// # impl TypedPayload for Priced { const EVENT_TYPE: &'static str = "doc.priced"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct RateStale { currency: String }
/// # impl TypedPayload for RateStale { const EVENT_TYPE: &'static str = "doc.rate_stale"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct RateMissing { currency: String }
/// # impl TypedPayload for RateMissing { const EVENT_TYPE: &'static str = "doc.rate_missing"; }
///
/// #[derive(Clone, Debug, StageOutputFacts)]
/// enum Pricing {
///     Priced(Priced),
///     RateStale(RateStale),
///     RateMissing(RateMissing),
/// }
///
/// let price = left_as_of(
///     |rate: &FxRate| rate.currency.clone(),
///     |rate: &FxRate| rate.effective_at,
///     |payment: &Payment| payment.currency.clone(),
///     |payment: &Payment| payment.booked_at,
///     |rate: AsOf<FxRate>, payment: Payment| match rate {
///         AsOf::Found(rate) if payment.booked_at - rate.effective_at > TimeDelta::hours(2) => {
///             Pricing::RateStale(RateStale { currency: payment.currency })
///         }
///         AsOf::Found(rate) => Pricing::Priced(Priced { rate: rate.rate }),
///         AsOf::BeforeFirstVersion | AsOf::NoVersions => {
///             Pricing::RateMissing(RateMissing { currency: payment.currency })
///         }
///     },
/// );
/// ```
///
/// A time written as text does not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::join::{left_as_of, AsOf};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct FxRate { currency: String, effective_at: String }
/// # impl TypedPayload for FxRate { const EVENT_TYPE: &'static str = "doc.fx_rate"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Payment { currency: String, booked_at: String }
/// # impl TypedPayload for Payment { const EVENT_TYPE: &'static str = "doc.payment"; }
///
/// let price = left_as_of(
///     |rate: &FxRate| rate.currency.clone(),
///     |rate: &FxRate| rate.effective_at.clone(),
///     |payment: &Payment| payment.currency.clone(),
///     |payment: &Payment| payment.booked_at.clone(),
///     |_rate: AsOf<FxRate>, payment: Payment| payment,
/// );
/// ```
///
/// Nor does a time with no total order, such as `f64`:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::join::{left_as_of, AsOf};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct FxRate { currency: String, effective_at: f64 }
/// # impl TypedPayload for FxRate { const EVENT_TYPE: &'static str = "doc.fx_rate"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Payment { currency: String, booked_at: f64 }
/// # impl TypedPayload for Payment { const EVENT_TYPE: &'static str = "doc.payment"; }
///
/// let price = left_as_of(
///     |rate: &FxRate| rate.currency.clone(),
///     |rate: &FxRate| rate.effective_at,
///     |payment: &Payment| payment.currency.clone(),
///     |payment: &Payment| payment.booked_at,
///     |_rate: AsOf<FxRate>, payment: Payment| payment,
/// );
/// ```
///
/// Nor do two accessors that return different time types:
///
/// ```compile_fail,E0308
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use chrono::{NaiveDate, NaiveDateTime};
/// use obzenflow_runtime::stages::join::{left_as_of, AsOf};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct FxRate { currency: String, effective_at: NaiveDateTime }
/// # impl TypedPayload for FxRate { const EVENT_TYPE: &'static str = "doc.fx_rate"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Payment { currency: String, booked_on: NaiveDate }
/// # impl TypedPayload for Payment { const EVENT_TYPE: &'static str = "doc.payment"; }
///
/// let price = left_as_of(
///     |rate: &FxRate| rate.currency.clone(),
///     |rate: &FxRate| rate.effective_at,
///     |payment: &Payment| payment.currency.clone(),
///     |payment: &Payment| payment.booked_on,
///     |_rate: AsOf<FxRate>, payment: Payment| payment,
/// );
/// ```
///
/// A point-in-time join has no live mode:
///
/// ```compile_fail,E0599
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::join::{AsOf, LeftJoinBuilder};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct FxRate { currency: String, effective_at: u64 }
/// # impl TypedPayload for FxRate { const EVENT_TYPE: &'static str = "doc.fx_rate"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Payment { currency: String, booked_at: u64 }
/// # impl TypedPayload for Payment { const EVENT_TYPE: &'static str = "doc.payment"; }
///
/// let price = LeftJoinBuilder::<FxRate, Payment, Payment>::new()
///     .catalog_key(|rate: &FxRate| rate.currency.clone())
///     .effective_at(|rate: &FxRate| rate.effective_at)
///     .stream_key(|payment: &Payment| payment.currency.clone())
///     .as_of(|payment: &Payment| payment.booked_at)
///     .live()
///     .build(|_rate: AsOf<FxRate>, payment: Payment| payment);
/// ```
pub fn left_as_of<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn>(
    catalog_key: CatalogKeyFn,
    effective_at: EffectiveAtFn,
    stream_key: StreamKeyFn,
    as_of: AsOfFn,
    join_fn: JoinFn,
) -> AsOfJoin<
    impl TypedAsOfJoinHandler<Reference = C, Stream = S, Output = E> + Clone + std::fmt::Debug,
>
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: OneFactStageOutput + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    T: Ord + Copy + Send + Sync + std::fmt::Debug + 'static,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone + 'static,
    EffectiveAtFn: Fn(&C) -> T + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone + 'static,
    AsOfFn: Fn(&S) -> T + Send + Sync + Clone + 'static,
    JoinFn: Fn(AsOf<C>, S) -> E + Send + Sync + Clone,
{
    LeftJoinBuilder::<C, S, E>::new()
        .catalog_key(catalog_key)
        .effective_at(effective_at)
        .stream_key(stream_key)
        .as_of(as_of)
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

/// Construct a strict join that hydrates the batch catalog, enriches the batch
/// stream, then runs live over every input.
pub fn strict_then_live<C, S, E, K, CatalogKeyFn, StreamKeyFn, JoinFn>(
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
        .then_live()
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

        let inner_then_live = inner_then_live(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Reference, _stream: Stream| Output,
        );
        assert_reference_mode(&inner_then_live, JoinReferenceMode::FiniteThenLive);

        let left_then_live = left_then_live(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Option<Reference>, _stream: Stream| Output,
        );
        assert_reference_mode(&left_then_live, JoinReferenceMode::FiniteThenLive);

        let strict_then_live = strict_then_live(
            |reference: &Reference| reference.key,
            |stream: &Stream| stream.key,
            |_reference: Reference, _stream: Stream| Output,
        );
        assert_reference_mode(&strict_then_live, JoinReferenceMode::FiniteThenLive);
    }
}
