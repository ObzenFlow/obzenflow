// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class typed stateful accumulators and convenience constructors.
//!
//! ## Choosing when to emit
//!
//! Accumulators describe the fold; an [`EmissionStrategy`] decides when its
//! current result is emitted. Conflation can publish the latest value for each
//! key after every input. A custom cadence composes through `with_emission`:
//!
//! ```
//! use obzenflow::stages::stateful::{self, EmissionStrategy};
//! use std::time::Duration;
//! # use obzenflow::schema::TypedPayload;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct Reading { sensor: String }
//! # impl TypedPayload for Reading { const EVENT_TYPE: &'static str = "sensor.reading"; }
//! # #[derive(Clone, Debug, Default, Serialize, Deserialize)] struct Count { total: u64 }
//! # impl TypedPayload for Count { const EVENT_TYPE: &'static str = "sensor.count"; }
//!
//! #[derive(Clone, Debug)]
//! struct EveryTwoReadings;
//!
//! impl EmissionStrategy for EveryTwoReadings {
//!     fn should_emit(&self, events_seen: u64, _elapsed: Option<Duration>) -> bool {
//!         events_seen >= 2
//!     }
//! }
//!
//! let latest = stateful::conflate(|reading: &Reading| reading.sensor.clone())
//!     .emit_always();
//! let counts = stateful::reduce(Count::default(), |count: &mut Count, _: &Reading| {
//!     count.total += 1;
//! }).with_emission(EveryTwoReadings);
//! ```
//!
//! The wrapper tracks events since the previous emission. The custom policy
//! above retains the accumulated count; [`EveryN`] supplies this cadence as a
//! built-in strategy.
//!
//! ## Custom accumulators
//!
//! Implement [`Accumulator`] to own the state and output projection, then attach
//! an emission strategy with [`StatefulWithEmission::new`]:
//!
//! ```
//! use obzenflow::stages::stateful::{Accumulator, OnEOF, StatefulWithEmission};
//! # use obzenflow::schema::TypedPayload;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct Reading { sensor: String }
//! # impl TypedPayload for Reading { const EVENT_TYPE: &'static str = "sensor.reading"; }
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct Count { total: u64 }
//! # impl TypedPayload for Count { const EVENT_TYPE: &'static str = "sensor.count"; }
//!
//! #[derive(Clone, Debug)]
//! struct CountReadings;
//!
//! impl Accumulator for CountReadings {
//!     type State = u64;
//!     type Input = Reading;
//!     type Output = Count;
//!
//!     fn initial_state(&self) -> u64 { 0 }
//!     fn accumulate(&self, total: &mut u64, _: Reading) { *total += 1; }
//!     fn outputs(&self, total: &u64) -> Vec<Count> {
//!         vec![Count { total: *total }]
//!     }
//! }
//!
//! let counts = StatefulWithEmission::new(CountReadings, OnEOF);
//! ```
//!
//! The resulting value is a [`TypedStatefulHandler`] suitable for a
//! `stateful!(Reading -> Count => counts)` stage.

pub use obzenflow_runtime::stages::stateful::{
    conflate, group_by, reduce, top_n, top_n_by, Accumulator, Conflate, ConflateState,
    ConflateTyped, EmissionStrategy, EmitAlways, EveryN, GroupBy, GroupByState, GroupByTyped,
    OnEOF, Reduce, ReduceState, ReduceTyped, StatefulWithEmission, TimeWindow, TopN, TopNBy,
    TopNByEntry, TopNBySnapshot, TopNByState, TopNByTyped, TopNEntry, TopNSnapshot, TopNState,
    TopNTyped, WrapperState,
};

pub use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, StatefulEmission, StatefulOutputContext, StatefulTerminationKind,
    TerminalValidation, TypedStatefulHandler,
};
