// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class typed stateful accumulators and convenience constructors.
//!
//! ## Choosing when to emit
//!
//! Accumulators describe the fold, and `.emit(...)` takes the
//! [`EmissionStrategy`] that decides when the result is emitted, either a value
//! from [`emit`] or a strategy of your own. `group_by` and `conflate` publish
//! the groups or keys updated since the previous emission, and `.all_groups()`
//! or `.all_keys()` publishes all of them:
//!
//! ```
//! use obzenflow::stages::stateful::{self, emit, EmissionStrategy};
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
//!     .emit(emit::every(Duration::from_secs(1)));
//! let every_sensor = stateful::conflate(|reading: &Reading| reading.sensor.clone())
//!     .emit(emit::always)
//!     .all_keys();
//! let counts = stateful::reduce(Count::default(), |count: &mut Count, _: &Reading| {
//!     count.total += 1;
//! })
//! .emit(EveryTwoReadings);
//! ```
//!
//! The wrapper tracks events since the previous emission. The custom policy
//! above retains the accumulated count; [`emit::every_n`] supplies this cadence
//! as a built-in strategy.
//!
//! ## Custom accumulators
//!
//! Implement [`Accumulator`] to own the state and output projection, then attach
//! an emission strategy with [`StatefulWithEmission::new`]:
//!
//! ```
//! use obzenflow::stages::stateful::{emit, Accumulator, StatefulWithEmission};
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
//! let counts = StatefulWithEmission::new(CountReadings, emit::on_eof);
//! ```
//!
//! The resulting value is a [`TypedStatefulHandler`] suitable for a
//! `stateful!(Reading -> Count => counts)` stage.

pub use obzenflow_runtime::stages::stateful::{
    conflate, emit, group_by, reduce, top_n, top_n_by, Accumulator, Conflate, ConflateState,
    ConflateTyped, EmissionStrategy, EmitAlways, EveryInterval, EveryN, GroupBy, GroupByState,
    GroupByTyped, OnEOF, Reduce, ReduceState, ReduceTyped, StatefulWithEmission, TimeWindow, TopN,
    TopNBy, TopNByEntry, TopNBySnapshot, TopNByState, TopNByTyped, TopNEntry, TopNSnapshot,
    TopNState, TopNTyped, WrapperState,
};

pub use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, StatefulEmission, StatefulOutputContext, StatefulTerminationKind,
    TerminalValidation, TypedStatefulHandler,
};
