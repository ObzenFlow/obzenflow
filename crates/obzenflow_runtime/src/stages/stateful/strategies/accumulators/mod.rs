// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class typed stateful accumulation strategies.
//!
//! Accumulators define what state is retained and which typed values are
//! projected. Emission strategies independently define when those projections
//! cross a stateful stage boundary.

pub mod conflate;
pub mod group_by;
pub mod reduce;
pub mod top_n;
pub mod top_n_by;
pub(crate) mod trace;
pub mod wrapper;

pub use conflate::{Conflate, ConflateState, ConflateTyped};
pub use group_by::{GroupBy, GroupByState, GroupByTyped};
pub use reduce::{Reduce, ReduceState, ReduceTyped};
pub use top_n::{TopN, TopNEntry, TopNSnapshot, TopNState, TopNTyped};
pub use top_n_by::{TopNBy, TopNByEntry, TopNBySnapshot, TopNByState, TopNByTyped};
pub use wrapper::{Accumulator, StatefulWithEmission, WrapperState};
