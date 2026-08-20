// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class typed stateful accumulators and convenience constructors.

pub use obzenflow_runtime::stages::stateful::{
    conflate, group_by, reduce, top_n, top_n_by, Accumulator, Conflate, ConflateState,
    ConflateTyped, GroupBy, GroupByState, GroupByTyped, Reduce, ReduceState, ReduceTyped,
    StatefulWithEmission, TopN, TopNBy, TopNByEntry, TopNBySnapshot, TopNByState, TopNByTyped,
    TopNEntry, TopNSnapshot, TopNState, TopNTyped, WrapperState,
};
