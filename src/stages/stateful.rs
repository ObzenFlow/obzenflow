// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class typed stateful accumulators and convenience constructors.

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
