// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed stateful accumulator facades and their emission wrapper.

pub(crate) mod trace;
mod typed_facades;
pub mod wrapper;

pub use typed_facades::{
    ConflateTyped, GroupByTyped, ReduceTyped, TopNByEntry, TopNBySnapshot, TopNByTyped,
};
pub use wrapper::{StatefulWithEmission, WrapperState};
