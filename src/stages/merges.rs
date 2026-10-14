// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework merge strategies, named in `merge!`'s `=>` slot.
//!
//! A merge runs no author code, so this is a closed set: `then_live` passes on
//! every batch producer's rows, up to each one's natural EOF, before any live
//! producer's.

pub use obzenflow_runtime::stages::merges::then_live;
