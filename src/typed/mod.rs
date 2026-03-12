// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Canonical typed helper facades for stage authoring.
//!
//! These modules provide a generic-free, namespace-and-verb surface for
//! constructing typed stage handlers. They are intended to be used on the
//! right-hand side of typed stage macros.

pub mod joins;
pub mod sinks;
pub mod sources;
pub mod stateful;
pub mod transforms;
