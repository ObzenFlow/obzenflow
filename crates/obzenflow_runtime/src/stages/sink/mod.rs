// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink stage implementations
//!
//! Sinks are journal-based stages that process events and write delivery facts.

pub mod journal_sink;
pub mod typed;

pub use typed::{FallibleSinkTyped, SinkTyped};
