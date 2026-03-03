// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared supervisor helpers
//!
//! This module contains free functions and small types extracted from the
//! per-stage supervisor event loops. Each helper is designed to be called
//! from any supervisor (transform, stateful, sink, join, source) without
//! requiring stage-specific trait bounds.

pub(crate) mod backpressure_drain;
pub(crate) mod control_resolution;
pub(crate) mod error_routing;
pub(crate) mod flow_context_factory;
pub(crate) mod forward_control_event;
pub(crate) mod lifecycle_actions;

#[cfg(test)]
mod tests;
