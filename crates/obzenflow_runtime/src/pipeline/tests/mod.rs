// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline component and lifecycle scenarios.
//!
//! Builder and handle unit tests are loaded by their owning modules from this
//! directory to retain access to private helpers without exposing production APIs.

mod fsm;
mod metrics;
mod shutdown;
mod startup;
mod supervisor;
pub(in crate::pipeline) mod support;
