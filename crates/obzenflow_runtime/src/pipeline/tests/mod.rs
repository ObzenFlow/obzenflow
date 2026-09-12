// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline component and lifecycle scenarios.
//!
//! Builder and handle unit tests are loaded by their owning modules from this
//! directory to retain access to private helpers without exposing production APIs.
//! Journal scenarios accept an outer factory and are individually invoked by Infra.

pub(crate) mod admission;
pub(crate) mod fsm;
pub(crate) mod metrics;
pub(crate) mod shutdown;
pub(crate) mod startup;
pub(crate) mod supervisor;
pub(in crate::pipeline) mod support;
