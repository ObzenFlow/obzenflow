// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-composite monitor-supervisor (FLOWIP-128a).
//!
//! Each first-class composite is materialised with its own `CompositeSupervisor`
//! beside the member stage supervisors. It is a monitor: it watches its members'
//! `StageLifecycle` through its own system-journal subscription, folds them via
//! the pure `CompositeRollup`, and authors the composite's own
//! `CompositeLifecycle` facts. It owns the composite's lifecycle truth and drives
//! no execution; the PipelineSupervisor stays composite-blind.

pub mod builder;
pub mod fsm;
pub mod rollup;
pub mod supervisor;

pub use builder::{CompositeSupervisorBuilder, CompositeSupervisorHandle};
pub use rollup::CompositeRollup;
