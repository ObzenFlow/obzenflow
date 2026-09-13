// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Calculates a composite group's status from its member stages.
//!
//! Supply membership through [`CompositeDefinition`] and apply stage events in
//! journal order. These shared rules let consumers such as Studio agree on the
//! group's status without each implementing their own interpretation.

mod projection;

pub use projection::{
    CompositeDefinition, CompositeLifecycleProjection, CompositeProjectionError, CompositeStatus,
};
