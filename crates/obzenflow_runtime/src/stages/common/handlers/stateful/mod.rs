// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful handler components

pub mod traits;
pub mod wrapper;

pub use traits::{EffectfulStatefulHandler, EffectfulStatefulHandlerAdapter, StatefulHandler};
pub use traits::{
    StatefulOutputContext, StatefulTerminationKind, TerminalValidation, UnifiedStatefulHandler,
};
pub use wrapper::{StatefulHandlerExt, StatefulHandlerWithEmission};
