// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful handler components

pub mod traits;
pub mod typed;

pub use traits::{EffectfulStatefulHandler, EffectfulStatefulHandlerAdapter};
pub use traits::{
    StatefulOutputContext, StatefulTerminationKind, TerminalValidation, UnifiedStatefulHandler,
};
pub use typed::{
    StatefulEmission, TypedStatefulContribution, TypedStatefulDrainInvocation,
    TypedStatefulHandler, TypedStatefulHandlerAdapter, TypedStatefulInvocation,
};
