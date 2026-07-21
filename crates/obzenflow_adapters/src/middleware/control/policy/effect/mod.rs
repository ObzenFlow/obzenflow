// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-effect policy composition at the effect boundary (FLOWIP-120c).
//!
//! One policy instance guards one protected dependency, which at the effect
//! boundary means one declared effect. Policies compose as brackets around
//! the physical effect operation: admission runs in declaration order and may
//! await (a rate limiter awaits its permit instead of blocking the worker),
//! and every policy that admitted observes how the attempt ended on the way
//! out, whichever arm ended it, so lifecycle finalization is structural
//! rather than a hook each middleware must remember (gap G8).
//!
//! Module shape: [`contract`] holds the policy authoring traits and outcome
//! types, [`attachment`] the opaque attachment plan and the generic
//! middleware bridge, and [`boundary`] the runtime boundary that routes
//! plain and recovering chains. Recovery decisions themselves live with the
//! circuit breaker (FLOWIP-115h AR1), never here.

mod attachment;
mod boundary;
mod contract;

#[cfg(test)]
mod tests;

pub use attachment::EffectPolicyAttachment;
pub use boundary::PerEffectPolicyBoundary;
pub use contract::{EffectAttemptOutcome, EffectPolicy, EventAwareEffectPolicy, PolicyAdmission};
