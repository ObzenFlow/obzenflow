// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-effect policy composition at the effect boundary (FLOWIP-120c).
//!
//! The public policy contract and attachment model are kept separate from the
//! boundary execution and retry coordination machinery. This keeps policy
//! authoring concerns small while preserving the structural admission/observe
//! onion at the live effect boundary.

mod attachment;
mod boundary;
mod contract;
mod disposition;
mod recovery;

pub use attachment::{effect_policy_from_middleware, EffectPolicyAttachment};
pub use boundary::PerEffectPolicyBoundary;
pub use contract::{EffectAttemptOutcome, EffectPolicy, EventAwareEffectPolicy, PolicyAdmission};

#[cfg(test)]
mod tests;
