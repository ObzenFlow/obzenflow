// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-side effect wrappers (FLOWIP-120h).

mod guarded;

pub use guarded::{CircuitBreakerOutcome, Guarded, GuardedEffectExt};
