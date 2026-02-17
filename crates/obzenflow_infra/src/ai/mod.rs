// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI infrastructure integrations.

#[cfg(feature = "ai-rig")]
pub mod rig;

mod token_estimation;

#[cfg(feature = "ai-tiktoken")]
mod tiktoken;

pub use token_estimation::estimator_for_model;

#[cfg(feature = "ai-tiktoken")]
pub use tiktoken::TiktokenEstimator;
