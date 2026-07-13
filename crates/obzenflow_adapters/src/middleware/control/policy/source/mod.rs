// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned source policy boundary (FLOWIP-115a).
//!
//! The runtime sees only `SourceBoundary`. This module owns the middleware
//! policy onion hidden behind that seam. Public authoring contracts are kept
//! separate from the physical-attempt and logical-recovery machinery.

mod attempt;
mod boundary;
mod context;
mod contract;
mod disposition;
mod execution;
mod recovery;

pub use boundary::PerSourcePolicyBoundary;
pub use context::SourcePolicyCtx;
pub use contract::{
    SourceAdmission, SourceAdmissionGuard, SourceAfterPoll, SourceBatchFacts, SourcePolicy,
    SourcePollOutcome,
};

#[cfg(test)]
use disposition::source_attempt_disposition;

#[cfg(test)]
mod tests;
