// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned sink-delivery policy boundary (FLOWIP-115b).
//!
//! The runtime sees only `SinkDeliveryBoundary`. This module owns the middleware
//! policy onion hidden behind that seam: admission runs forward, the delivery
//! attempt runs once, and observation runs in reverse over the raw outcome,
//! mirroring the source (FLOWIP-115a) and effect (FLOWIP-120c) boundaries.

mod attempt;
mod boundary;
mod context;
mod contract;
mod disposition;
mod recovery;

use attempt::{run_once, run_retryable_attempt, RejectedSinkAttempt, RetryableSinkAttempt};
pub use boundary::PerSinkDeliveryPolicyBoundary;
pub use context::SinkPolicyCtx;
pub use contract::{SinkAdmission, SinkAdmissionGuard, SinkDeliveryPolicyOutcome, SinkPolicy};
use disposition::{handler_retry_after, sink_attempt_disposition};

#[cfg(test)]
mod tests;
