// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::chain_event::ChainEvent;
use std::sync::Arc;
use std::time::Duration;

pub(super) type FailureClassifier = Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> bool + Send + Sync>;
pub(super) type FailureClassificationClassifier =
    Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> FailureClassification + Send + Sync>;

/// Policy for how the breaker should treat errors whose `ErrorKind` is
/// `Unknown` or `None` (legacy/unclassified cases).
#[derive(Debug, Clone, Copy)]
pub enum UnknownErrorKindPolicy {
    /// Treat Unknown/None as infra failures for breaker purposes.
    TreatAsInfraFailure,
    /// Do not count Unknown/None toward breaker thresholds.
    IgnoreForBreaker,
}

/// Rich classification used by the circuit breaker to decide whether to retry,
/// count toward opening, or ignore outcomes.
#[derive(Debug, Clone, PartialEq)]
pub enum FailureClassification {
    Success,
    TransientFailure,
    PermanentFailure,
    RateLimited(Duration),
    PartialSuccess { failed_ratio: f32 },
}

/// Policy knobs for how `FailureClassification` affects breaker accounting.
#[derive(Debug, Clone)]
pub struct FailureClassificationPolicy {
    pub partial_failure_threshold: f32,
    pub rate_limited_counts_as_failure: bool,
}

impl Default for FailureClassificationPolicy {
    fn default() -> Self {
        Self {
            partial_failure_threshold: 0.5,
            rate_limited_counts_as_failure: false,
        }
    }
}
