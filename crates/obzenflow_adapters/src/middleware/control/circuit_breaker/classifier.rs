// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime::effects::EffectError;
use std::sync::Arc;
use std::time::Duration;

pub(in crate::middleware::control) type FailureClassificationClassifier =
    Arc<dyn Fn(&EffectError) -> FailureHealth + Send + Sync>;

/// Optional advanced health override for one executed effect failure.
///
/// Recovery eligibility is derived independently from the typed
/// [`EffectError`], so this value can only change breaker health accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureHealth {
    CountedFailure,
    Ignored,
}

/// Internal breaker-health classification. Retry eligibility is derived
/// separately from the typed effect error.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::middleware::control) enum FailureClassification {
    Success,
    TransientFailure,
    PermanentFailure,
    RateLimited(Duration),
    /// The dependency returned a typed caller/domain result that does not
    /// contribute either success or failure health.
    Ignored,
}

/// Synthetic error event carrying the typed failure's derived kind, the
/// classification input for raw physical-call failures (FLOWIP-115h).
pub(crate) fn effect_error_event(event: &ChainEvent, error: &EffectError) -> ChainEvent {
    event
        .clone()
        .mark_as_error(error.to_string(), error.error_kind())
}
