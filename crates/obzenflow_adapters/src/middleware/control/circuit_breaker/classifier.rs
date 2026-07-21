// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_runtime::effects::EffectError;
use std::sync::Arc;
use std::time::Duration;

pub(in crate::middleware::control) type FailureClassificationClassifier =
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
    PartialSuccess {
        failed_ratio: f32,
    },
    /// The dependency returned a typed caller/domain result that does not
    /// contribute either success or failure health.
    Ignored,
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

fn recorded_error_kind(error_type: &str) -> ErrorKind {
    match error_type {
        "timeout" => ErrorKind::Timeout,
        "transport" => ErrorKind::Remote,
        "rate_limited" => ErrorKind::RateLimited,
        "validation" => ErrorKind::Validation,
        "domain" => ErrorKind::Domain,
        "execution" | "permanent" => ErrorKind::PermanentFailure,
        _ => ErrorKind::Unknown,
    }
}

fn effect_error_kind(error: &EffectError) -> ErrorKind {
    match error {
        EffectError::Timeout(_) => ErrorKind::Timeout,
        EffectError::Transport(_) => ErrorKind::Remote,
        EffectError::RateLimited { .. } => ErrorKind::RateLimited,
        EffectError::Validation(_) => ErrorKind::Validation,
        EffectError::Domain(_) => ErrorKind::Domain,
        EffectError::RecordedFailure { error_type, .. } => recorded_error_kind(error_type.as_str()),
        EffectError::Serialization(_)
        | EffectError::Journal(_)
        | EffectError::MissingRecordedEffect { .. }
        | EffectError::DuplicateRecordedEffect { .. }
        | EffectError::DescriptorMismatch { .. }
        | EffectError::BoundaryRejected { .. }
        | EffectError::TypedOutcomeCoordination { .. }
        | EffectError::EffectProvenanceMismatch(_)
        | EffectError::IncompleteOutcomeGroup { .. }
        | EffectError::MissingIdempotencyKey { .. }
        | EffectError::UndeclaredEffect { .. }
        | EffectError::UndeclaredOutput { .. }
        | EffectError::EmitUnsupported { .. }
        | EffectError::MissingEffectPort { .. }
        | EffectError::TransactionalCommitMissing { .. }
        | EffectError::Execution(_)
        | EffectError::Permanent(_)
        | EffectError::ReplayArchive(_)
        | EffectError::CompletedWithoutOutput { .. }
        | EffectError::CompletedEmptyWithOutput { .. } => ErrorKind::PermanentFailure,
    }
}

/// Synthetic error event carrying the typed failure's derived kind, the
/// classification input for raw physical-call failures (FLOWIP-115h).
pub(crate) fn effect_error_event(event: &ChainEvent, error: &EffectError) -> ChainEvent {
    event
        .clone()
        .mark_as_error(error.to_string(), effect_error_kind(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn typed_effect_failures_map_to_structured_breaker_kinds() {
        assert_eq!(
            effect_error_kind(&EffectError::Timeout("timeout".to_string())),
            ErrorKind::Timeout
        );
        assert_eq!(
            effect_error_kind(&EffectError::Transport("offline".to_string())),
            ErrorKind::Remote
        );
        assert_eq!(
            effect_error_kind(&EffectError::RateLimited {
                message: "slow down".to_string(),
                retry_after: Duration::from_millis(250),
            }),
            ErrorKind::RateLimited
        );
        assert_eq!(
            effect_error_kind(&EffectError::Permanent("denied".to_string())),
            ErrorKind::PermanentFailure
        );
        assert_eq!(
            effect_error_kind(&EffectError::Validation("invalid".to_string())),
            ErrorKind::Validation
        );
        assert_eq!(
            effect_error_kind(&EffectError::Domain("declined".to_string())),
            ErrorKind::Domain
        );
        assert_eq!(
            effect_error_kind(&EffectError::Execution("opaque".to_string())),
            ErrorKind::PermanentFailure
        );
        assert_eq!(
            effect_error_kind(&EffectError::RecordedFailure {
                error_type: "domain".into(),
                error_message: "declined".to_string(),
                retry: obzenflow_core::event::RetryDisposition::NotRetryable,
                cause: None,
            }),
            ErrorKind::Domain
        );
    }
}
