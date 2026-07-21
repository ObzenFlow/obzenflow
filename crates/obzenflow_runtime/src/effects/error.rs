// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[derive(Debug, Clone, Error)]
pub enum EffectError {
    #[error("effect serialization failed: {0}")]
    Serialization(String),

    #[error("effect journal write failed: {0}")]
    Journal(String),

    #[error("missing recorded effect for cursor {cursor:?}")]
    MissingRecordedEffect { cursor: EffectCursor },

    #[error("duplicate recorded effect for cursor {cursor:?}")]
    DuplicateRecordedEffect { cursor: EffectCursor },

    #[error("effect descriptor mismatch for cursor {cursor:?}: expected {expected}, recorded {recorded}")]
    DescriptorMismatch {
        cursor: EffectCursor,
        expected: EffectDescriptorHash,
        recorded: EffectDescriptorHash,
    },

    #[error("recorded effect failure: {error_type}: {error_message}")]
    RecordedFailure {
        error_type: EffectFailureKind,
        error_message: String,
        retry: RetryDisposition,
        cause: Option<EffectFailureCause>,
    },

    #[error("effect rejected at boundary by {rejected_by}: {code}: {message}")]
    BoundaryRejected {
        rejected_by: EffectFailureSource,
        code: EffectFailureCode,
        message: String,
        retry: RetryDisposition,
    },

    #[error("effect provenance mismatch: {0}")]
    EffectProvenanceMismatch(String),

    #[error(
        "incomplete effect outcome group for cursor {cursor:?}: {present} of {expected} facts present"
    )]
    IncompleteOutcomeGroup {
        cursor: EffectCursor,
        expected: usize,
        present: usize,
    },

    #[error("non-idempotent effect '{effect_type}' has no idempotency key")]
    MissingIdempotencyKey { effect_type: String },

    #[error("effectful stage '{stage_key}' performed undeclared effect '{effect_type}'")]
    UndeclaredEffect {
        stage_key: String,
        effect_type: String,
    },

    #[error("effectful stage '{stage_key}' emitted undeclared output fact '{event_type}'")]
    UndeclaredOutput {
        stage_key: String,
        event_type: String,
    },

    #[error("effectful stage '{stage_key}' cannot emit output facts from this handler context")]
    EmitUnsupported { stage_key: String },

    #[error("effectful stage '{stage_key}' completed without authoring an output fact")]
    CompletedWithoutOutput { stage_key: String },

    #[error(
        "effectful stage '{stage_key}' used complete_empty after committing {committed} output facts"
    )]
    CompletedEmptyWithOutput { stage_key: String, committed: usize },

    #[error("effect port '{name}' for type '{type_name}' is not registered")]
    MissingEffectPort {
        type_name: &'static str,
        name: String,
    },

    #[error(
        "transactional effect '{effect_type}' using executor '{executor}' returned without committing an effect result"
    )]
    TransactionalCommitMissing {
        effect_type: String,
        executor: String,
    },

    /// Legacy opaque call failure. Circuit-breaker recovery treats it as
    /// permanent and ineligible; effect ports should author a typed variant.
    #[error("effect execution failed: {0}")]
    Execution(String),

    /// The physical call exceeded its operation timeout.
    #[error("effect call timed out: {0}")]
    Timeout(String),

    /// The physical call failed at the transport boundary.
    #[error("effect call transport failed: {0}")]
    Transport(String),

    /// The dependency refused the call and supplied a minimum retry delay.
    #[error("effect call was rate limited: {message}")]
    RateLimited {
        message: String,
        retry_after: Duration,
    },

    /// The physical call failed in a way another call cannot repair.
    #[error("effect call failed permanently: {0}")]
    Permanent(String),

    /// The dependency rejected invalid call input.
    #[error("effect call validation failed: {0}")]
    Validation(String),

    /// The dependency returned a domain-level failure.
    #[error("effect call domain failure: {0}")]
    Domain(String),

    #[error("effect replay archive failed: {0}")]
    ReplayArchive(String),
}

impl EffectError {
    fn recovery_eligible_by_default(&self) -> bool {
        match self {
            EffectError::RecordedFailure { retry, .. }
            | EffectError::BoundaryRejected { retry, .. } => retry.is_retryable(),
            EffectError::EffectProvenanceMismatch(_) => false,
            EffectError::IncompleteOutcomeGroup { .. } => false,
            EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::CompletedWithoutOutput { .. }
            | EffectError::CompletedEmptyWithOutput { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TransactionalCommitMissing { .. } => false,
            EffectError::Timeout(_)
            | EffectError::Transport(_)
            | EffectError::RateLimited { .. }
            | EffectError::Serialization(_)
            | EffectError::Journal(_)
            | EffectError::Execution(_)
            | EffectError::ReplayArchive(_) => true,
            EffectError::Permanent(_) | EffectError::Validation(_) | EffectError::Domain(_) => {
                false
            }
        }
    }

    pub(super) fn retry_disposition(&self) -> RetryDisposition {
        RetryDisposition::from_bool(self.recovery_eligible_by_default())
    }

    pub(super) fn error_type(&self) -> EffectFailureKind {
        match self {
            EffectError::Serialization(_) => "serialization",
            EffectError::Journal(_) => "journal",
            EffectError::MissingRecordedEffect { .. } => "missing_recorded_effect",
            EffectError::DuplicateRecordedEffect { .. } => "duplicate_recorded_effect",
            EffectError::DescriptorMismatch { .. } => "descriptor_mismatch",
            EffectError::RecordedFailure { error_type, .. } => return error_type.clone(),
            EffectError::BoundaryRejected { .. } => "boundary_rejected",
            EffectError::EffectProvenanceMismatch(_) => "effect_provenance_mismatch",
            EffectError::IncompleteOutcomeGroup { .. } => "incomplete_outcome_group",
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
            EffectError::UndeclaredEffect { .. } => "undeclared_effect",
            EffectError::UndeclaredOutput { .. } => "undeclared_output",
            EffectError::EmitUnsupported { .. } => "emit_unsupported",
            EffectError::CompletedWithoutOutput { .. } => "completed_without_output",
            EffectError::CompletedEmptyWithOutput { .. } => "completed_empty_with_output",
            EffectError::MissingEffectPort { .. } => "missing_effect_port",
            EffectError::TransactionalCommitMissing { .. } => "transactional_commit_missing",
            EffectError::Execution(_) => "execution",
            EffectError::Timeout(_) => "timeout",
            EffectError::Transport(_) => "transport",
            EffectError::RateLimited { .. } => "rate_limited",
            EffectError::Permanent(_) => "permanent",
            EffectError::Validation(_) => "validation",
            EffectError::Domain(_) => "domain",
            EffectError::ReplayArchive(_) => "replay_archive",
        }
        .into()
    }

    /// The business-facing failure reason, identical live and replayed
    /// (FLOWIP-120i).
    ///
    /// A live `Execution("gateway_timeout_simulated")` and the
    /// `RecordedFailure` its journaled outcome rehydrates to on replay both
    /// project to `"gateway_timeout_simulated"`, so failure-mapping code has
    /// one path and never parses Display wrappers. Variants carrying a
    /// semantic message borrow it; infrastructure variants with structured
    /// fields fall back to their Display rendering.
    pub fn semantic_reason(&self) -> std::borrow::Cow<'_, str> {
        match self {
            EffectError::Execution(message)
            | EffectError::Timeout(message)
            | EffectError::Transport(message)
            | EffectError::Permanent(message)
            | EffectError::Validation(message)
            | EffectError::Domain(message)
            | EffectError::Serialization(message)
            | EffectError::Journal(message)
            | EffectError::ReplayArchive(message)
            | EffectError::EffectProvenanceMismatch(message) => std::borrow::Cow::Borrowed(message),
            EffectError::RecordedFailure { error_message, .. } => {
                std::borrow::Cow::Borrowed(error_message)
            }
            EffectError::RateLimited { message, .. } => std::borrow::Cow::Borrowed(message),
            EffectError::BoundaryRejected { message, .. } => std::borrow::Cow::Borrowed(message),
            EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::IncompleteOutcomeGroup { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::CompletedWithoutOutput { .. }
            | EffectError::CompletedEmptyWithOutput { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TransactionalCommitMissing { .. } => {
                std::borrow::Cow::Owned(self.to_string())
            }
        }
    }

    /// The message recorded into a `Failed` outcome payload. The semantic
    /// reason, never the Display wrapper: recording `self.to_string()` here
    /// baked "effect execution failed: " into journaled facts, which replay
    /// then surfaced to user code as a different business payload than the
    /// live run produced (the FLOWIP-120i wrapper leak).
    pub(super) fn error_message(&self) -> String {
        self.semantic_reason().into_owned()
    }

    /// Structured cause carried into a recorded `Failed` outcome, present when
    /// execution was rejected by a named component such as boundary middleware.
    /// Live `BoundaryRejected` errors and replayed `RecordedFailure` errors
    /// return the same source/code pair.
    pub fn failure_cause(&self) -> Option<EffectFailureCause> {
        match self {
            EffectError::BoundaryRejected {
                rejected_by, code, ..
            } => Some(EffectFailureCause {
                source: rejected_by.clone(),
                code: code.clone(),
            }),
            EffectError::RecordedFailure { cause, .. } => cause.clone(),
            _ => None,
        }
    }
}

impl From<ReplayError> for EffectError {
    fn from(value: ReplayError) -> Self {
        Self::ReplayArchive(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FLOWIP-120i: the semantic reason is identical across the live error,
    /// what gets recorded, and the replay-side rehydration, with no Display
    /// wrapper anywhere in the chain.
    #[test]
    fn semantic_reason_is_stable_across_live_recording_and_replay() {
        let live = EffectError::Execution("gateway_timeout_simulated".to_string());
        assert_eq!(live.semantic_reason(), "gateway_timeout_simulated");
        assert_eq!(live.error_message(), "gateway_timeout_simulated");

        // What replay rehydrates from the recorded Failed payload.
        let replayed = EffectError::RecordedFailure {
            error_type: live.error_type(),
            error_message: live.error_message(),
            retry: live.retry_disposition(),
            cause: live.failure_cause(),
        };
        assert_eq!(replayed.semantic_reason(), live.semantic_reason());
        assert_eq!(
            replayed.error_message(),
            "gateway_timeout_simulated",
            "re-recording a recorded failure must not wrap again"
        );
        assert!(
            !replayed
                .semantic_reason()
                .contains("effect execution failed"),
            "no Display wrapper may leak into the semantic reason"
        );
    }

    #[test]
    fn semantic_reason_projects_boundary_rejections_to_their_message() {
        let rejected = EffectError::BoundaryRejected {
            rejected_by: EffectFailureSource::new("effect_boundary"),
            code: EffectFailureCode::new("breaker_open"),
            message: "circuit breaker open".to_string(),
            retry: RetryDisposition::from_bool(true),
        };
        assert_eq!(rejected.semantic_reason(), "circuit breaker open");
        assert_eq!(
            rejected.error_message(),
            "circuit breaker open",
            "structured source and code live in the recorded cause, never the message"
        );
        assert!(rejected.failure_cause().is_some());
    }

    #[test]
    fn semantic_reason_falls_back_to_display_for_infrastructure_variants() {
        let infra = EffectError::MissingIdempotencyKey {
            effect_type: "demo.effect".to_string(),
        };
        assert_eq!(infra.semantic_reason(), infra.to_string());
    }
}
