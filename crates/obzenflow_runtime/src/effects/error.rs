// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[derive(Debug, Error)]
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
        rejected_by: String,
        code: String,
        message: String,
        retry: RetryDisposition,
    },

    #[error(
        "typed-outcome coordination error on stage '{stage_key}' for effect '{effect_type}': {message}"
    )]
    TypedOutcomeCoordination {
        stage_key: String,
        effect_type: String,
        message: String,
    },

    #[error("effect provenance mismatch: {0}")]
    EffectProvenanceMismatch(String),

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

    #[error("effect execution failed: {0}")]
    Execution(String),

    #[error("effect replay archive failed: {0}")]
    ReplayArchive(String),
}

impl EffectError {
    pub fn retryable(&self) -> bool {
        match self {
            EffectError::RecordedFailure { retry, .. }
            | EffectError::BoundaryRejected { retry, .. } => retry.is_retryable(),
            EffectError::EffectProvenanceMismatch(_) => false,
            EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TypedOutcomeCoordination { .. }
            | EffectError::TransactionalCommitMissing { .. } => false,
            EffectError::Serialization(_)
            | EffectError::Journal(_)
            | EffectError::Execution(_)
            | EffectError::ReplayArchive(_) => true,
        }
    }

    pub(super) fn retry_disposition(&self) -> RetryDisposition {
        RetryDisposition::from_bool(self.retryable())
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
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
            EffectError::UndeclaredEffect { .. } => "undeclared_effect",
            EffectError::UndeclaredOutput { .. } => "undeclared_output",
            EffectError::EmitUnsupported { .. } => "emit_unsupported",
            EffectError::MissingEffectPort { .. } => "missing_effect_port",
            EffectError::TypedOutcomeCoordination { .. } => "typed_outcome_coordination",
            EffectError::TransactionalCommitMissing { .. } => "transactional_commit_missing",
            EffectError::Execution(_) => "execution",
            EffectError::ReplayArchive(_) => "replay_archive",
        }
        .into()
    }

    pub(super) fn error_message(&self) -> String {
        self.to_string()
    }

    /// Structured cause carried into a recorded `Failed` outcome, present when
    /// execution was rejected by a named component such as boundary middleware.
    pub(super) fn failure_cause(&self) -> Option<EffectFailureCause> {
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
