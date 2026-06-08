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
        error_type: String,
        error_message: String,
        retryable: bool,
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
            EffectError::RecordedFailure { retryable, .. } => *retryable,
            EffectError::EffectProvenanceMismatch(_) => false,
            EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TransactionalCommitMissing { .. } => false,
            EffectError::Serialization(_)
            | EffectError::Journal(_)
            | EffectError::Execution(_)
            | EffectError::ReplayArchive(_) => true,
        }
    }

    pub(super) fn error_type(&self) -> String {
        match self {
            EffectError::Serialization(_) => "serialization",
            EffectError::Journal(_) => "journal",
            EffectError::MissingRecordedEffect { .. } => "missing_recorded_effect",
            EffectError::DuplicateRecordedEffect { .. } => "duplicate_recorded_effect",
            EffectError::DescriptorMismatch { .. } => "descriptor_mismatch",
            EffectError::RecordedFailure { error_type, .. } => return error_type.clone(),
            EffectError::EffectProvenanceMismatch(_) => "effect_provenance_mismatch",
            EffectError::MissingIdempotencyKey { .. } => "missing_idempotency_key",
            EffectError::UndeclaredEffect { .. } => "undeclared_effect",
            EffectError::UndeclaredOutput { .. } => "undeclared_output",
            EffectError::EmitUnsupported { .. } => "emit_unsupported",
            EffectError::MissingEffectPort { .. } => "missing_effect_port",
            EffectError::TransactionalCommitMissing { .. } => "transactional_commit_missing",
            EffectError::Execution(_) => "execution",
            EffectError::ReplayArchive(_) => "replay_archive",
        }
        .to_string()
    }

    pub(super) fn error_message(&self) -> String {
        self.to_string()
    }
}

impl From<ReplayError> for EffectError {
    fn from(value: ReplayError) -> Self {
        Self::ReplayArchive(value.to_string())
    }
}
