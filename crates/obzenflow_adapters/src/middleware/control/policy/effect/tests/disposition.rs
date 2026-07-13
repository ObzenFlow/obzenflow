// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[test]
fn effect_error_disposition_matrix_is_exhaustive_and_preserves_retry_hints() {
    let cursor = EffectCursor::new("test_flow", "test_stage", 1, 0);
    let retry_after = Duration::from_millis(725);
    let cases = [
        (
            EffectError::TransientExecution("transient".to_string()),
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Remote,
                retry_after: None,
            },
        ),
        (
            EffectError::RateLimited {
                message: "rate limited with hint".to_string(),
                retry_after: Some(retry_after),
            },
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::RateLimited,
                retry_after: Some(retry_after),
            },
        ),
        (
            EffectError::RateLimited {
                message: "rate limited without hint".to_string(),
                retry_after: None,
            },
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::RateLimited,
                retry_after: None,
            },
        ),
        (
            EffectError::Serialization("serialization".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Deserialization,
            },
        ),
        (
            EffectError::Execution("execution".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::Journal("journal".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::MissingRecordedEffect {
                cursor: cursor.clone(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::DuplicateRecordedEffect {
                cursor: cursor.clone(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::DescriptorMismatch {
                cursor: cursor.clone(),
                expected: EffectDescriptorHash::new("expected"),
                recorded: EffectDescriptorHash::new("recorded"),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::RecordedFailure {
                error_type: "recorded".into(),
                error_message: "recorded failure".to_string(),
                retry: RetryDisposition::Retryable,
                cause: None,
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::BoundaryRejected {
                rejected_by: EffectFailureSource::new("test"),
                code: EffectFailureCode::new("rejected"),
                message: "boundary rejected".to_string(),
                retry: RetryDisposition::Retryable,
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::TypedOutcomeCoordination {
                stage_key: "test_stage".to_string(),
                effect_type: "effect.retry_test".to_string(),
                message: "coordination".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::EffectProvenanceMismatch("provenance".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::IncompleteOutcomeGroup {
                cursor: cursor.clone(),
                expected: 2,
                present: 1,
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::MissingIdempotencyKey {
                effect_type: "effect.retry_test".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::UndeclaredEffect {
                stage_key: "test_stage".to_string(),
                effect_type: "effect.retry_test".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::UndeclaredOutput {
                stage_key: "test_stage".to_string(),
                event_type: "test.output".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::EmitUnsupported {
                stage_key: "test_stage".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::MissingEffectPort {
                type_name: "TestPort",
                name: "test".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::TransactionalCommitMissing {
                effect_type: "effect.retry_test".to_string(),
                executor: "test_executor".to_string(),
            },
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
        (
            EffectError::ReplayArchive("archive".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(effect_attempt_disposition(&Err(error)), expected);
    }
    assert_eq!(
        effect_attempt_disposition(&Ok(Vec::new())),
        AttemptDisposition::Completed
    );
}
