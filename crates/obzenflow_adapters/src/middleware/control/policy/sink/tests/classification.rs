// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[test]
fn sink_disposition_matrix_is_exhaustive_and_preserves_retry_hints() {
    let retry_after = Duration::from_millis(725);
    let errors = [
        (
            HandlerError::Timeout("timeout".to_string()),
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Timeout,
                retry_after: None,
            },
        ),
        (
            HandlerError::Remote("remote".to_string()),
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Remote,
                retry_after: None,
            },
        ),
        (
            HandlerError::RateLimited {
                message: "rate limited with hint".to_string(),
                retry_after: Some(retry_after),
            },
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::RateLimited,
                retry_after: Some(retry_after),
            },
        ),
        (
            HandlerError::RateLimited {
                message: "rate limited without hint".to_string(),
                retry_after: None,
            },
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::RateLimited,
                retry_after: None,
            },
        ),
        (
            HandlerError::PermanentFailure("permanent".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::PermanentFailure,
            },
        ),
        (
            HandlerError::Deserialization("malformed".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Deserialization,
            },
        ),
        (
            HandlerError::Validation("invalid".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Validation,
            },
        ),
        (
            HandlerError::Domain("domain".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Domain,
            },
        ),
        (
            HandlerError::Other("unknown".to_string()),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            },
        ),
    ];

    for (error, expected) in errors {
        assert_eq!(
            sink_attempt_disposition(&SinkDeliveryAttemptOutcome::Delivered(Err(error))),
            expected
        );
    }

    let reports = [
        DeliveryPayload::success(DeliveryMethod::Noop, None),
        DeliveryPayload::buffered(DeliveryMethod::Noop, None),
        DeliveryPayload::failed(DeliveryMethod::Noop, "terminal", "failed", true),
        DeliveryPayload::partial(DeliveryMethod::Noop, 1, 1, "partial", None),
    ];
    for payload in reports {
        let outcome =
            SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(payload))));
        assert_eq!(
            sink_attempt_disposition(&outcome),
            AttemptDisposition::Completed
        );
    }

    assert_eq!(
        sink_attempt_disposition(&SinkDeliveryAttemptOutcome::Panicked {
            message: "panic".to_string(),
        }),
        AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        }
    );
}
