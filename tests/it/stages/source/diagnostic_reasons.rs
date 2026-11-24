// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::stages::sources::{SourceDiagnosticReason, SourceError};
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SkipsEmptyTags {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
}

impl TypedPayload for SkipsEmptyTags {
    const EVENT_TYPE: &'static str = "flowip_084l.skips_empty_tags";
}

/// Hosted admission's journal-form proof (FLOWIP-084l B4).
fn unrepresentable_value() -> SourceError {
    let refused =
        obzenflow_runtime::stages::prove_journal_form(SkipsEmptyTags { tags: Vec::new() })
            .expect_err("an empty skipped list cannot be recorded faithfully");
    SourceError::Validation(refused.into_diagnostic())
}

fn produce(reason: SourceDiagnosticReason) -> Option<SourceError> {
    use SourceDiagnosticReason::*;
    Some(match reason {
        UnrepresentableValue => unrepresentable_value(),
        InputUnavailable | InputClosed | InputChanged | TimedOut | RateLimited | RemoteRejected
        | RemoteFailed | MalformedInput | UnsupportedConstruct | DuplicateKey
        | SizeLimitExceeded | SelectionNotFound | UnexpectedShape | MissingField | UnknownField
        | InvalidValue | InvalidRecord | Unclassified => return None,
    })
}

#[test]
fn produced_reason_hides_input_details() {
    use SourceDiagnosticReason::*;
    let reasons = [
        InputUnavailable,
        InputClosed,
        InputChanged,
        TimedOut,
        RateLimited,
        RemoteRejected,
        RemoteFailed,
        MalformedInput,
        UnsupportedConstruct,
        DuplicateKey,
        SizeLimitExceeded,
        SelectionNotFound,
        UnexpectedShape,
        MissingField,
        UnknownField,
        InvalidValue,
        InvalidRecord,
        UnrepresentableValue,
        Unclassified,
    ];
    let mut produced = 0;
    for reason in reasons {
        let Some(error) = produce(reason) else {
            continue;
        };
        produced += 1;
        assert_eq!(error.diagnostic().reason(), reason, "{error:?}");
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains("SECRET"), "{rendered}");
        assert!(
            !rendered.contains("flowip-084n"),
            "no filesystem path: {rendered}"
        );
    }
    assert_eq!(produced, 1);
}
