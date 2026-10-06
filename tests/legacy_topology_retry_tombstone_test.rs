// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The current topology contract rejects the retired ordered-stack representation.

#[test]
fn retired_topology_retry_member_is_rejected() {
    let retired = serde_json::json!({
        "stack": ["retry"],
        "retry": { "max_attempts": 3, "backoff": "fixed", "base_delay_ms": 100 }
    });
    assert!(serde_json::from_value::<obzenflow_topology::MiddlewareInfo>(retired).is_err());
}

#[test]
fn resolved_retry_attachment_round_trips_with_its_operation() {
    use obzenflow_topology::{
        MiddlewareAttachmentInfo, MiddlewareAttachmentKey, MiddlewareAuthoredSite,
        MiddlewareDetailsInfo, MiddlewareFamily, MiddlewareInfo, MiddlewareOperation,
        ResolvedSettingInfo, SettingProvenanceInfo, SettingSubject, SettingValueInfo,
    };
    let settings = [
        ("middleware.retry.max_attempts", SettingValueInfo::U64(3)),
        (
            "middleware.retry.kind",
            SettingValueInfo::Text("fixed".into()),
        ),
        (
            "middleware.retry.fixed_delay_ms",
            SettingValueInfo::U64(100),
        ),
        (
            "middleware.retry.max_backoff_ms",
            SettingValueInfo::U64(30_000),
        ),
        (
            "middleware.retry.attempt_start_window_ms",
            SettingValueInfo::U64(120_000),
        ),
    ]
    .into_iter()
    .map(|(key, value)| {
        (
            key.to_owned(),
            ResolvedSettingInfo {
                value,
                provenance: SettingProvenanceInfo {
                    source: "dsl".into(),
                    scope: "stage:payments".into(),
                    winner_subject: SettingSubject::Unqualified,
                },
            },
        )
    })
    .collect();
    let expected = MiddlewareInfo {
        attachments: vec![MiddlewareAttachmentInfo {
            key: MiddlewareAttachmentKey::from_bytes([7; 16]),
            label: "retry".into(),
            authored_site: MiddlewareAuthoredSite::Implementation,
            operation: MiddlewareOperation::Effect {
                effect_type: "payments.authorize".into(),
            },
            details: MiddlewareDetailsInfo::try_from_settings(MiddlewareFamily::Retry, settings)
                .unwrap(),
        }],
    };
    let encoded = serde_json::to_value(&expected).unwrap();
    assert!(encoded.get("stack").is_none());
    assert!(encoded.get("retry").is_none());
    assert_eq!(
        serde_json::from_value::<MiddlewareInfo>(encoded).unwrap(),
        expected
    );
}
