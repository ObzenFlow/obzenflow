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
        MiddlewareAttachmentInfo, MiddlewareAuthoredSite, MiddlewareFamily, MiddlewareInfo,
        MiddlewareOperation,
    };
    let expected = MiddlewareInfo {
        attachments: vec![MiddlewareAttachmentInfo {
            key: "payments:authorize:retry".into(),
            label: "retry".into(),
            family: MiddlewareFamily::Retry,
            authored_site: MiddlewareAuthoredSite::Implementation,
            operation: MiddlewareOperation::Effect {
                effect_type: "payments.authorize".into(),
            },
            configuration: serde_json::json!({
                "middleware.retry.max_attempts": { "value": 3, "source": "dsl", "scope": "stage:payments" },
                "middleware.retry.kind": { "value": "fixed", "source": "dsl", "scope": "stage:payments" },
                "middleware.retry.fixed_delay_ms": { "value": 100, "source": "dsl", "scope": "stage:payments" },
            }),
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
