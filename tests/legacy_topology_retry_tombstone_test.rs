// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compatibility fixture for the independently published topology 0.5.1
//! document. Runtime production of this field is retired; the published serde
//! type must still round-trip an older document until the 119-series versioned
//! topology boundary removes it.

#[test]
fn legacy_topology_retry_member_round_trips_without_runtime_authority() {
    let legacy = serde_json::json!({
        "stack": ["retry"],
        "retry": {
            "max_attempts": 3,
            "backoff": "fixed",
            "base_delay_ms": 100
        }
    });

    let decoded: obzenflow_topology::MiddlewareInfo =
        serde_json::from_value(legacy.clone()).expect("0.5.1 retry document deserialises");
    let encoded = serde_json::to_value(decoded).expect("0.5.1 retry document serialises");

    assert_eq!(encoded, legacy);
}
