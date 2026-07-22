// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn effect_provenance_does_not_claim_middleware_synthesis() {
    let source = include_str!("../src/event/payloads/effect_payload.rs")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        !source.contains("set when a middleware synthesized the outcome group"),
        "effect provenance still describes the retired middleware-synthesis branch"
    );
}
