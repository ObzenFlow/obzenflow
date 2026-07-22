// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn committed_fact_evidence_does_not_claim_middleware_synthesis() {
    let source = include_str!("../src/effects/runtime.rs")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        !source.contains("middleware-synthesized fallback facts"),
        "effect runtime still describes retired middleware-synthesized facts"
    );
}
