// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

fn normalized(source: &str) -> String {
    source.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn retired_effect_policy_synthesis_language_is_absent() {
    let shipped_sources = [
        (
            "middleware README",
            include_str!("../src/middleware/README.md"),
        ),
        (
            "middleware module docs",
            include_str!("../src/middleware/mod.rs"),
        ),
        (
            "control-policy module docs",
            include_str!("../src/middleware/control/policy/mod.rs"),
        ),
        (
            "effect-policy contract",
            include_str!("../src/middleware/control/policy/effect/contract.rs"),
        ),
        (
            "circuit-breaker hook adapters",
            include_str!("../src/middleware/control/circuit_breaker/hook_adapters.rs"),
        ),
    ];
    let retired_claims = [
        "PolicyAdmission::Synthesize",
        "`Synthesize`",
        "rejects, or synthesizes at a live-I/O boundary",
        "synthesize around a protected runtime unit",
        "pause, reject, synthesize, or otherwise affect live I/O",
        "a later policy rejected or synthesized before the protected call executed",
    ];

    for (label, source) in shipped_sources {
        let source = normalized(source);
        for retired in retired_claims {
            assert!(
                !source.contains(retired),
                "{label} still contains retired effect-policy language: {retired}"
            );
        }
    }
}
