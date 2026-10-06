// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Guard the 115v cutover against restoring position-based attachment authority.
//! Behavioural permutation and conflict proofs live in the integration fixtures.

use std::fs;
use std::path::PathBuf;

#[test]
fn middleware_identity_has_no_grammar_ordinals_or_retired_authoring_exports() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_adapters/src/middleware/carrier.rs",
        "crates/obzenflow_dsl/src/dsl/binder.rs",
        "crates/obzenflow_dsl/src/dsl/stage_descriptor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read identity source");
        for retired in [
            "MiddlewareDeclarationIndex",
            "MiddlewareDeclarationPosition",
            "declaration.ordinal",
        ] {
            assert!(!source.contains(retired), "{relative} restored {retired}");
        }
    }
    let facade = fs::read_to_string(root.join("src/middleware.rs")).expect("middleware facade");
    let facade = facade
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    for retired in [
        "EffectResilience",
        "RateLimiterBuilder",
        "RateLimiterFactory",
        "CheckedCircuitBreakerBuilder",
        "rate_limit_with_burst",
    ] {
        assert!(
            !facade.contains(retired),
            "retired authoring export {retired}"
        );
    }
}
