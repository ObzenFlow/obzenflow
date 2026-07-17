// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120z compile-time contracts for typed stage outputs and capabilities.

#[test]
fn typed_transform_brace_arrows_require_an_exact_leaf_contract() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/transform_brace_arrow_raw_handler.rs");
    t.compile_fail("tests/compile_fail/transform_brace_arrow_missing_leaf.rs");
    t.compile_fail("tests/compile_fail/transform_brace_arrow_extra_leaf.rs");
}

#[test]
fn effectful_handlers_require_complete_exact_contract_witnesses() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/effectful_transform_missing_contract_types.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_missing_contract_types.rs");
    t.compile_fail("tests/compile_fail/effectful_transform_raw_handler.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_raw_handler.rs");
    t.compile_fail("tests/compile_fail/effectful_transform_input_mismatch.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_input_mismatch.rs");
    t.compile_fail("tests/compile_fail/effectful_arrow_missing_member.rs");
    t.compile_fail("tests/compile_fail/effectful_arrow_extra_member.rs");
    t.compile_fail("tests/compile_fail/effectful_manifest_undeclared_effect.rs");
    t.compile_fail("tests/compile_fail/effectful_handler_effect_missing_from_manifest.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_arrow_missing_member.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_arrow_extra_member.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_manifest_undeclared_effect.rs");
    t.compile_fail("tests/compile_fail/effectful_stateful_handler_effect_missing_from_manifest.rs");
    t.compile_fail("tests/compile_fail/effectful_handler_reused_incompatible_arrow.rs");
}
