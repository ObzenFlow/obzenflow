// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120z compile-time contracts for pure typed transform carriers.

#[test]
fn typed_transform_brace_arrows_require_an_exact_leaf_contract() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/transform_brace_arrow_raw_handler.rs");
    t.compile_fail("tests/compile_fail/transform_brace_arrow_missing_leaf.rs");
    t.compile_fail("tests/compile_fail/transform_brace_arrow_extra_leaf.rs");
    t.compile_fail("tests/compile_fail/effectful_arrow_missing_member.rs");
    t.compile_fail("tests/compile_fail/effectful_arrow_extra_member.rs");
    t.compile_fail("tests/compile_fail/effectful_manifest_undeclared_effect.rs");
    t.compile_fail("tests/compile_fail/effectful_manifest_unused_effect.rs");
    t.compile_fail("tests/compile_fail/effectful_handler_reused_incompatible_arrow.rs");
}
