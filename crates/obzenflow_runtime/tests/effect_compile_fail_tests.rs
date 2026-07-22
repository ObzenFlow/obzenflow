// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn effect_requires_explicit_safety() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/effect_missing_safety.rs");
}

#[test]
fn delivery_requires_explicit_safety() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/delivery_missing_safety.rs");
}

/// FLOWIP-120z: an effect capability set rejects duplicate members.
#[test]
fn effect_set_rejects_duplicate_members() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/effect_set_duplicate_member.rs");
}

#[test]
fn typed_effect_operations_enforce_output_and_capability_contracts() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/effects_emit_outside_output.rs");
    t.compile_fail("tests/compile_fail/effects_perform_undeclared_effect.rs");
    t.compile_fail("tests/compile_fail/effects_outcome_not_output_subset.rs");
}

/// FLOWIP-120z: an effectful stateful output commits exactly one fact per value.
#[test]
fn effectful_stateful_output_rejects_product_carriers() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/stateful_output_product_carrier.rs");
}

#[test]
fn single_use_effect_operation_cannot_execute_twice() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/single_use_effect_operation_constructor.rs");
    t.compile_fail("tests/compile_fail/single_use_effect_operation_twice.rs");
    t.compile_fail("tests/compile_fail/single_use_effect_operation_clone.rs");
}
