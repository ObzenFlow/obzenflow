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

#[test]
fn single_use_effect_operation_cannot_execute_twice() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/single_use_effect_operation_constructor.rs");
    t.compile_fail("tests/compile_fail/single_use_effect_operation_twice.rs");
    t.compile_fail("tests/compile_fail/single_use_effect_operation_clone.rs");
    t.compile_fail("tests/compile_fail/single_use_effect_operation_reject_after_execute.rs");
}
