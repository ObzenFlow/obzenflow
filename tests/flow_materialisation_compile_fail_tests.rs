// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn retired_flow_bindings_section_has_a_teaching_diagnostic() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flow_bindings_removed.rs");
}

#[cfg(feature = "test-support")]
#[test]
fn retired_test_flow_bindings_section_has_a_teaching_diagnostic() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/test_flow_bindings_removed.rs");
}
