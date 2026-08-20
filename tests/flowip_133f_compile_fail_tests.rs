// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133f retired namespace compile-time proof.

#[test]
fn all_retired_typed_capability_families_fail_to_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133f_typed_joins_removed.rs");
    tests.compile_fail("tests/compile_fail/flowip_133f_typed_sinks_removed.rs");
    tests.compile_fail("tests/compile_fail/flowip_133f_typed_stateful_removed.rs");
    tests.compile_fail("tests/compile_fail/flowip_133f_typed_transforms_removed.rs");
}
