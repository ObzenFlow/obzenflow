// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Retired typed-facade compile-time proof.

#[test]
fn all_retired_typed_capability_families_fail_to_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/retired_typed_facade_joins_removed.rs");
    tests.compile_fail("tests/compile_fail/retired_typed_facade_sinks_removed.rs");
    tests.compile_fail("tests/compile_fail/retired_typed_facade_stateful_removed.rs");
    tests.compile_fail("tests/compile_fail/retired_typed_facade_transforms_removed.rs");
}
