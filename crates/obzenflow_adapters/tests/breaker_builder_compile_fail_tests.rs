// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn incomplete_fallback_shapes_do_not_build() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/breaker_builder/incomplete_branch_build.rs");
}
