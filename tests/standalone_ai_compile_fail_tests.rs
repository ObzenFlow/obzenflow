// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "ai")]

#[test]
fn locked_standalone_ai_syntax_compiles() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/compile_pass/standalone_ai_effect_syntax.rs");
}

#[test]
fn retired_standalone_ai_surface_does_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/standalone_ai_removed_*.rs");
}
