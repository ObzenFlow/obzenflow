// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-132a grammar and typed-binding rejection matrix.

#[test]
fn retired_effect_grammar_and_incoherent_bindings_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_132a_*.rs");
}

#[test]
fn public_typed_registration_builder_compiles_for_non_framework_providers() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/compile_pass/public_effect_registration_builder.rs");
}
