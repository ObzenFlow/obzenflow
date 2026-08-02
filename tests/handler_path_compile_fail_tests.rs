// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133a public handler-slot diagnostics.

#[test]
fn every_public_handler_and_role_slot_rejects_expressions() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/handler_path_expression_matrix.rs");
    tests.compile_fail("tests/compile_fail/handler_path_decorated_matrix.rs");
    tests.compile_fail("tests/compile_fail/handler_path_timeout_tuple.rs");
    tests.compile_fail("tests/compile_fail/handler_path_diagnostic_precedence.rs");
}
