// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public compile-fail proof for FLOWIP-115g's retired authoring routes.

#[test]
fn retired_handler_shell_authoring_routes_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/handler_shell_retirement/*.rs");
}
