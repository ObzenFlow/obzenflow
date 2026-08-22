// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public middleware-attachment grammar retirement and delimiter diagnostics.

#[test]
fn retired_and_reserved_attachment_spellings_have_curated_diagnostics() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/middleware_attachment_*.rs");
}
