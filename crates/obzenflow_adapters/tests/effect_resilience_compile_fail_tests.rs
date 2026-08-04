// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn retry_requires_concrete_configuration() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/effect_resilience/retry_none.rs");
    cases.compile_fail("tests/ui/effect_resilience/retry_some.rs");
}
