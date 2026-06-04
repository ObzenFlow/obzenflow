// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn effect_requires_explicit_safety() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/effect_missing_safety.rs");
}
