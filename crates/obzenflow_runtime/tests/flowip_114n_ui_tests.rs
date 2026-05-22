// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn flowip_114n_ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/flowip_114n_*.rs");
}
