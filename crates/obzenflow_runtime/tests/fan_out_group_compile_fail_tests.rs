// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn fan_out_group_rejects_payload_identity_keys() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/fan_out_group_*.rs");
}
