// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn retry_privilege_and_serial_effect_calls_are_compile_time_confined() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/effect_recovery/private_breaker_attachment.rs");
    cases.compile_fail("tests/ui/effect_recovery/private_breaker_attachment_match.rs");
    cases.compile_fail("tests/ui/effect_recovery/concurrent_effect_operation.rs");
}
