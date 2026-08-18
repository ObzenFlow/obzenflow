// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133e authoring-surface closure proofs.

#[test]
fn removed_registry_plumbing_and_unsafe_shortcuts_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133e_flow_effect_ports.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_capturing_inference_handler.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_role_is_not_inference_handler.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_undeclared_chat_operation.rs");
}

#[cfg(feature = "test-support")]
#[test]
fn test_flow_also_rejects_registry_plumbing() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133e_test_flow_effect_ports.rs");
}
