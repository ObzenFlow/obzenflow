// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Root authoring-surface compile-time closure proofs.

#[test]
fn removed_registry_plumbing_and_unsafe_shortcuts_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/root_authoring_flow_effect_ports_section_removed.rs");
    tests.compile_fail("tests/compile_fail/root_authoring_free_inference_factory_removed.rs");
    tests.compile_fail("tests/compile_fail/root_authoring_inference_requires_handler_trait.rs");
    tests.compile_fail("tests/compile_fail/root_authoring_typed_sources_facade_removed.rs");
    tests.compile_fail("tests/compile_fail/root_authoring_undeclared_chat_operation_rejected.rs");
}

#[cfg(feature = "test-support")]
#[test]
fn test_flow_also_rejects_registry_plumbing() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail(
        "tests/compile_fail/root_authoring_test_flow_effect_ports_section_removed.rs",
    );
}
