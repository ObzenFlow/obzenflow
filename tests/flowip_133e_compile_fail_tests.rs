// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133e authoring-surface closure proofs.

use std::path::Path;

#[test]
fn scalar_ai_handler_has_one_root_facade() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let typed_ai = root.join("src/typed/ai.rs");
    assert!(
        !typed_ai.exists(),
        "the retired typed AI facade must remain absent: {}",
        typed_ai.display()
    );

    let typed_mod = std::fs::read_to_string(root.join("src/typed/mod.rs"))
        .expect("typed facade module should be readable");
    assert!(
        !typed_mod.lines().any(|line| line.trim() == "pub mod ai;"),
        "obzenflow::typed::ai must not be reintroduced"
    );

    let ai_facade = std::fs::read_to_string(root.join("src/ai.rs"))
        .expect("AI facade module should be readable");
    assert!(
        ai_facade.contains("pub use obzenflow_runtime::stages::InferenceHandler;"),
        "obzenflow::ai must remain the root facade for the InferenceHandler trait"
    );
    assert!(
        !ai_facade.contains("inference_handler"),
        "the retired free inference factory must not return"
    );
}

#[test]
fn removed_registry_plumbing_and_unsafe_shortcuts_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133e_flow_effect_ports.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_free_inference_factory_removed.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_inference_handler_trait_required.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_undeclared_chat_operation.rs");
}

#[cfg(feature = "test-support")]
#[test]
fn test_flow_also_rejects_registry_plumbing() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133e_test_flow_effect_ports.rs");
}
