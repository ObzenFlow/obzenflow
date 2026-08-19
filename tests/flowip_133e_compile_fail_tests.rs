// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133e authoring-surface closure proofs.

use std::path::Path;

#[test]
fn authored_adapters_have_owned_root_facades() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let typed_root = root.join("src/typed");
    assert!(
        !typed_root.exists(),
        "the retired typed facade must remain absent: {}",
        typed_root.display()
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

    let source_facade = std::fs::read_to_string(root.join("src/sources.rs"))
        .expect("source facade module should be readable");
    assert!(
        source_facade.contains("pub use obzenflow_adapters::sources"),
        "obzenflow::sources must re-export its constructors from adapters"
    );
}

#[test]
fn removed_registry_plumbing_and_unsafe_shortcuts_do_not_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133e_flow_effect_ports.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_free_inference_factory_removed.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_inference_handler_trait_required.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_typed_sources_removed.rs");
    tests.compile_fail("tests/compile_fail/flowip_133e_undeclared_chat_operation.rs");
}

#[cfg(feature = "test-support")]
#[test]
fn test_flow_also_rejects_registry_plumbing() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/flowip_133e_test_flow_effect_ports.rs");
}
