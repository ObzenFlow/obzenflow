// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128b T1/T5 source authority and contraction guard.

use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn standalone_handlers_and_builders_have_no_direct_provider_call_authority() {
    for relative in [
        "crates/obzenflow_adapters/src/ai/builders.rs",
        "crates/obzenflow_adapters/src/ai/transforms/chat.rs",
        "crates/obzenflow_adapters/src/ai/transforms/embedding.rs",
    ] {
        let source = std::fs::read_to_string(root().join(relative)).unwrap();
        for forbidden in [
            ".chat(",
            ".embed(",
            "Arc<dyn ChatClient>",
            "Arc<dyn EmbeddingClient>",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} regained direct provider authority through {forbidden}"
            );
        }
        assert!(
            source.contains(".perform(effect)") || relative.ends_with("builders.rs"),
            "standalone handlers must cross the typed effect facade"
        );
    }
}

#[test]
fn retired_builder_implementation_and_public_exports_stay_absent() {
    assert!(!root()
        .join("crates/obzenflow_infra/src/ai/rig_builder.rs")
        .exists());
    let facade = std::fs::read_to_string(root().join("src/ai.rs")).unwrap();
    for retired in [
        "ChatRequestTemplate",
        "ChatTransformBuilderWithContext",
        "ChatTransformExt",
        "EmbeddingTransformExt",
        "ModelChatBuilder",
        "ModelChatBuilderWithContext",
    ] {
        assert!(
            !facade.contains(retired),
            "retired public identifier {retired} was re-exported"
        );
    }
}

#[test]
fn embedding_reply_shape_has_no_raw_provider_envelope() {
    let types =
        std::fs::read_to_string(root().join("crates/obzenflow_core/src/ai/types.rs")).unwrap();
    let response = types
        .split("pub struct EmbeddingResponse")
        .nth(1)
        .and_then(|tail| tail.split("pub struct EmbeddingGenerationReply").next())
        .expect("embedding response source span");
    assert!(!response.contains("raw:"));
}
