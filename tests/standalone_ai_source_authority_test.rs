// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI source authority and contraction guard: the members that run AI roles
//! reach a model only through their effect, and the retired standalone
//! surfaces stay absent.

use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_sources_beneath(directory: &Path, sources: &mut Vec<PathBuf>) {
    if !directory.exists() {
        return;
    }
    for entry in std::fs::read_dir(directory).expect("guarded source directory") {
        let path = entry.expect("guarded source entry").path();
        if path.is_dir() {
            rust_sources_beneath(&path, sources);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            sources.push(path);
        }
    }
}

fn guarded_role_sources() -> Vec<PathBuf> {
    let mut sources = Vec::new();
    rust_sources_beneath(&root().join("crates/obzenflow_runtime/src/ai"), &mut sources);
    sources.push(root().join("crates/obzenflow_adapters/src/ai/shapes.rs"));
    sources.push(root().join("crates/obzenflow_dsl/src/dsl/ai_stage.rs"));
    sources.sort();
    sources
}

#[test]
fn ai_role_members_and_builders_have_no_direct_provider_call_authority() {
    let root = root();
    let guarded = guarded_role_sources();
    assert!(
        guarded.len() >= 9,
        "the guard must cover every runtime AI module, the builders and the lowering"
    );
    for path in guarded {
        let relative = path.strip_prefix(&root).unwrap().display();
        let source = std::fs::read_to_string(&path).unwrap();
        for forbidden in [
            ".chat(",
            ".embed(",
            "Arc<dyn ChatClient>",
            "Arc<dyn EmbeddingClient>",
            ".connect(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} regained direct provider authority through {forbidden}"
            );
        }
    }
    let handlers =
        std::fs::read_to_string(root.join("crates/obzenflow_runtime/src/ai/handlers.rs")).unwrap();
    assert!(
        handlers.contains("fx.perform::<E, At, SubsetProofEnd>(effect)"),
        "generated AI members perform their call through the effect runtime"
    );
}

#[test]
fn retired_standalone_surfaces_stay_absent() {
    let root = root();
    for retired in [
        "crates/obzenflow_adapters/src/ai/transforms",
        "crates/obzenflow_adapters/src/ai/builders.rs",
        "crates/obzenflow_adapters/src/ai/operations.rs",
        "crates/obzenflow_infra/src/ai/rig_builder.rs",
        "crates/obzenflow_infra/src/ai/chat_effect_binding.rs",
        "crates/obzenflow_infra/src/ai/embedding_effect_binding.rs",
        "crates/obzenflow_infra/src/ai/model_config.rs",
        "crates/obzenflow_dsl/src/dsl/inference.rs",
        "crates/obzenflow_dsl/src/dsl/ai_effect.rs",
        "crates/obzenflow_dsl/src/dsl/composites/ai_map_reduce.rs",
        "crates/obzenflow_runtime/src/stages/common/handlers/inference.rs",
    ] {
        assert!(!root.join(retired).exists(), "{retired} returned");
    }
    let facade = std::fs::read_to_string(root.join("src/ai.rs")).unwrap();
    for retired in [
        "ChatRequestTemplate",
        "ChatTransformBuilderWithContext",
        "ChatTransformExt",
        "EmbeddingTransformExt",
        "ModelChatBuilder",
        "ModelChatBuilderWithContext",
        "ChatTransform",
        "EmbeddingTransform",
        "ChatEffects",
        "EmbeddingEffects",
        "ChatEffectBinding",
        "EmbeddingEffectBinding",
        "ModelConfig",
        "InferenceHandler",
        "AiMapRole",
        "AiFinalizeRole",
        "AiRoleLogicFailure",
        "ChunkInfo",
        "Many",
    ] {
        assert!(
            !facade.contains(retired),
            "retired public identifier {retired} was re-exported"
        );
    }
    let middleware = std::fs::read_to_string(root.join("src/middleware.rs")).unwrap();
    assert!(!middleware.contains("pub use obzenflow_adapters::middleware::control::{ai_resilience"));
    assert!(!middleware.contains("AiResilience"));
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
