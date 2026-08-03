// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128b T1/T5 source authority and contraction guard.

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

fn guarded_standalone_sources() -> Vec<PathBuf> {
    let ai = root().join("crates/obzenflow_adapters/src/ai");
    let mut sources = Vec::new();
    rust_sources_beneath(&ai.join("transforms"), &mut sources);
    rust_sources_beneath(&ai.join("builders"), &mut sources);
    sources.push(ai.join("builders.rs"));
    sources.sort();
    sources
}

#[test]
fn standalone_handlers_and_builders_have_no_direct_provider_call_authority() {
    let root = root();
    let guarded = guarded_standalone_sources();
    assert!(
        guarded.len() >= 4,
        "the recursive guard must cover builders.rs and the complete transform module"
    );
    for path in guarded {
        let relative = path.strip_prefix(&root).unwrap().display();
        let source = std::fs::read_to_string(&path).unwrap();
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
        if source.contains("EffectfulTransformHandler for") {
            assert!(
                source.contains(".perform(effect)"),
                "standalone handler {relative} must cross the typed effect facade"
            );
        }
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
