// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g drift guards for the destructive handler-shell contraction.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_under(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

#[test]
fn generic_handler_shell_and_standalone_retry_vocabulary_stay_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![root.join("src/lib.rs")];
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let source_root = crate_entry.expect("read crate entry").path().join("src");
        if source_root.is_dir() {
            rust_sources_under(&source_root, &mut sources);
        }
    }

    let forbidden = [
        "MiddlewareTransform",
        "UnifiedMiddlewareTransform",
        "MiddlewareStateful",
        "TransformHandlerExt",
        "StatefulHandlerMiddlewareExt",
        "TransformMiddlewareBuilder",
        "StatefulMiddlewareBuilder",
        "MiddlewareAsyncFiniteSource",
        "MiddlewareAsyncInfiniteSource",
        "MiddlewareFiniteSource",
        "MiddlewareInfiniteSource",
        "AsyncFiniteSourceHandlerExt",
        "AsyncInfiniteSourceHandlerExt",
        "FiniteSourceHandlerExt",
        "InfiniteSourceHandlerExt",
        "AsyncFiniteSourceMiddlewareBuilder",
        "AsyncInfiniteSourceMiddlewareBuilder",
        "FiniteSourceMiddlewareBuilder",
        "InfiniteSourceMiddlewareBuilder",
        "MiddlewareJoin",
        "JoinHandlerMiddlewareExt",
        "JoinMiddlewareBuilder",
        "MiddlewareSink",
        "SinkHandlerExt",
        "SinkMiddlewareBuilder",
        "MiddlewareAction",
        "ErrorAction",
        "TopologyMiddlewareConfigSlot::Retry",
        "MiddlewareLifecycle::Retry",
        "RetryEvent",
        "MiddlewarePlanContribution",
        "AiMapReduceChunkContext",
        "AiMapReduceChunkContextKey",
        "UnifiedMiddlewareStateful",
    ];

    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        for token in forbidden {
            assert!(
                !source.contains(token),
                "retired token {token:?} resurfaced in {}",
                source_path.display()
            );
        }
    }
}

#[test]
fn effectful_stateful_keeps_only_its_existing_typed_lowerer() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let descriptor =
        fs::read_to_string(root.join("crates/obzenflow_dsl/src/dsl/stage_descriptor.rs"))
            .expect("read stage descriptor");
    let stateful_config =
        fs::read_to_string(root.join("crates/obzenflow_runtime/src/stages/stateful/config.rs"))
            .expect("read stateful config");

    assert!(descriptor.contains("EffectfulStatefulHandlerAdapter(self.handler)"));
    assert!(descriptor.contains("EffectfulStatefulPendingBoundary"));
    assert!(
        !stateful_config.contains("effect_boundary"),
        "FLOWIP-120l must not be pre-implemented as a StatefulConfig carrier"
    );
}
