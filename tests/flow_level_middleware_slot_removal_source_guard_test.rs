// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115r source and API-shape guard.

use std::fs;
use std::path::PathBuf;

fn source(root: &std::path::Path, relative: &str) -> String {
    fs::read_to_string(root.join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

#[test]
fn deleted_flow_middleware_plumbing_stays_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    assert!(
        !root
            .join("crates/obzenflow_dsl/src/middleware_resolution.rs")
            .exists(),
        "the flow-to-stage resolver must stay deleted"
    );

    let dsl = source(&root, "crates/obzenflow_dsl/src/dsl/dsl.rs");
    assert!(!dsl.contains("macro_rules! build_typed_flow"));
    assert!(!dsl.contains("create_flow_middleware"));
    assert!(dsl.contains("flow! has no middleware slot"));
    assert!(dsl.contains("test_flow! has no middleware slot"));

    let dsl_root = source(&root, "crates/obzenflow_dsl/src/lib.rs");
    assert!(!dsl_root.contains("mod middleware_resolution"));

    let builder = source(&root, "crates/obzenflow_dsl/src/dsl/flow_builder.rs");
    for retired in [
        "middleware_resolution",
        "create_flow_middleware",
        "PolicyMiddlewareOnFlowScope",
        "MiddlewareSourceScope",
    ] {
        assert!(
            !builder.contains(retired),
            "retired flow-lane token resurfaced in the ordinary builder: {retired}"
        );
    }
    assert!(
        builder.contains("clause.declare(&mut __dsl_candidates, ConfigScope::Flow)"),
        "flow-level backpressure must retain its legitimate flow config scope"
    );
    assert!(
        builder.contains(".create_handle(config, resources, control_middleware.clone())"),
        "all stages must retain the one shared control aggregator"
    );

    let stage_descriptor = source(&root, "crates/obzenflow_dsl/src/dsl/stage_descriptor.rs");
    assert!(!stage_descriptor.contains("create_handle_with_flow_middleware"));

    let errors = source(&root, "crates/obzenflow_dsl/src/dsl/error.rs");
    assert!(!errors.contains("PolicyMiddlewareOnFlowScope"));
    assert!(!errors.contains("MiddlewareResolution"));

    let binder = source(&root, "crates/obzenflow_dsl/src/dsl/binder.rs");
    assert!(!binder.contains("middleware_origin_from_source"));
    assert!(!binder.contains("MiddlewareOrigin"));

    let carrier = source(&root, "crates/obzenflow_adapters/src/middleware/carrier.rs");
    for retired in [
        "pub enum MiddlewareOrigin",
        "pub origin:",
        "push_origin",
        "origin.kind",
        "origin.family",
        "origin.flow_label",
        "origin.stage_label",
        "middleware-attachment:v3",
    ] {
        assert!(
            !carrier.contains(retired),
            "retired adapter provenance token resurfaced: {retired}"
        );
    }
    assert!(carrier.contains("middleware-attachment:v5"));

    let adapter_exports = source(&root, "crates/obzenflow_adapters/src/middleware/mod.rs");
    assert!(!adapter_exports.contains("MiddlewareOrigin"));
}
