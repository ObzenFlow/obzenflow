// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115s structural guard: authority-bearing grammar positions must not
//! be flattened back into a role-routed middleware vector.

use std::fs;
use std::path::{Path, PathBuf};

fn source(root: &Path, relative: &str) -> String {
    fs::read_to_string(root.join(relative))
        .unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

#[test]
fn lowering_retains_closed_positions_and_lane_local_indices() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let descriptor = source(&root, "crates/obzenflow_dsl/src/dsl/stage_descriptor.rs");
    let binder = source(&root, "crates/obzenflow_dsl/src/dsl/binder.rs");
    let builder = source(&root, "crates/obzenflow_dsl/src/dsl/flow_builder.rs");
    let typing = source(&root, "crates/obzenflow_dsl/src/dsl/typing.rs");
    let carrier = source(&root, "crates/obzenflow_adapters/src/middleware/carrier.rs");

    for field in [
        "pub source_policies: Vec<Box<dyn MiddlewareFactory>>",
        "pub ingress_policy: Option<Box<dyn MiddlewareFactory>>",
        "pub sink_policies: Vec<Box<dyn MiddlewareFactory>>",
        "pub observers: Vec<Box<dyn MiddlewareFactory>>",
    ] {
        assert!(
            descriptor.contains(field),
            "descriptor lost its grammar-owned lane: {field}"
        );
    }
    for index in [
        "MiddlewareDeclarationIndex::source_with(source_policy_index)",
        "MiddlewareDeclarationIndex::ingress_with()",
        "MiddlewareDeclarationIndex::effect_with()",
        "MiddlewareDeclarationIndex::sink_with(sink_policy_index)",
        "MiddlewareDeclarationIndex::observers(observer_index)",
    ] {
        assert!(
            descriptor.contains(index),
            "lowering lost its lane-local declaration coordinate: {index}"
        );
    }

    for retired in [
        "transitional_policy_specs",
        "MiddlewareDeclarationScope",
        "middleware-attachment:v4",
    ] {
        assert!(
            !descriptor.contains(retired)
                && !binder.contains(retired)
                && !builder.contains(retired)
                && !carrier.contains(retired),
            "retired position-erasing vocabulary resurfaced: {retired}"
        );
    }
    assert!(
        !descriptor.contains("pub middleware: Vec<Box<dyn MiddlewareFactory>>"),
        "a flat descriptor middleware vector would erase attachment authority"
    );
    assert!(
        !binder.contains("declaration.is_control() && declaration.supports"),
        "the binder must not choose a control boundary from role and surfaces"
    );
    assert!(
        builder.contains("positioned_stage_middleware_factories()"),
        "preflight diagnostics must retain positions rather than flattening factories"
    );
    assert!(
        typing
            .matches("self.inner.positioned_stage_middleware_factories()")
            .count()
            >= 2,
        "descriptor wrappers must forward positions instead of falling back to Observers"
    );
    assert!(carrier.contains("middleware-attachment:v5"));
    for stable_label in [
        "\"source_with\"",
        "\"ingress_with\"",
        "\"effect_with\"",
        "\"sink_with\"",
        "\"observers\"",
    ] {
        assert!(
            carrier.contains(stable_label),
            "missing stable position label {stable_label}"
        );
    }
}
