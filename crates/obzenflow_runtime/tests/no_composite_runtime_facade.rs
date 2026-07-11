// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The B1 Moore projection is semantic core, not runtime signalling API.

#[test]
fn runtime_has_no_composite_facade_or_implementation_module() {
    let runtime_lib = include_str!("../src/lib.rs");
    assert!(!runtime_lib.contains("pub mod composite"));
    let old_module = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/composite");
    assert!(!old_module.join("mod.rs").exists());
    assert!(!old_module.join("projection.rs").exists());
}

#[test]
fn composite_metrics_store_and_builder_seam_remain_documentation_hidden() {
    let fsm = include_str!("../src/metrics/fsm.rs");
    let store = fsm.find("pub struct MetricsStore").unwrap();
    assert!(fsm[store.saturating_sub(120)..store].contains("#[doc(hidden)]"));

    let builder = include_str!("../src/metrics/builder.rs");
    let method = builder.find("pub fn with_composite_boundaries").unwrap();
    assert!(builder[method.saturating_sub(180)..method].contains("#[doc(hidden)]"));
}
