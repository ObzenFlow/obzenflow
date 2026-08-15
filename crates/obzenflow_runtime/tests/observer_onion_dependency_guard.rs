// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn runtime_manifest_has_no_outward_observer_layer_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in ["obzenflow_adapters", "obzenflow_dsl", "obzenflow_infra"] {
        assert!(
            !manifest.contains(forbidden),
            "runtime must not depend on outward layer {forbidden}"
        );
    }
}
