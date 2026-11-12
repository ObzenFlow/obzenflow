// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115r compile coverage for every surviving flow-macro arm.

#[cfg(feature = "test-support")]
use obzenflow_dsl::test_flow;
use obzenflow_dsl::{flow, FlowDefinition};
use obzenflow_infra::journal::memory_journals;

fn deferred_flow<F>(factory: F) -> FlowDefinition
where
    F: FnOnce() -> FlowDefinition + Send + 'static,
{
    FlowDefinition::materialize(move |_| Ok(factory()))
}

#[test]
fn flow_surviving_arm_matrix_compiles() {
    let definitions = [
        deferred_flow(move || {
            flow! {
                name: "flow_named",
                journals: memory_journals(),
                stages: {},
                topology: {}
            }
        }),
        deferred_flow(move || {
            flow! {
                journals: memory_journals(),
                stages: {},
                topology: {}
            }
        }),
    ];
    drop(definitions);
}

#[cfg(feature = "test-support")]
#[test]
fn test_flow_surviving_arm_matrix_compiles() {
    let futures = (
        test_flow! {
            name: "test_flow_named",
            journals: memory_journals(),
            stages: {},
            topology: {}
        },
        test_flow! {
            journals: memory_journals(),
            stages: {},
            topology: {}
        },
        test_flow! {
            build_context: obzenflow_runtime::run_context::FlowBuildContext::for_tests(),
            name: "test_flow_named_with_build_context",
            journals: memory_journals(),
            stages: {},
            topology: {}
        },
        test_flow! {
            build_context: obzenflow_runtime::run_context::FlowBuildContext::for_tests(),
            journals: memory_journals(),
            stages: {},
            topology: {}
        },
    );
    drop(futures);
}
