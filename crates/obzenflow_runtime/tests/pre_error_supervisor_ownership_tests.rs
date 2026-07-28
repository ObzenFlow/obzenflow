// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g source guards for supervisor-owned pre-error routing.

const STATEFUL_RUNNING: &str = include_str!("../src/stages/stateful/supervisor/running.rs");
const STATEFUL_DRAINING: &str = include_str!("../src/stages/stateful/supervisor/draining.rs");
const JOIN_HYDRATING: &str = include_str!("../src/stages/join/supervisor/hydrating.rs");
const JOIN_ENRICHING: &str = include_str!("../src/stages/join/supervisor/enriching.rs");
const JOIN_LIVE: &str = include_str!("../src/stages/join/supervisor/live.rs");
const JOIN_DRAINING: &str = include_str!("../src/stages/join/supervisor/draining.rs");

const PRE_ERROR_GUARD: &str = "if matches!(event.processing_info.status, ProcessingStatus::Error";

fn positions(source: &str, needle: &str) -> Vec<usize> {
    source
        .match_indices(needle)
        .map(|(index, _)| index)
        .collect()
}

fn assert_each_handler_call_has_an_immediately_preceding_guard(
    label: &str,
    source: &str,
    handler_call: &str,
    expected_paths: usize,
) {
    let guards = positions(source, PRE_ERROR_GUARD);
    let calls = positions(source, handler_call);

    assert_eq!(
        guards.len(),
        expected_paths,
        "{label} must guard every data-dispatch path"
    );
    assert_eq!(
        calls.len(),
        expected_paths,
        "{label} handler-dispatch inventory changed; review pre-error ownership"
    );

    for index in 0..expected_paths {
        assert!(
            guards[index] < calls[index],
            "{label} path {index} invokes the handler before checking pre-error status"
        );
        if index > 0 {
            assert!(
                calls[index - 1] < guards[index],
                "{label} path {index} is relying on another path's guard"
            );
        }
    }
}

#[test]
fn stateful_running_and_draining_guard_before_cloning_the_handler() {
    assert_each_handler_call_has_an_immediately_preceding_guard(
        "stateful running",
        STATEFUL_RUNNING,
        "let mut handler = (*ctx.handler).clone()",
        1,
    );
    assert_each_handler_call_has_an_immediately_preceding_guard(
        "stateful draining",
        STATEFUL_DRAINING,
        "let mut handler = (*ctx.handler).clone()",
        1,
    );
}

#[test]
fn every_join_side_and_lifecycle_path_guards_before_handler_dispatch() {
    for (label, source, expected_paths) in [
        ("finite reference hydration", JOIN_HYDRATING, 1),
        ("finite stream enrichment", JOIN_ENRICHING, 1),
        ("live reference and stream", JOIN_LIVE, 2),
        ("draining reference and stream", JOIN_DRAINING, 2),
    ] {
        assert_each_handler_call_has_an_immediately_preceding_guard(
            label,
            source,
            "ctx.handler.process_event",
            expected_paths,
        );
    }
}
