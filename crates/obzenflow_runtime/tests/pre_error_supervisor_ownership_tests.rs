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

const PRE_ERROR_GUARD: &str = "if matches!(event.processing.status, ProcessingStatus::Error";
const FATAL_BRANCH: &str = "if let Some(fatal) = err.as_fatal()";

fn positions(source: &str, needle: &str) -> Vec<usize> {
    // Resource ownership can introduce multiline handler access. Compare the
    // same token spelling independently of rustfmt's line wrapping.
    let compact = |text: &str| {
        text.chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>()
    };
    let source = compact(source);
    let needle = compact(needle);
    source
        .match_indices(&needle)
        .map(|(index, _)| index)
        .collect()
}

fn assert_each_handler_call_has_an_immediately_preceding_guard(
    label: &str,
    source: &str,
    handler_calls: &[&str],
    expected_paths: usize,
) {
    let guards = positions(source, PRE_ERROR_GUARD);
    let mut calls = handler_calls
        .iter()
        .flat_map(|handler_call| positions(source, handler_call))
        .collect::<Vec<_>>();
    calls.sort_unstable();

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

fn assert_each_handler_call_has_its_own_following_fatal_branch(
    label: &str,
    source: &str,
    handler_calls: &[&str],
    expected_paths: usize,
) {
    let mut calls = handler_calls
        .iter()
        .flat_map(|handler_call| positions(source, handler_call))
        .collect::<Vec<_>>();
    calls.sort_unstable();
    let fatal_branches = positions(source, FATAL_BRANCH);

    assert_eq!(
        calls.len(),
        expected_paths,
        "{label} typed-dispatch inventory changed; review fatal routing"
    );
    assert_eq!(
        fatal_branches.len(),
        expected_paths,
        "{label} must intercept Fatal at every typed dispatch position"
    );

    for index in 0..expected_paths {
        assert!(
            calls[index] < fatal_branches[index],
            "{label} path {index} does not inspect the handler result for Fatal"
        );
        if index + 1 < expected_paths {
            assert!(
                fatal_branches[index] < calls[index + 1],
                "{label} path {index} is relying on a later path's Fatal branch"
            );
        }
    }
}

#[test]
fn stateful_running_and_draining_guard_before_cloning_the_handler() {
    assert_each_handler_call_has_an_immediately_preceding_guard(
        "stateful running",
        STATEFUL_RUNNING,
        &["let mut handler = (**ctx.handler.as_ref().expect(\"handler available before cleanup\")).clone()"],
        1,
    );
    assert_each_handler_call_has_an_immediately_preceding_guard(
        "stateful draining",
        STATEFUL_DRAINING,
        &["let mut handler = (**ctx.handler.as_ref().expect(\"handler available before cleanup\")).clone()"],
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
            &[".process_reference(", ".process_stream("],
            expected_paths,
        );
    }
}

#[test]
fn every_join_dispatch_position_promotes_fatal_to_stage_failure() {
    assert_each_handler_call_has_its_own_following_fatal_branch(
        "finite reference hydration",
        JOIN_HYDRATING,
        &[".process_reference("],
        1,
    );
    assert_each_handler_call_has_its_own_following_fatal_branch(
        "finite stream enrichment",
        JOIN_ENRICHING,
        &[".process_stream("],
        1,
    );
    assert_each_handler_call_has_its_own_following_fatal_branch(
        "live reference and stream",
        JOIN_LIVE,
        &[".process_reference(", ".process_stream("],
        2,
    );
    assert_each_handler_call_has_its_own_following_fatal_branch(
        "draining input and terminal hooks",
        JOIN_DRAINING,
        &[
            ".process_reference(",
            ".process_stream(",
            "match handler.on_stream_eof(",
            "match handler.drain(",
        ],
        6,
    );
}
