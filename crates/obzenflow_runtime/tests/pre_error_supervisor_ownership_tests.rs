// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source guards: an error-marked input is refused by the subscription that
//! would deliver it, so no family supervisor routes one itself.

const TRANSFORM_RUNNING: &str = include_str!("../src/stages/transform/supervisor/running.rs");
const TRANSFORM_DRAINING: &str = include_str!("../src/stages/transform/supervisor/draining.rs");
const TRANSFORM_DIRECT_FACT: &str =
    include_str!("../src/stages/transform/supervisor/direct_fact_continuation.rs");
const MERGE_COMMON: &str = include_str!("../src/stages/merge/supervisor/common.rs");
const STATEFUL_RUNNING: &str = include_str!("../src/stages/stateful/supervisor/running.rs");
const STATEFUL_DRAINING: &str = include_str!("../src/stages/stateful/supervisor/draining.rs");
const JOIN_HYDRATING: &str = include_str!("../src/stages/join/supervisor/hydrating.rs");
const JOIN_ENRICHING: &str = include_str!("../src/stages/join/supervisor/enriching.rs");
const JOIN_LIVE: &str = include_str!("../src/stages/join/supervisor/live.rs");
const JOIN_DRAINING: &str = include_str!("../src/stages/join/supervisor/draining.rs");

const PRE_ERROR_GUARD: &str = "if matches!(event.processing.status, ProcessingStatus::Error";
const STAGE_FATAL_RECORD: &str = "common::record_join_stage_fatal(";

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
    let fatal_branches = positions(source, STAGE_FATAL_RECORD);

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
            "{label} path {index} does not record its handler's Fatal as a stage fatal"
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
fn no_family_supervisor_branches_on_an_error_marked_input() {
    for (label, source) in [
        ("transform running", TRANSFORM_RUNNING),
        ("transform draining", TRANSFORM_DRAINING),
        ("transform direct facts", TRANSFORM_DIRECT_FACT),
        ("merge", MERGE_COMMON),
        ("stateful running", STATEFUL_RUNNING),
        ("stateful draining", STATEFUL_DRAINING),
        ("finite reference hydration", JOIN_HYDRATING),
        ("finite stream enrichment", JOIN_ENRICHING),
        ("live reference and stream", JOIN_LIVE),
        ("draining reference and stream", JOIN_DRAINING),
    ] {
        assert!(
            positions(source, PRE_ERROR_GUARD).is_empty(),
            "{label} must leave an error-marked input to its subscription's refusal"
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
        2,
    );
}
