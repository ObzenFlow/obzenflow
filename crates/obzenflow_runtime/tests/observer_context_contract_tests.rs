// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2 guards for the public observer-coordinate contract.

const OBSERVER_PORTS: &str = include_str!("../src/stages/observer/ports.rs");
const OBSERVER_MODULE: &str = include_str!("../src/stages/observer/mod.rs");
const JOIN_COMMON: &str = include_str!("../src/stages/join/supervisor/common.rs");
const JOIN_LIVE: &str = include_str!("../src/stages/join/supervisor/live.rs");

#[test]
fn speculative_join_merge_metadata_cannot_reenter_the_observer_contract() {
    let sources = [
        ("observer ports", OBSERVER_PORTS),
        ("observer exports", OBSERVER_MODULE),
        ("join observer construction", JOIN_COMMON),
        ("join live dispatch", JOIN_LIVE),
    ];
    let forbidden = [
        "JoinCanonicalMergeMetadata",
        "canonical_merge: Option<",
        "selected_feed: Option<String>",
        "reader_index: Option<usize>",
    ];

    for (source_name, source) in sources {
        for symbol in forbidden {
            assert!(
                !source.contains(symbol),
                "{source_name} must not expose speculative observer metadata `{symbol}`"
            );
        }
    }
}
