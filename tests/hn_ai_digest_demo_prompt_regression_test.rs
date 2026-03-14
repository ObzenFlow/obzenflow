// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn hn_ai_digest_demo_prompt_discourages_story_echo_and_supports_group_caps() {
    let flow_source = include_str!("../examples/hn_ai_digest_demo/flow.rs");
    let config_source = include_str!("../examples/hn_ai_digest_demo/config.rs");

    assert!(
        config_source.contains("HN_AI_GROUP_MAX_STORIES"),
        "hn_ai_digest_demo should expose HN_AI_GROUP_MAX_STORIES"
    );
    assert!(
        flow_source.contains("IMPORTANT: Do not repeat the input story list."),
        "hn_ai_digest_demo chunk prompt should explicitly discourage echoing the story list"
    );
    assert!(
        flow_source.contains("Input stories (numbered; do not repeat):")
            || flow_source.contains("Input stories (numbered; do not repeat)"),
        "hn_ai_digest_demo chunk prompt should clearly label input stories"
    );
    assert!(
        flow_source.contains("strip_accidental_story_echo"),
        "hn_ai_digest_demo should defensively strip accidental story-list echo from chunk summaries"
    );
}
