// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn hn_ai_digest_demo_does_not_import_rig_clients_directly() {
    let source = include_str!("../examples/hn_ai_digest_demo/flow.rs");

    assert!(
        !source.contains("RigChatClient"),
        "hn_ai_digest_demo should not reference RigChatClient directly"
    );
    assert!(
        !source.contains("obzenflow_infra::ai"),
        "hn_ai_digest_demo should not import from obzenflow_infra::ai"
    );
    assert!(
        !source.contains("Arc<dyn ChatClient>"),
        "hn_ai_digest_demo should not require manual Arc<dyn ChatClient> wiring"
    );
}
