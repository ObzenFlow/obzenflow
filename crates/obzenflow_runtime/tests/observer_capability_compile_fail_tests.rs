// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn ordinary_observers_have_only_read_only_content_evidence_capabilities() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/observer_cannot_mutate_outputs.rs");
    t.compile_fail("tests/compile_fail/observer_report_rejects_chain_event.rs");
    t.compile_fail("tests/compile_fail/observer_evidence_rejects_forbidden_family.rs");
}
