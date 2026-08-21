// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122a conformance-verdict construction authority.

#[test]
fn fixtures_cannot_mint_harness_reports_failures_or_run_evidence() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/sink_conformance_authority/*.rs");
}
