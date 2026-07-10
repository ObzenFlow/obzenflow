// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128a D5 compile-time contract: a `stages:` entry that is neither a
//! stage descriptor nor a composite descriptor fails at the `IntoFlowMember`
//! seam with a trait-bound error, not a deferred runtime `Err`.

#[test]
fn stages_block_rejects_non_member_expressions() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/flow_member_rejects_non_member.rs");
}
