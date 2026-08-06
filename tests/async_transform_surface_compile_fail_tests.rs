// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public API gates for FLOWIP-134a's transform-surface consolidation.

#[test]
fn retired_and_internal_transform_surfaces_do_not_compile_downstream() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/async_transform_surface/*.rs");
    tests.pass("tests/compile_pass/supported_transform_surfaces.rs");
}
