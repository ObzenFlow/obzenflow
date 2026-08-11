// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134g compile-time source witness matrix.

#[test]
fn every_source_variant_requires_the_typed_witness_and_exact_output_set() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/compile_fail/source_witness/*.rs");
}
