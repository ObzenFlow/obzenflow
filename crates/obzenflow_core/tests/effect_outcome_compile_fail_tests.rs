// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[test]
fn effect_outcome_rejects_invalid_carrier_shapes() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/effect_outcome_multi_field_variant.rs");
    t.compile_fail("tests/compile_fail/effect_outcome_unit_variant.rs");
    t.compile_fail("tests/compile_fail/effect_outcome_struct_variant.rs");
    t.compile_fail("tests/compile_fail/effect_outcome_tuple_struct.rs");
    t.compile_fail("tests/compile_fail/effect_outcome_carrier_cannot_be_typed_payload.rs");
}
