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
    t.compile_fail("tests/compile_fail/effect_outcome_duplicate_member.rs");
    t.compile_fail("tests/compile_fail/effect_outcome_generic_carrier.rs");
    t.compile_fail("tests/compile_fail/effect_outcome_bad_crate_attribute.rs");
}

/// FLOWIP-120z: declared stage fact sets and pure stage output carriers
/// reject the shapes the type system cannot make unambiguous.
#[test]
fn stage_fact_sets_reject_invalid_shapes() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/stage_fact_set_duplicate_member.rs");
    t.compile_fail("tests/compile_fail/stage_output_duplicate_variant_leaf_set.rs");
    t.compile_fail("tests/compile_fail/stage_output_empty_variant_requires_attribute.rs");
    t.compile_fail("tests/compile_fail/stage_output_carrier_cannot_be_typed_payload.rs");
    t.compile_fail("tests/compile_fail/stage_output_generic_carrier.rs");
    t.compile_fail("tests/compile_fail/stage_output_duplicate_leaf_in_variant.rs");
}
