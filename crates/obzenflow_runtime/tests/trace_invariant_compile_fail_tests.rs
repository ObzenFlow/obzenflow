// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095l Gap 12: the `TraceInvariant` witness cannot be forged from another
//! crate. The type-enforced seal is that `TraceInvarianceProof` has a private field and
//! its minter is `pub(crate)`, so neither is reachable here (integration tests compile
//! as a separate crate).
//!
//! Deliberately NOT asserted: `InputOrderSemantics::__trace_invariant_proven()` is
//! `pub`, because the `#[trace_invariant]` attribute expands in the user's crate and
//! must call it to mint the proof. That public path is the convention-grade escape
//! (`#[doc(hidden)]`, a dunder name, documented as unsupported), the same trust posture
//! as effect safety. The seal the type system enforces is what these fixtures pin.

#[test]
fn trace_invariant_witness_cannot_be_forged_cross_crate() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/trace_invariant_minter_is_in_crate.rs");
    t.compile_fail("tests/compile_fail/trace_invariant_variant_needs_proof.rs");
}
