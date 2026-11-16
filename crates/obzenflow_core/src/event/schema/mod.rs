// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event schema support
//!
//! This module provides type-safe event handling through the TypedPayload trait,
//! enabling compile-time event type checking.

mod effect_outcome;
mod fold;
mod member_set;
mod middleware_context_key;
mod outcome;
mod outcome_facts;
mod outcome_list;
mod outcomes;
mod typed_payload;

pub use effect_outcome::EffectOutcome;
// `Member` and `SubsetOf` appear in public bounds; the rest is doc(hidden)
// macro plumbing.
pub use member_set::{
    const_str_eq, require_distinct, AppendList, EmptySet, FactList, FactNameDisjoint, FactOr,
    Member, MemberAfter, MemberFound, MemberList, NoOtherFact, SubsetOf, SubsetProofEnd,
    SubsetProofStep, WithMember,
};
pub use middleware_context_key::MiddlewareContextKey;
pub use outcome::{
    assert_distinct_outcome_facts, DecodeCommitted, EffectfulOutcome, FactsOf, Outcome,
};
pub use outcome_facts::{
    decode_member_fact, missing_fact_group_error, OutcomeError, OutcomeFacts, TypedFact,
    TypedFactType,
};
pub use outcome_list::{
    outcome, CaseFacts, ListsOutcome, NoOtherOutcome, OutcomeList, OutcomeOr, SingleFactCases,
};
pub use outcomes::{OneFactOutcome, Outcomes, TransformOutcome};
pub use typed_payload::{TypedPayload, TypedPayloadError};

/// Compiler support for `outcome!` and `fold!`.
#[doc(hidden)]
pub mod __private {
    pub use super::fold::{fold_start, AllFolded, FoldStart, Folded, TakeFact};
    pub use super::outcome_list::Refused;
    pub use obzenflow_core_derive::sorted_outcomes;
}
