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
mod outcome_list;
mod stage_fact_set;
mod stage_output;
mod typed_fact_set;
mod typed_payload;

pub use effect_outcome::EffectOutcomeFacts;
// `Member` and `SubsetOf` appear in public bounds; the rest is doc(hidden)
// macro plumbing.
pub use member_set::{
    const_str_eq, require_distinct, AppendList, EmptySet, FactList, FactNameDisjoint, FactOr,
    Member, MemberAfter, MemberFound, MemberList, NoOtherFact, SubsetOf, SubsetProofEnd,
    SubsetProofStep, WithMember,
};
pub use middleware_context_key::MiddlewareContextKey;
pub use outcome_list::{
    outcome, ListsOutcome, NoOtherOutcome, OutcomeFacts, OutcomeList, OutcomeOr, SingleFactOutcomes,
};
pub use stage_fact_set::{
    assert_distinct_stage_fact_set, DecodeCommitted, EffectfulStageOutput, FactsOf, StageFactSet,
};
pub use stage_output::{OneFactStageOutput, StageOutputFacts, StageOutputs};
pub use typed_fact_set::{
    decode_member_fact, missing_fact_group_error, TypedFact, TypedFactSet, TypedFactSetError,
    TypedFactType,
};
pub use typed_payload::{TypedPayload, TypedPayloadError};

/// Compiler support for `outcome!` and `fold!`.
#[doc(hidden)]
pub mod __private {
    pub use super::fold::{fold_start, AllFolded, FoldStart, Folded, TakeFact};
    pub use super::outcome_list::Refused;
    pub use obzenflow_core_derive::sorted_outcomes;
}
