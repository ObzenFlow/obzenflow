// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event schema support
//!
//! This module provides type-safe event handling through the TypedPayload trait,
//! enabling compile-time event type checking.

mod effect_outcome;
mod member_set;
mod middleware_context_key;
mod stage_fact_set;
mod stage_output;
mod typed_fact_set;
mod typed_middleware_event;
mod typed_payload;

pub use effect_outcome::EffectOutcomeFacts;
// FLOWIP-120z proof vocabulary: `Member`/`SubsetOf` appear in public bounds;
// the rest is doc(hidden) macro and derive plumbing.
pub use member_set::{
    const_str_eq, require_distinct, EmptySet, FactList, FactNameDisjoint, Member, MemberAfter,
    MemberFound, MemberList, SubsetOf, SubsetProofEnd, SubsetProofStep, WithMember,
};
// The derive shares the trait's name, serde-style: one import brings both.
pub use middleware_context_key::MiddlewareContextKey;
pub use obzenflow_derive::{EffectOutcomeFacts, StageOutputFacts};
pub use stage_fact_set::{assert_distinct_stage_fact_set, DeclaredStageFactSet, StageFactSet};
pub use stage_output::{OneFactStageOutput, StageOutputFacts};
pub use typed_fact_set::{
    decode_member_fact, missing_fact_group_error, sum_group_arity_error, TypedFact, TypedFactSet,
    TypedFactSetError, TypedFactType,
};
pub use typed_middleware_event::TypedMiddlewareEvent;
pub use typed_payload::TypedPayload;
