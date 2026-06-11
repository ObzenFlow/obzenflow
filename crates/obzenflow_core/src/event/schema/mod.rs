// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event schema support
//!
//! This module provides type-safe event handling through the TypedPayload trait,
//! enabling compile-time event type checking.

mod effect_outcome;
mod middleware_context_key;
mod typed_fact_set;
mod typed_middleware_event;
mod typed_payload;

pub use effect_outcome::EffectOutcomeFacts;
pub use middleware_context_key::MiddlewareContextKey;
pub use typed_fact_set::{
    decode_member_fact, missing_fact_group_error, sum_group_arity_error, TypedFact, TypedFactSet,
    TypedFactSetError, TypedFactType,
};
pub use typed_middleware_event::TypedMiddlewareEvent;
pub use typed_payload::TypedPayload;
