// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed application payloads, stage output carriers, and effect outcome facts.
//!
//! Derives explicitly select this schema path, including when the dependency is
//! renamed: `#[stage_output(schema = obzenflow::schema)]` or
//! `#[effect_outcome(schema = obzenflow::schema)]`.

pub use obzenflow_core::event::schema::{
    DeclaredStageFactSet, EffectOutcomeFacts, Member, OneFactStageOutput, StageFactSet,
    StageOutputFacts, StageOutputs, SubsetOf, TypedFact, TypedFactSet, TypedFactSetError,
    TypedFactType, TypedPayload, TypedPayloadError,
};
pub use obzenflow_core::stage_fact_set;
pub use obzenflow_core::{ChainEvent, EventId, EventType, FlowId, StageId, StageKey, WriterId};

/// Compiler support for schema-path derives; not an application authoring API.
#[doc(hidden)]
pub use obzenflow_core::event::schema::__private;
