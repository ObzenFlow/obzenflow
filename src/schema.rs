// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed application payloads, outcome lists and fact sets.
//!
//! For an application with `ValidatedOrder`, `InvalidOrder`, and `OrderCancelled`
//! payloads, a transform's `Output` lists the outcomes it may return:
//!
//! ```rust,ignore
//! use obzenflow::schema::outcome;
//!
//! type Output = outcome![ValidatedOrder | (InvalidOrder, OrderCancelled)];
//! ```
//!
//! `process` returns exactly one of them, built with `outcome(...)`, and a port's
//! answer is written the same way. Declare fact sets with [`stage_fact_set!`].

pub use obzenflow_core::event::schema::{
    DeclaredStageFactSet, EffectOutcomeFacts, Member, OneFactStageOutput, StageFactSet,
    StageOutputFacts, StageOutputs, SubsetOf, TypedFact, TypedFactSet, TypedFactSetError,
    TypedFactType, TypedPayload, TypedPayloadError,
};
pub use obzenflow_core::{outcome, stage_fact_set};
pub use obzenflow_core::{ChainEvent, EventId, EventType, FlowId, StageId, StageKey, WriterId};
