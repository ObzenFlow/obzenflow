// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed application payloads and outcome lists.
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
//! `process` returns exactly one of them, built with `outcome(...)`. A port's
//! answer and a command's `Output` are written the same way.

pub use obzenflow_core::event::schema::{
    EffectOutcomeFacts, Member, OneFactStageOutput, StageFactSet, StageOutputFacts, StageOutputs,
    SubsetOf, TypedFact, TypedFactSet, TypedFactSetError, TypedFactType, TypedPayload,
    TypedPayloadError,
};
pub use obzenflow_core::outcome;
pub use obzenflow_core::{ChainEvent, EventId, EventType, FlowId, StageId, StageKey, WriterId};
