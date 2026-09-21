// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed application payloads, stage output carriers, and effect outcome facts.
//!
//! Carrier derives select the schema facade explicitly. For an application with
//! `ValidatedOrder`, `InvalidOrder`, and `OrderCancelled` payloads:
//!
//! ```rust,ignore
//! use obzenflow::schema::StageOutputFacts;
//!
//! #[derive(Debug, Clone, StageOutputFacts)]
//! #[stage_output(schema = obzenflow::schema)]
//! enum ValidationOutput {
//!     Valid(ValidatedOrder),
//!     Invalid { invalid: InvalidOrder, cancelled: OrderCancelled },
//! }
//! ```
//!
//! `EffectOutcomeFacts` uses `#[effect_outcome(schema = obzenflow::schema)]`.
//! If the dependency is renamed to `of`, use `of::schema` in either attribute.
//! Declare fact sets with [`stage_fact_set!`]; effect sets use
//! [`crate::effects::effect_set!`].

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
