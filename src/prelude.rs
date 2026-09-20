// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The small vocabulary shared by ordinary flow applications.
//!
//! Import specialised capabilities explicitly from their owning facade module.

pub use crate::application::FlowApplication;
pub use crate::error::HandlerError;
pub use crate::flow::{
    ai_map_reduce, async_infinite_source, async_source, effectful_stateful, effectful_transform,
    flow, handler_set, inference, infinite_source, join, placeholder, sink, source, stateful,
    transform, FlowDefinition,
};
pub use crate::schema::{EffectOutcomeFacts, StageOutputFacts, TypedPayload};
