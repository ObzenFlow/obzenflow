// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! `ChainEvent` is ObzenFlow's canonical wide event type.
//!
//! It is the public event contract where the framework's CHAIN philosophy
//! becomes concrete:
//!
//! - Causality: `causality`, correlation, and cycle metadata preserve links
//!   between state changes.
//! - History: immutable journaling and `replay_context` preserve durable
//!   state-transition history.
//! - Agency: `writer_id` records which stage or service emitted the event,
//!   leaving room for richer provenance later.
//! - Intent: `intent` keeps goals explicit rather than inferred.
//! - Narrative: wide-event observability and contextual metadata make
//!   reconstruction possible.
//!
//! This module split is structural only. It does not change `ChainEvent`
//! semantics or the existing public constructor surface.
//!
//! `event_type()` remains the richer user-facing classifier, while
//! `event_type_name()` stays the coarse, zero-allocation fast-path classifier
//! used in journal and metrics code paths.

mod factory;
mod journal;
mod lineage;
mod model;
mod params;

pub use factory::ChainEventFactory;
pub use model::{ChainEvent, ChainEventContent};
pub use params::{
    CircuitBreakerSummaryEventParams, ConsumptionFinalEventParams, ConsumptionProgressEventParams,
    SourceContractEventParams,
};

#[cfg(test)]
mod tests;
