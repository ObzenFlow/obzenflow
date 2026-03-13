// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-internal transport payloads for AI map-reduce composites.

use super::ChunkPlanningSummary;
use crate::{EventId, TypedPayload};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Serialize, Deserialize)]
pub struct Many<T> {
    pub items: Vec<T>,
    pub planning: ChunkPlanningSummary,
}

impl<T> Default for Many<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        }
    }
}

impl<T> std::fmt::Debug for Many<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Many")
            .field("items_len", &self.items.len())
            .field("planning", &self.planning)
            .finish()
    }
}

impl<T> TypedPayload for Many<T>
where
    T: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.many";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReducePlanningManifest {
    pub job_key: EventId,
    pub chunk_count: usize,
    pub planning: ChunkPlanningSummary,
    /// Raw JSON payload of the outer seed event.
    ///
    /// This is used by the composite to provide the reduce handler with the
    /// original input without forcing users to reconstruct it from partials.
    pub seed_payload: Value,
    /// The seed's event type string, for debugging and observability.
    pub seed_event_type: String,
}

impl TypedPayload for AiMapReducePlanningManifest {
    const EVENT_TYPE: &'static str = "ai.map_reduce.planning_manifest";
    const SCHEMA_VERSION: u32 = 1;
}

/// Internal transport payload delivered to the finalise (reduce) stage.
///
/// The key ergonomic constraint is that the user-facing reduce contract is
/// `(Seed, Collected) -> Out`. The composite therefore pairs the original seed
/// with the collected partials before calling the user handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiMapReduceReduceInput<Seed, Collected> {
    pub seed: Option<Seed>,
    pub collected: Collected,
    pub planning: ChunkPlanningSummary,
}

impl<Seed, Collected> TypedPayload for AiMapReduceReduceInput<Seed, Collected>
where
    Seed: Serialize + DeserializeOwned,
    Collected: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.reduce_input";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiMapReduceTaggedPartial<T> {
    pub job_key: EventId,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub partial: T,
}

impl<T> TypedPayload for AiMapReduceTaggedPartial<T>
where
    T: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.map_reduce.tagged_partial";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReduceChunkFailed {
    pub job_key: EventId,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub reason: String,
}

impl TypedPayload for AiMapReduceChunkFailed {
    const EVENT_TYPE: &'static str = "ai.map_reduce.chunk_failed";
    const SCHEMA_VERSION: u32 = 1;
}
