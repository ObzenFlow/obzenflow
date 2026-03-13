// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-internal transport payloads for AI map-reduce composites.

use super::ChunkPlanningSummary;
use crate::{EventId, TypedPayload};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiMapReducePlanningManifest {
    pub job_key: EventId,
    pub chunk_count: usize,
    pub planning: ChunkPlanningSummary,
}

impl TypedPayload for AiMapReducePlanningManifest {
    const EVENT_TYPE: &'static str = "ai.map_reduce.planning_manifest";
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
