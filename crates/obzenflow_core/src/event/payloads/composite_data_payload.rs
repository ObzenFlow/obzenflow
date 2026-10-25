// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Existing durable composite protocol records. The event descriptor selects the
//! closed protocol shape; application values nested inside it remain untouched.

use crate::ai::{
    AiMapReduceChunkFailed, AiMapReduceGroupingEnded, AiMapReduceMapInput, AiMapReduceReduceFailed,
    AiMapReduceReduceInput, AiMapReduceTaggedPartial, AiMapReduceWindowFailed,
};
use serde::{Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum CompositeDataPayload {
    MapInput(AiMapReduceMapInput<Value>),
    GroupingEnded(AiMapReduceGroupingEnded),
    TaggedPartial(AiMapReduceTaggedPartial<Value, Value>),
    ChunkFailed(AiMapReduceChunkFailed),
    ReduceInput(AiMapReduceReduceInput<Value, Value>),
    WindowFailed(AiMapReduceWindowFailed),
    ReduceFailed(AiMapReduceReduceFailed),
}

impl CompositeDataPayload {
    /// The payload schema version this build writes and reads for a composite
    /// protocol event type.
    pub fn schema_version_of(event_type: &str) -> Option<std::num::NonZeroU32> {
        let version = match event_type {
            "ai.map_reduce.map_input" | "ai.map_reduce.tagged_partial" => 2,
            "ai.map_reduce.chunk_failed" | "ai.map_reduce.reduce_input" => 3,
            "ai.map_reduce.grouping_ended"
            | "ai.map_reduce.window_failed"
            | "ai.map_reduce.reduce_failed" => 1,
            _ => return None,
        };
        std::num::NonZeroU32::new(version)
    }

    pub fn payload_schema_version(&self) -> std::num::NonZeroU32 {
        Self::schema_version_of(self.event_type())
            .expect("every composite payload variant has a schema version")
    }

    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::MapInput(_) => "ai.map_reduce.map_input",
            Self::GroupingEnded(_) => "ai.map_reduce.grouping_ended",
            Self::TaggedPartial(_) => "ai.map_reduce.tagged_partial",
            Self::ChunkFailed(_) => "ai.map_reduce.chunk_failed",
            Self::ReduceInput(_) => "ai.map_reduce.reduce_input",
            Self::WindowFailed(_) => "ai.map_reduce.window_failed",
            Self::ReduceFailed(_) => "ai.map_reduce.reduce_failed",
        }
    }

    pub fn decode(event_type: &str, payload: Value) -> Result<Self, serde_json::Error> {
        match event_type {
            "ai.map_reduce.map_input" => serde_json::from_value(payload).map(Self::MapInput),
            "ai.map_reduce.grouping_ended" => {
                serde_json::from_value(payload).map(Self::GroupingEnded)
            }
            "ai.map_reduce.tagged_partial" => {
                serde_json::from_value(payload).map(Self::TaggedPartial)
            }
            "ai.map_reduce.chunk_failed" => serde_json::from_value(payload).map(Self::ChunkFailed),
            "ai.map_reduce.reduce_input" => serde_json::from_value(payload).map(Self::ReduceInput),
            "ai.map_reduce.window_failed" => {
                serde_json::from_value(payload).map(Self::WindowFailed)
            }
            "ai.map_reduce.reduce_failed" => {
                serde_json::from_value(payload).map(Self::ReduceFailed)
            }
            _ => Err(<serde_json::Error as serde::de::Error>::custom(
                "event descriptor does not match payload",
            )),
        }
    }
}

impl Serialize for CompositeDataPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::MapInput(payload) => payload.serialize(serializer),
            Self::GroupingEnded(payload) => payload.serialize(serializer),
            Self::TaggedPartial(payload) => payload.serialize(serializer),
            Self::ChunkFailed(payload) => payload.serialize(serializer),
            Self::ReduceInput(payload) => payload.serialize(serializer),
            Self::WindowFailed(payload) => payload.serialize(serializer),
            Self::ReduceFailed(payload) => payload.serialize(serializer),
        }
    }
}
