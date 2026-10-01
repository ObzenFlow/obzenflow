// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Existing durable composite protocol records. The event descriptor selects the
//! closed protocol shape; application values nested inside it remain untouched.

use crate::ai::{
    AiMapReduceChunkFailed, AiMapReduceFinaliseFailed, AiMapReduceJobFailed, AiMapReduceMapInput,
    AiMapReducePlanningFailed, AiMapReducePlanningManifest, AiMapReduceReduceInput,
    AiMapReduceTaggedPartial, ChunkEnvelope,
};
use serde::{Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum CompositeDataPayload {
    MapInput(AiMapReduceMapInput<ChunkEnvelope<Value>>),
    PlanningManifest(AiMapReducePlanningManifest),
    TaggedPartial(AiMapReduceTaggedPartial<Value>),
    ReduceInput(AiMapReduceReduceInput<Value, Value>),
    PlanningFailed(AiMapReducePlanningFailed),
    ChunkFailed(AiMapReduceChunkFailed),
    JobFailed(AiMapReduceJobFailed),
    FinaliseFailed(AiMapReduceFinaliseFailed),
}

impl CompositeDataPayload {
    pub const fn payload_schema_version(&self) -> std::num::NonZeroU32 {
        match self {
            Self::ReduceInput(_) | Self::ChunkFailed(_) => std::num::NonZeroU32::new(2).unwrap(),
            _ => std::num::NonZeroU32::MIN,
        }
    }

    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::MapInput(_) => "ai.map_reduce.map_input",
            Self::PlanningManifest(_) => "ai.map_reduce.planning_manifest",
            Self::TaggedPartial(_) => "ai.map_reduce.tagged_partial",
            Self::ReduceInput(_) => "ai.map_reduce.reduce_input",
            Self::PlanningFailed(_) => "ai.map_reduce.planning_failed",
            Self::ChunkFailed(_) => "ai.map_reduce.chunk_failed",
            Self::JobFailed(_) => "ai.map_reduce.job_failed",
            Self::FinaliseFailed(_) => "ai.map_reduce.finalise_failed",
        }
    }

    pub fn decode(event_type: &str, payload: Value) -> Result<Self, serde_json::Error> {
        match event_type {
            "ai.map_reduce.map_input" => serde_json::from_value(payload).map(Self::MapInput),
            "ai.map_reduce.planning_manifest" => {
                serde_json::from_value(payload).map(Self::PlanningManifest)
            }
            "ai.map_reduce.tagged_partial" => {
                serde_json::from_value(payload).map(Self::TaggedPartial)
            }
            "ai.map_reduce.reduce_input" => serde_json::from_value(payload).map(Self::ReduceInput),
            "ai.map_reduce.planning_failed" => {
                serde_json::from_value(payload).map(Self::PlanningFailed)
            }
            "ai.map_reduce.chunk_failed" => serde_json::from_value(payload).map(Self::ChunkFailed),
            "ai.map_reduce.job_failed" => serde_json::from_value(payload).map(Self::JobFailed),
            "ai.map_reduce.finalise_failed" => {
                serde_json::from_value(payload).map(Self::FinaliseFailed)
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
            Self::PlanningManifest(payload) => payload.serialize(serializer),
            Self::TaggedPartial(payload) => payload.serialize(serializer),
            Self::ReduceInput(payload) => payload.serialize(serializer),
            Self::PlanningFailed(payload) => payload.serialize(serializer),
            Self::ChunkFailed(payload) => payload.serialize(serializer),
            Self::JobFailed(payload) => payload.serialize(serializer),
            Self::FinaliseFailed(payload) => payload.serialize(serializer),
        }
    }
}
