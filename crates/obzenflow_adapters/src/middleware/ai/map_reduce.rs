// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-owned middleware used by the AI map-reduce composite lowering.
//!
//! These middleware adapters are intentionally narrow:
//! - `chunk` stage: emit a framework-internal planning manifest per input job.
//! - `map` stage: drop manifests, tag partial outputs with job/chunk metadata,
//!   and emit a terminal failure marker when a chunk produces no partial output.

use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::ai::{
    AiMapReduceChunkFailed, AiMapReducePlanningManifest, AiMapReduceTaggedPartial,
    ChunkPlanningSummary,
};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::observability::AiChunkingSnapshot;
use obzenflow_core::event::payloads::observability_payload::{MetricsLifecycle, ObservabilityPayload};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, EventId, TypedPayload};
use obzenflow_runtime::pipeline::config::StageConfig;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::marker::PhantomData;
use std::sync::Arc;

const MAP_FAILURE_EVENT_TYPE: &str = "ai.map_reduce.map_failed.v1";
const CHUNK_FAILED_EVENT_TYPE: &str = "ai.map_reduce.chunk_failed.v1";
const TAGGED_PARTIAL_EVENT_TYPE: &str = "ai.map_reduce.tagged_partial.v1";

const BAGGAGE_JOB_KEY: &str = "ai.map_reduce.job_key";
const BAGGAGE_CHUNK_INDEX: &str = "ai.map_reduce.chunk_index";
const BAGGAGE_CHUNK_COUNT: &str = "ai.map_reduce.chunk_count";

fn job_key_from_chunk_event(chunk_event: &ChainEvent) -> EventId {
    chunk_event
        .causality
        .parent_ids
        .first()
        .copied()
        .unwrap_or(chunk_event.id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AiMapReduceMapFailed {
    job_key: EventId,
    chunk_index: usize,
    chunk_count: usize,
    reason: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChunkEnvelopeIndexCount {
    chunk_index: usize,
    chunk_count: usize,
}

fn remove_control_events_by_type(ctx: &mut MiddlewareContext, event_type: &str) {
    ctx.control_events.retain(|e| {
        match &e.content {
            ChainEventContent::Data {
                event_type: actual,
                ..
            } => actual != event_type,
            _ => true,
        }
    });
}

fn has_typed_output<T: TypedPayload>(outputs: &[ChainEvent]) -> bool {
    outputs.iter().any(|event| match &event.content {
        ChainEventContent::Data { event_type, .. } => T::event_type_matches(event_type),
        _ => false,
    })
}

fn should_retry(ctx: &MiddlewareContext) -> bool {
    ctx.get_baggage("circuit_breaker.should_retry")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn update_chunk_failed_reason(
    event: &mut ChainEvent,
    ctx: &MiddlewareContext,
    default_reason: &str,
) {
    if !matches!(&event.content, ChainEventContent::Data { .. }) {
        return;
    }

    let (event_type, payload) = match &mut event.content {
        ChainEventContent::Data { event_type, payload } => (event_type, payload),
        _ => return,
    };

    if !AiMapReduceChunkFailed::event_type_matches(event_type) {
        return;
    }

    let Ok(mut decoded) = serde_json::from_value::<AiMapReduceChunkFailed>(payload.clone()) else {
        return;
    };

    if decoded.reason != default_reason {
        return;
    }

    if let Some(rejected) = ctx.find_event("circuit_breaker", "rejected") {
        let reason = rejected
            .data
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("rejected");
        decoded.reason = format!("circuit_breaker:{reason}");
        if let Ok(next) = serde_json::to_value(&decoded) {
            *payload = next;
        }
        return;
    }

    if let Some(throttled) = ctx.find_event("rate_limiter", "throttled") {
        let reason = throttled
            .data
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("throttled");
        decoded.reason = format!("rate_limiter:{reason}");
        if let Ok(next) = serde_json::to_value(&decoded) {
            *payload = next;
        }
    }
}

// ============================================================================
// Chunk stage: emit planning manifest
// ============================================================================

#[derive(Debug, Clone)]
pub struct AiMapReduceChunkManifestMiddleware<Chunk> {
    _phantom: PhantomData<Chunk>,
}

impl<Chunk> AiMapReduceChunkManifestMiddleware<Chunk> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<Chunk> Default for AiMapReduceChunkManifestMiddleware<Chunk> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Chunk> Middleware for AiMapReduceChunkManifestMiddleware<Chunk>
where
    Chunk: TypedPayload + Send + Sync,
{
    fn middleware_name(&self) -> &'static str {
        "ai_map_reduce.chunk_manifest"
    }

    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        MiddlewareAction::Continue
    }

    fn post_handle(&self, event: &ChainEvent, outputs: &[ChainEvent], ctx: &mut MiddlewareContext) {
        // Derive chunk_count + planning summary from the chunker's outputs.
        let mut chunk_count = outputs
            .iter()
            .filter(|out| match &out.content {
                ChainEventContent::Data { event_type, .. } => Chunk::event_type_matches(event_type),
                _ => false,
            })
            .count();

        let mut planning = None::<ChunkPlanningSummary>;

        for out in outputs {
            let ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::Custom { name, value, .. },
            )) = &out.content
            else {
                continue;
            };

            if name != "ai_chunking.snapshot" {
                continue;
            }

            if let Ok(snapshot) = serde_json::from_value::<AiChunkingSnapshot>(value.clone()) {
                chunk_count = snapshot.chunk_count;
                planning = Some(ChunkPlanningSummary {
                    input_items_total: snapshot.input_items_total,
                    planned_items_total: snapshot.planned_items_total,
                    excluded_items_total: snapshot.excluded_items_total,
                });
                break;
            }
        }

        if planning.is_none() {
            // Fall back to a best-effort parse from the first chunk envelope.
            for out in outputs {
                let ChainEventContent::Data { event_type, payload } = &out.content else {
                    continue;
                };
                if !Chunk::event_type_matches(event_type) {
                    continue;
                }

                #[derive(Deserialize)]
                struct PlanningOnly {
                    planning: ChunkPlanningSummary,
                }

                if let Ok(decoded) = serde_json::from_value::<PlanningOnly>(payload.clone()) {
                    planning = Some(decoded.planning);
                    break;
                }
            }
        }

        let planning = planning.unwrap_or(ChunkPlanningSummary {
            input_items_total: 0,
            planned_items_total: 0,
            excluded_items_total: 0,
        });

        let manifest = AiMapReducePlanningManifest {
            job_key: event.id,
            chunk_count,
            planning,
        };

        let Ok(payload) = serde_json::to_value(&manifest) else {
            return;
        };

        let manifest_event = ChainEventFactory::derived_data_event(
            event.writer_id,
            event,
            AiMapReducePlanningManifest::versioned_event_type(),
            payload,
        );

        ctx.write_control_event(manifest_event);
    }
}

#[derive(Debug, Clone)]
pub struct AiMapReduceChunkManifestFactory<Chunk> {
    _phantom: PhantomData<Chunk>,
}

impl<Chunk> AiMapReduceChunkManifestFactory<Chunk> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<Chunk> Default for AiMapReduceChunkManifestFactory<Chunk> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Chunk> MiddlewareFactory for AiMapReduceChunkManifestFactory<Chunk>
where
    Chunk: TypedPayload + Send + Sync + 'static,
{
    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<crate::middleware::control::ControlMiddlewareAggregator>,
    ) -> Box<dyn Middleware> {
        Box::new(AiMapReduceChunkManifestMiddleware::<Chunk>::new())
    }

    fn name(&self) -> &str {
        "ai_map_reduce.chunk_manifest"
    }
}

// ============================================================================
// Map stage: drop manifests, tag partials, emit chunk_failed
// ============================================================================

#[derive(Debug, Clone)]
pub struct AiMapReduceMapMiddleware<Chunk, Partial> {
    _phantom: PhantomData<(Chunk, Partial)>,
}

impl<Chunk, Partial> AiMapReduceMapMiddleware<Chunk, Partial> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<Chunk, Partial> Default for AiMapReduceMapMiddleware<Chunk, Partial> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Chunk, Partial> Middleware for AiMapReduceMapMiddleware<Chunk, Partial>
where
    Chunk: TypedPayload + Send + Sync,
    Partial: TypedPayload + Send + Sync,
{
    fn middleware_name(&self) -> &'static str {
        "ai_map_reduce.map_wrapper"
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        let ChainEventContent::Data { event_type, payload } = &event.content else {
            return MiddlewareAction::Skip(vec![]);
        };

        // Drop framework planning manifests before the user map handler.
        if AiMapReducePlanningManifest::event_type_matches(event_type) {
            return MiddlewareAction::Skip(vec![]);
        }

        // Only chunk envelopes are allowed to reach the user map handler.
        if !Chunk::event_type_matches(event_type) {
            return MiddlewareAction::Skip(vec![]);
        }

        let header: ChunkEnvelopeIndexCount = match serde_json::from_value(payload.clone()) {
            Ok(h) => h,
            Err(err) => {
                let reason = format!("ai_map_reduce: chunk header decode failed: {err}");
                let error_event = event
                    .clone()
                    .mark_as_error(reason, ErrorKind::Deserialization);
                return MiddlewareAction::Skip(vec![error_event]);
            }
        };

        let job_key = job_key_from_chunk_event(event);
        ctx.set_baggage(BAGGAGE_JOB_KEY, json!(job_key));
        ctx.set_baggage(BAGGAGE_CHUNK_INDEX, json!(header.chunk_index));
        ctx.set_baggage(BAGGAGE_CHUNK_COUNT, json!(header.chunk_count));

        let default_reason = "map produced no partial output";

        // Pre-allocate the failure marker so it survives middleware short-circuit paths
        // (e.g., circuit breaker OpenPolicy::Skip/EmitFallback without fallback).
        let failure = AiMapReduceChunkFailed {
            job_key,
            chunk_index: header.chunk_index,
            chunk_count: header.chunk_count,
            reason: default_reason.to_string(),
        };

        if let Ok(payload) = serde_json::to_value(&failure) {
            let event = ChainEventFactory::derived_data_event(
                event.writer_id,
                event,
                AiMapReduceChunkFailed::versioned_event_type(),
                payload,
            );
            ctx.write_control_event(event);
        }

        // Also pre-allocate a structured error event for the map stage error journal.
        // This is primarily for circuit-breaker rejection paths that otherwise emit
        // only observability events.
        let failed = AiMapReduceMapFailed {
            job_key,
            chunk_index: header.chunk_index,
            chunk_count: header.chunk_count,
            reason: default_reason.to_string(),
        };

        if let Ok(payload) = serde_json::to_value(&failed) {
            let event = ChainEventFactory::derived_data_event(
                event.writer_id,
                event,
                MAP_FAILURE_EVENT_TYPE,
                payload,
            )
            .mark_as_error(default_reason, ErrorKind::PermanentFailure);
            ctx.write_control_event(event);
        }

        MiddlewareAction::Continue
    }

    fn post_handle(&self, _event: &ChainEvent, outputs: &[ChainEvent], ctx: &mut MiddlewareContext) {
        // Integrated retry: do not surface chunk_failed markers until the terminal attempt.
        if should_retry(ctx) {
            remove_control_events_by_type(ctx, CHUNK_FAILED_EVENT_TYPE);
            remove_control_events_by_type(ctx, MAP_FAILURE_EVENT_TYPE);
            return;
        }

        if has_typed_output::<Partial>(outputs) {
            // We observed at least one partial output, so clear the pre-allocated
            // failure markers for this attempt.
            remove_control_events_by_type(ctx, CHUNK_FAILED_EVENT_TYPE);
            remove_control_events_by_type(ctx, MAP_FAILURE_EVENT_TYPE);
        }
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        update_chunk_failed_reason(event, ctx, "map produced no partial output");

        let ChainEventContent::Data { event_type, payload } = &mut event.content else {
            return;
        };

        if !Partial::event_type_matches(event_type) {
            return;
        }

        let Some(job_key) = ctx
            .get_baggage(BAGGAGE_JOB_KEY)
            .cloned()
            .and_then(|v| serde_json::from_value::<EventId>(v).ok())
        else {
            return;
        };
        let chunk_index = ctx
            .get_baggage(BAGGAGE_CHUNK_INDEX)
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        let chunk_count = ctx
            .get_baggage(BAGGAGE_CHUNK_COUNT)
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        let tagged = AiMapReduceTaggedPartial::<serde_json::Value> {
            job_key,
            chunk_index,
            chunk_count,
            partial: payload.clone(),
        };

        let Ok(next_payload) = serde_json::to_value(&tagged) else {
            return;
        };

        *event_type = TAGGED_PARTIAL_EVENT_TYPE.to_string();
        *payload = next_payload;
    }
}

#[derive(Debug, Clone)]
pub struct AiMapReduceMapFactory<Chunk, Partial> {
    _phantom: PhantomData<(Chunk, Partial)>,
}

impl<Chunk, Partial> AiMapReduceMapFactory<Chunk, Partial> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<Chunk, Partial> Default for AiMapReduceMapFactory<Chunk, Partial> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Chunk, Partial> MiddlewareFactory for AiMapReduceMapFactory<Chunk, Partial>
where
    Chunk: TypedPayload + Send + Sync + 'static,
    Partial: TypedPayload + Send + Sync + 'static,
{
    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<crate::middleware::control::ControlMiddlewareAggregator>,
    ) -> Box<dyn Middleware> {
        Box::new(AiMapReduceMapMiddleware::<Chunk, Partial>::new())
    }

    fn name(&self) -> &str {
        "ai_map_reduce.map_wrapper"
    }
}
