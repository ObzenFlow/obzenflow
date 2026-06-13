// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Framework-owned middleware used by the AI map-reduce composite lowering.
//!
//! These middleware adapters are intentionally narrow:
//! - `chunk` stage: emit a framework-internal planning manifest per input job.
//! - `map` stage: drop manifests, tag partial outputs with job/chunk metadata,
//!   and emit a terminal failure marker when a chunk produces no partial output.

use crate::middleware::{
    context_keys::{
        AiMapReduceChunkContext, AiMapReduceChunkContextKey, CircuitBreakerShouldRetry,
    },
    ControlMiddlewareRole, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
    MiddlewareOverrideKey, MiddlewarePlanContribution, SourceMiddlewarePhase,
    TopologyMiddlewareConfigSlot,
};
use obzenflow_core::ai::{
    AiMapReduceChunkFailed, AiMapReducePlanningManifest, AiMapReduceTaggedPartial,
    ChunkPlanningSummary,
};
use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::observability::AiChunkingSnapshot;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerRejectionReason, MetricsLifecycle, MiddlewareLifecycle,
    ObservabilityPayload,
};
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
    ctx.retain_control_events(|e| match &e.content {
        ChainEventContent::Data {
            event_type: actual, ..
        } => actual != event_type,
        _ => true,
    });
}

fn has_typed_output<T: TypedPayload>(outputs: &[ChainEvent]) -> bool {
    outputs.iter().any(|event| match &event.content {
        ChainEventContent::Data { event_type, .. } => T::event_type_matches(event_type),
        _ => false,
    })
}

fn should_retry(ctx: &MiddlewareContext) -> bool {
    ctx.get::<CircuitBreakerShouldRetry>()
        .copied()
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
        ChainEventContent::Data {
            event_type,
            payload,
        } => (event_type, payload),
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

    let rejection_reason =
        ctx.ephemeral_events()
            .iter()
            .rev()
            .find_map(|event| match &event.content {
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::Rejected {
                        reason,
                        ..
                    }),
                )) => Some(*reason),
                _ => None,
            });

    if let Some(reason) = rejection_reason {
        fn render(reason: CircuitBreakerRejectionReason) -> &'static str {
            match reason {
                CircuitBreakerRejectionReason::CircuitOpen => "circuit_open",
                CircuitBreakerRejectionReason::ProbeInProgress => "probe_in_progress",
                CircuitBreakerRejectionReason::Unknown => "unknown",
            }
        }

        decoded.reason = format!("circuit_breaker:{}", render(reason));
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
    fn label(&self) -> &'static str {
        "ai_map_reduce.chunk_manifest"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Structural
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
                let ChainEventContent::Data {
                    event_type,
                    payload,
                } = &out.content
                else {
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

        let (seed_payload, seed_event_type) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (payload.clone(), event_type.clone()),
            _ => (json!(null), event.event_type()),
        };

        let manifest = AiMapReducePlanningManifest {
            job_key: event.id,
            chunk_count,
            planning,
            seed_payload,
            seed_event_type,
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

pub struct AiMapReduceChunkManifestFamily;

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
    fn label(&self) -> &'static str {
        "ai_map_reduce.chunk_manifest"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<AiMapReduceChunkManifestFamily>("ai_map_reduce.chunk_manifest")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        // Structural-transitional (FLOWIP-120c H2): deleted when FLOWIP-120p
        // moves the AI legs onto effects.
        crate::middleware::MiddlewareKind::Structural
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<crate::middleware::control::ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(AiMapReduceChunkManifestMiddleware::<Chunk>::new()))
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
    fn label(&self) -> &'static str {
        "ai_map_reduce.map_wrapper"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Structural
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        let ChainEventContent::Data {
            event_type,
            payload,
        } = &event.content
        else {
            return MiddlewareAction::Skip {
                results: vec![],
                cause: None,
            };
        };

        // Drop framework planning manifests before the user map handler.
        if AiMapReducePlanningManifest::event_type_matches(event_type) {
            return MiddlewareAction::Skip {
                results: vec![],
                cause: None,
            };
        }

        // Only chunk envelopes are allowed to reach the user map handler.
        if !Chunk::event_type_matches(event_type) {
            return MiddlewareAction::Skip {
                results: vec![],
                cause: None,
            };
        }

        let header: ChunkEnvelopeIndexCount = match serde_json::from_value(payload.clone()) {
            Ok(h) => h,
            Err(err) => {
                let reason = format!("ai_map_reduce: chunk header decode failed: {err}");
                let error_event = event
                    .clone()
                    .mark_as_error(reason, ErrorKind::Deserialization);
                return MiddlewareAction::Skip {
                    results: vec![error_event],
                    cause: None,
                };
            }
        };

        let job_key = job_key_from_chunk_event(event);
        ctx.insert::<AiMapReduceChunkContextKey>(AiMapReduceChunkContext {
            job_key,
            chunk_index: header.chunk_index,
            chunk_count: header.chunk_count,
        });

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

    fn post_handle(
        &self,
        _event: &ChainEvent,
        outputs: &[ChainEvent],
        ctx: &mut MiddlewareContext,
    ) {
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

        let ChainEventContent::Data {
            event_type,
            payload,
        } = &mut event.content
        else {
            return;
        };

        if !Partial::event_type_matches(event_type) {
            return;
        }

        let Some(chunk_ctx) = ctx.get::<AiMapReduceChunkContextKey>().copied() else {
            return;
        };
        let job_key = chunk_ctx.job_key;
        let chunk_index = chunk_ctx.chunk_index;
        let chunk_count = chunk_ctx.chunk_count;

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

pub struct AiMapReduceMapFamily;

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
    fn label(&self) -> &'static str {
        "ai_map_reduce.map_wrapper"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<AiMapReduceMapFamily>("ai_map_reduce.map_wrapper")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        // Structural-transitional (FLOWIP-120c H2): deleted when FLOWIP-120p
        // moves the AI legs onto effects.
        crate::middleware::MiddlewareKind::Structural
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<crate::middleware::control::ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(AiMapReduceMapMiddleware::<Chunk, Partial>::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{MiddlewareAction, MiddlewareContext};
    use obzenflow_core::ai::ChunkPlanningSummary;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::observability::AiChunkingSnapshot;
    use obzenflow_core::event::payloads::observability_payload::{
        MetricsLifecycle, ObservabilityPayload,
    };
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::{EventId, StageId, TypedPayload, WriterId};
    use obzenflow_runtime::pipeline::config::StageConfig;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestChunkEnvelope {
        chunk_index: usize,
        chunk_count: usize,
        planning: ChunkPlanningSummary,
    }

    impl TypedPayload for TestChunkEnvelope {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.chunk";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPartial {
        value: u32,
    }

    impl TypedPayload for TestPartial {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.partial";
    }

    fn stage_config() -> StageConfig {
        StageConfig {
            stage_id: StageId::new(),
            name: "stage".to_string(),
            flow_name: "flow".to_string(),
            cycle_guard: None,
        }
    }

    fn control_aggregator() -> Arc<ControlMiddlewareAggregator> {
        Arc::new(ControlMiddlewareAggregator::new())
    }

    fn writer_id() -> WriterId {
        WriterId::from(StageId::new())
    }

    fn mk_chunk_manifest_middleware() -> Box<dyn Middleware> {
        AiMapReduceChunkManifestFactory::<TestChunkEnvelope>::new()
            .create(&stage_config(), control_aggregator())
            .expect("chunk manifest middleware should materialize in tests")
    }

    fn mk_map_middleware() -> Box<dyn Middleware> {
        AiMapReduceMapFactory::<TestChunkEnvelope, TestPartial>::new()
            .create(&stage_config(), control_aggregator())
            .expect("map wrapper middleware should materialize in tests")
    }

    #[test]
    fn chunk_manifest_factory_creates_middleware() {
        let factory = AiMapReduceChunkManifestFactory::<TestChunkEnvelope>::new();
        assert_eq!(factory.label(), "ai_map_reduce.chunk_manifest");

        let middleware = factory
            .create(&stage_config(), control_aggregator())
            .expect("chunk manifest factory should materialize middleware");
        assert_eq!(middleware.label(), "ai_map_reduce.chunk_manifest");
    }

    #[test]
    fn chunk_manifest_emits_planning_manifest_from_snapshot() {
        let middleware = mk_chunk_manifest_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let input = ChainEventFactory::data_event(writer_id(), "job", json!({}));

        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 2,
            planning: ChunkPlanningSummary {
                input_items_total: 100,
                planned_items_total: 80,
                excluded_items_total: 20,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &input,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        let snapshot = AiChunkingSnapshot {
            input_items_total: 10,
            planned_items_total: 7,
            excluded_items_total: 3,
            chunk_count: 5,
            rerender_attempts_total: 0,
            max_decomposition_depth_reached: 0,
            budget_overhead_tokens: 0,
            oversize_policy: "exclude".to_string(),
            exclusions_by_reason: HashMap::new(),
            excluded_items: None,
        };

        let snapshot_event = ChainEventFactory::observability_event(
            writer_id(),
            ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
                name: "ai_chunking.snapshot".to_string(),
                value: serde_json::to_value(snapshot).expect("snapshot should serialize"),
                tags: None,
            }),
        );

        middleware.post_handle(&input, &[chunk_event, snapshot_event], &mut ctx);

        assert_eq!(ctx.control_events().len(), 1);
        let manifest_event = &ctx.control_events()[0];
        assert_eq!(
            manifest_event.event_type(),
            AiMapReducePlanningManifest::versioned_event_type()
        );
        assert_eq!(manifest_event.causality.parent_ids.first(), Some(&input.id));

        let manifest = AiMapReducePlanningManifest::try_from_event(manifest_event)
            .expect("manifest should decode");
        assert_eq!(manifest.job_key, input.id);
        assert_eq!(manifest.chunk_count, 5);
        assert_eq!(manifest.planning.input_items_total, 10);
        assert_eq!(manifest.planning.planned_items_total, 7);
        assert_eq!(manifest.planning.excluded_items_total, 3);
    }

    #[test]
    fn chunk_manifest_falls_back_to_count_and_planning_from_first_chunk() {
        let middleware = mk_chunk_manifest_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let input = ChainEventFactory::data_event(writer_id(), "job", json!({}));

        let planning = ChunkPlanningSummary {
            input_items_total: 3,
            planned_items_total: 2,
            excluded_items_total: 1,
        };

        let outputs: Vec<_> = (0..3)
            .map(|i| {
                let chunk = TestChunkEnvelope {
                    chunk_index: i,
                    chunk_count: 3,
                    planning: planning.clone(),
                };
                ChainEventFactory::derived_data_event(
                    writer_id(),
                    &input,
                    TestChunkEnvelope::versioned_event_type(),
                    serde_json::to_value(chunk).expect("chunk should serialize"),
                )
            })
            .collect();

        middleware.post_handle(&input, &outputs, &mut ctx);

        assert_eq!(ctx.control_events().len(), 1);
        let manifest = AiMapReducePlanningManifest::try_from_event(&ctx.control_events()[0])
            .expect("manifest should decode");
        assert_eq!(manifest.job_key, input.id);
        assert_eq!(manifest.chunk_count, 3);
        assert_eq!(manifest.planning, planning);
    }

    #[test]
    fn chunk_manifest_defaults_planning_to_zero_when_missing() {
        let middleware = mk_chunk_manifest_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let input = ChainEventFactory::data_event(writer_id(), "job", json!({}));

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &input,
            TestChunkEnvelope::versioned_event_type(),
            json!({
                "chunk_index": 0,
                "chunk_count": 1,
            }),
        );

        middleware.post_handle(&input, &[chunk_event], &mut ctx);

        assert_eq!(ctx.control_events().len(), 1);
        let manifest = AiMapReducePlanningManifest::try_from_event(&ctx.control_events()[0])
            .expect("manifest should decode");
        assert_eq!(manifest.chunk_count, 1);
        assert_eq!(manifest.seed_event_type, "job");
        assert_eq!(manifest.seed_payload, json!({}));
        assert_eq!(
            manifest.planning,
            ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0
            }
        );
    }

    #[test]
    fn map_factory_creates_middleware() {
        let factory = AiMapReduceMapFactory::<TestChunkEnvelope, TestPartial>::new();
        assert_eq!(factory.label(), "ai_map_reduce.map_wrapper");

        let middleware = factory
            .create(&stage_config(), control_aggregator())
            .expect("map wrapper factory should materialize middleware");
        assert_eq!(middleware.label(), "ai_map_reduce.map_wrapper");
    }

    #[test]
    fn map_wrapper_drops_planning_manifests_before_user_handler() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let manifest = AiMapReducePlanningManifest {
            job_key: EventId::new(),
            chunk_count: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 1,
                planned_items_total: 1,
                excluded_items_total: 0,
            },
            seed_payload: json!({ "seed": true }),
            seed_event_type: "seed.event".to_string(),
        }
        .to_event(writer_id());

        let action = middleware.pre_handle(&manifest, &mut ctx);
        assert!(matches!(action, MiddlewareAction::Skip { results: v, .. } if v.is_empty()));
        assert!(ctx.control_events().is_empty());
    }

    #[test]
    fn map_wrapper_skips_non_chunk_data_events() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let other = ChainEventFactory::data_event(writer_id(), "other", json!({}));
        let action = middleware.pre_handle(&other, &mut ctx);
        assert!(matches!(action, MiddlewareAction::Skip { results: v, .. } if v.is_empty()));
        assert!(ctx.control_events().is_empty());
    }

    #[test]
    fn map_wrapper_reports_deserialization_error_when_chunk_header_decode_fails() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let chunk = ChainEventFactory::data_event(
            writer_id(),
            TestChunkEnvelope::versioned_event_type(),
            json!({ "oops": true }),
        );

        let action = middleware.pre_handle(&chunk, &mut ctx);
        let MiddlewareAction::Skip {
            results: events, ..
        } = action
        else {
            panic!("expected Skip {{ .. }}");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].processing_info.status.kind(),
            Some(&ErrorKind::Deserialization)
        );
        assert!(ctx.control_events().is_empty());
    }

    #[test]
    fn map_wrapper_sets_chunk_context_and_preallocates_failure_markers() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let job_key = parent.id;

        let chunk_payload = TestChunkEnvelope {
            chunk_index: 1,
            chunk_count: 3,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        let action = middleware.pre_handle(&chunk_event, &mut ctx);
        assert!(matches!(action, MiddlewareAction::Continue));

        let stored = ctx
            .get::<AiMapReduceChunkContextKey>()
            .copied()
            .expect("chunk context slot");
        assert_eq!(stored.job_key, job_key);
        assert_eq!(stored.chunk_index, 1);
        assert_eq!(stored.chunk_count, 3);

        assert_eq!(ctx.control_events().len(), 2);

        let chunk_failed = ctx
            .control_events()
            .iter()
            .find(|e| AiMapReduceChunkFailed::event_type_matches(&e.event_type()))
            .expect("chunk_failed marker");
        let failed = AiMapReduceChunkFailed::try_from_event(chunk_failed)
            .expect("chunk_failed should decode");
        assert_eq!(failed.job_key, job_key);
        assert_eq!(failed.chunk_index, 1);
        assert_eq!(failed.chunk_count, 3);
        assert_eq!(failed.reason, "map produced no partial output");

        let map_failed = ctx
            .control_events()
            .iter()
            .find(|e| e.event_type() == MAP_FAILURE_EVENT_TYPE)
            .expect("map_failed marker");
        assert_eq!(
            map_failed.processing_info.status.kind(),
            Some(&ErrorKind::PermanentFailure)
        );

        let decoded: AiMapReduceMapFailed =
            serde_json::from_value(map_failed.payload()).expect("map_failed payload should decode");
        assert_eq!(decoded.job_key, job_key);
        assert_eq!(decoded.chunk_index, 1);
        assert_eq!(decoded.chunk_count, 3);
    }

    #[test]
    fn map_wrapper_clears_failure_markers_when_retry_will_happen() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        assert!(matches!(
            middleware.pre_handle(&chunk_event, &mut ctx),
            MiddlewareAction::Continue
        ));
        assert_eq!(ctx.control_events().len(), 2);

        ctx.insert::<CircuitBreakerShouldRetry>(true);
        middleware.post_handle(&chunk_event, &[], &mut ctx);
        assert!(ctx.control_events().is_empty());
    }

    #[test]
    fn map_wrapper_clears_failure_markers_when_partial_output_is_observed() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        assert!(matches!(
            middleware.pre_handle(&chunk_event, &mut ctx),
            MiddlewareAction::Continue
        ));
        assert_eq!(ctx.control_events().len(), 2);

        let partial_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &chunk_event,
            TestPartial::versioned_event_type(),
            json!({ "value": 1 }),
        );
        middleware.post_handle(&chunk_event, &[partial_event], &mut ctx);
        assert!(ctx.control_events().is_empty());
    }

    #[test]
    fn map_wrapper_retains_failure_markers_when_no_partial_outputs_are_emitted() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        assert!(matches!(
            middleware.pre_handle(&chunk_event, &mut ctx),
            MiddlewareAction::Continue
        ));

        let other_output =
            ChainEventFactory::derived_data_event(writer_id(), &chunk_event, "other", json!({}));

        middleware.post_handle(&chunk_event, &[other_output], &mut ctx);
        assert_eq!(ctx.control_events().len(), 2);
        assert!(ctx
            .control_events()
            .iter()
            .any(|e| e.event_type() == CHUNK_FAILED_EVENT_TYPE));
    }

    #[test]
    fn map_wrapper_tags_partial_outputs_in_pre_write() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let job_key = parent.id;

        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 2,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        assert!(matches!(
            middleware.pre_handle(&chunk_event, &mut ctx),
            MiddlewareAction::Continue
        ));

        let mut partial_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &chunk_event,
            TestPartial::versioned_event_type(),
            json!({ "value": 7 }),
        );

        middleware.pre_write(&mut partial_event, &ctx);
        assert_eq!(partial_event.event_type(), TAGGED_PARTIAL_EVENT_TYPE);

        let tagged = AiMapReduceTaggedPartial::<serde_json::Value>::try_from_event(&partial_event)
            .expect("tagged partial should decode");
        assert_eq!(tagged.job_key, job_key);
        assert_eq!(tagged.chunk_index, 0);
        assert_eq!(tagged.chunk_count, 2);
        assert_eq!(tagged.partial, json!({ "value": 7 }));
    }

    #[test]
    fn map_wrapper_updates_chunk_failed_reason_from_cb_rejection() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        assert!(matches!(
            middleware.pre_handle(&chunk_event, &mut ctx),
            MiddlewareAction::Continue
        ));

        ctx.emit_ephemeral_event(ChainEventFactory::observability_event(
            writer_id(),
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Rejected {
                    reason: CircuitBreakerRejectionReason::CircuitOpen,
                    cooldown_remaining_ms: None,
                    circuit_open_duration_ms: None,
                },
            )),
        ));

        let marker = ctx
            .control_events()
            .iter()
            .find(|e| AiMapReduceChunkFailed::event_type_matches(&e.event_type()))
            .cloned()
            .expect("chunk_failed marker");
        let mut marker = marker;

        middleware.pre_write(&mut marker, &ctx);

        let updated =
            AiMapReduceChunkFailed::try_from_event(&marker).expect("chunk_failed should decode");
        assert_eq!(updated.reason, "circuit_breaker:circuit_open");
    }

    #[test]
    fn map_wrapper_does_not_tag_non_partial_events() {
        let middleware = mk_map_middleware();
        let mut ctx = MiddlewareContext::live_handler();

        let parent = ChainEventFactory::data_event(writer_id(), "job", json!({}));
        let chunk_payload = TestChunkEnvelope {
            chunk_index: 0,
            chunk_count: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 0,
                planned_items_total: 0,
                excluded_items_total: 0,
            },
        };

        let chunk_event = ChainEventFactory::derived_data_event(
            writer_id(),
            &parent,
            TestChunkEnvelope::versioned_event_type(),
            serde_json::to_value(chunk_payload).expect("chunk should serialize"),
        );

        assert!(matches!(
            middleware.pre_handle(&chunk_event, &mut ctx),
            MiddlewareAction::Continue
        ));

        let mut other =
            ChainEventFactory::derived_data_event(writer_id(), &chunk_event, "other", json!({}));
        middleware.pre_write(&mut other, &ctx);
        assert_eq!(other.event_type(), "other");
    }
}
