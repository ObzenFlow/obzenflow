// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ai::{
    AiMapReducePlanningFailed, AiMapReducePlanningManifest, ChunkPlanningSummary,
};
use obzenflow_core::event::observability::AiChunkingSnapshot;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{
    ChainEventContent, ChainEventFactory, StageFatalCode, StageFatalReason,
};
use obzenflow_core::id::CompositeId;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::common::handlers::TransformHandler;
use serde::Deserialize;
use std::fmt;

#[derive(Clone)]
pub struct GeneratedAiChunkHandler<Chunk, Inner> {
    inner: Inner,
    composite_id: CompositeId,
    lineage: obzenflow_core::config::LineagePolicy,
    _chunk: std::marker::PhantomData<fn() -> Chunk>,
}

impl<Chunk, Inner> GeneratedAiChunkHandler<Chunk, Inner> {
    pub fn new(inner: Inner, composite_id: CompositeId) -> Self {
        Self {
            inner,
            composite_id,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            _chunk: std::marker::PhantomData,
        }
    }
}

impl<Chunk, Inner: fmt::Debug> fmt::Debug for GeneratedAiChunkHandler<Chunk, Inner> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedAiChunkHandler")
            .field("inner", &self.inner)
            .field("composite_id", &self.composite_id)
            .finish()
    }
}

fn fatal(reason: StageFatalReason, detail: impl Into<String>) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(StageFatalCode::Protocol, reason, detail))
}

#[async_trait]
impl<Chunk, Inner> TransformHandler for GeneratedAiChunkHandler<Chunk, Inner>
where
    Chunk: TypedPayload + Send + Sync + 'static,
    Inner: TransformHandler + Send + Sync,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let matching = event
            .composite_activations()
            .iter()
            .filter(|activation| activation.composite_id == self.composite_id)
            .collect::<Vec<_>>();
        let [activation] = matching.as_slice() else {
            return Err(fatal(
                StageFatalReason::ProtocolInputIntegrity,
                format!(
                    "AI map-reduce seed requires exactly one activation for composite '{}', found {}",
                    self.composite_id,
                    matching.len()
                ),
            ));
        };
        let job_key = activation.activation;

        let outputs = match self.inner.process(event.clone()) {
            Ok(outputs) => outputs,
            Err(HandlerError::AiMapReducePlanning(cause)) => {
                let failed = AiMapReducePlanningFailed { job_key, cause };
                let payload = serde_json::to_value(failed).map_err(|error| {
                    fatal(
                        StageFatalReason::ProtocolInputIntegrity,
                        format!("planning failure serialization failed: {error}"),
                    )
                })?;
                return Ok(vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    AiMapReducePlanningFailed::versioned_event_type(),
                    payload,
                    self.lineage,
                )]);
            }
            Err(error) => return Err(error),
        };

        let mut chunk_count = outputs
            .iter()
            .filter(|output| Chunk::event_type_matches(&output.event_type()))
            .count();
        let mut planning = None;
        for output in &outputs {
            let ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::Custom { name, value, .. },
            )) = &output.content
            else {
                continue;
            };
            if name == "ai_chunking.snapshot" {
                let snapshot: AiChunkingSnapshot =
                    serde_json::from_value(value.clone()).map_err(|error| {
                        fatal(
                            StageFatalReason::ProtocolInputIntegrity,
                            format!("chunking snapshot decode failed: {error}"),
                        )
                    })?;
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
            #[derive(Deserialize)]
            struct PlanningOnly {
                planning: ChunkPlanningSummary,
            }
            planning = outputs.iter().find_map(|output| {
                let ChainEventContent::Data {
                    event_type,
                    payload,
                } = &output.content
                else {
                    return None;
                };
                Chunk::event_type_matches(event_type)
                    .then(|| serde_json::from_value::<PlanningOnly>(payload.clone()).ok())
                    .flatten()
                    .map(|decoded| decoded.planning)
            });
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
            _ => {
                return Err(fatal(
                    StageFatalReason::ProtocolInputIntegrity,
                    "AI map-reduce seed is not Data",
                ))
            }
        };
        let manifest = AiMapReducePlanningManifest {
            job_key,
            chunk_count,
            planning,
            seed_payload,
            seed_event_type,
        };
        let payload = serde_json::to_value(manifest).map_err(|error| {
            fatal(
                StageFatalReason::ProtocolInputIntegrity,
                format!("planning manifest serialization failed: {error}"),
            )
        })?;
        let mut outputs = outputs;
        outputs.push(ChainEventFactory::derived_data_event(
            event.writer_id,
            &event,
            AiMapReducePlanningManifest::versioned_event_type(),
            payload,
            self.lineage,
        ));
        Ok(outputs)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        self.inner.drain().await
    }

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
        self.inner.install_lineage_policy(policy);
    }
}
