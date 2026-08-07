// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed chunk-planning transform helpers (FLOWIP-086z, FLOWIP-134b).

use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::{
    TransformHandler, TypedTransformHandler, TypedTransformInvocation,
};
use async_trait::async_trait;
use obzenflow_core::ai::{
    plan_chunks_by_budget, AiMapReduceMapInput, AiMapReducePlanningFailed,
    AiMapReducePlanningManifest, ChunkEnvelope, ChunkPlanningConfig, ChunkPlanningError,
    ChunkPlanningSummary, ChunkRenderContext, OversizePolicy, TokenCount, TokenEstimator,
};
use obzenflow_core::event::observability::AiChunkingSnapshot;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{
    ChainEventContent, ChainEventFactory, StageFatalCode, StageFatalReason,
};
use obzenflow_core::id::CompositeId;
use obzenflow_core::{ChainEvent, StageOutputs, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;
use std::sync::Arc;

type ItemExtractor<In, Item> = Arc<dyn Fn(&In) -> Vec<Item> + Send + Sync + 'static>;
type ItemRenderer<Item> = Arc<dyn Fn(&Item, ChunkRenderContext) -> String + Send + Sync + 'static>;

#[derive(Clone)]
pub struct ChunkByBudgetBuilder<In, Item> {
    estimator: Arc<dyn TokenEstimator>,
    items: Option<ItemExtractor<In, Item>>,
    render: Option<ItemRenderer<Item>>,
    budget: Option<TokenCount>,
    max_items_per_chunk: Option<usize>,
    oversize_policy: OversizePolicy,
    budget_overhead_tokens: TokenCount,
    snapshot_excluded_items_limit: usize,
    _types: std::marker::PhantomData<fn(In) -> Item>,
}

impl<In, Item> fmt::Debug for ChunkByBudgetBuilder<In, Item> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkByBudgetBuilder")
            .field("budget", &self.budget)
            .field("max_items_per_chunk", &self.max_items_per_chunk)
            .field("oversize_policy", &self.oversize_policy)
            .field("budget_overhead_tokens", &self.budget_overhead_tokens)
            .field(
                "snapshot_excluded_items_limit",
                &self.snapshot_excluded_items_limit,
            )
            .finish()
    }
}

impl<In, Item> ChunkByBudgetBuilder<In, Item> {
    pub fn new() -> Self {
        Self {
            estimator: Arc::new(obzenflow_core::ai::HeuristicTokenEstimator::default()),
            items: None,
            render: None,
            budget: None,
            max_items_per_chunk: None,
            oversize_policy: OversizePolicy::default(),
            budget_overhead_tokens: TokenCount::ZERO,
            snapshot_excluded_items_limit: 0,
            _types: std::marker::PhantomData,
        }
    }

    pub fn estimator(mut self, estimator: Arc<dyn TokenEstimator>) -> Self {
        self.estimator = estimator;
        self
    }

    pub fn items<F>(mut self, extractor: F) -> Self
    where
        F: Fn(&In) -> Vec<Item> + Send + Sync + 'static,
    {
        self.items = Some(Arc::new(extractor));
        self
    }

    pub fn render<F>(mut self, render: F) -> Self
    where
        F: Fn(&Item, ChunkRenderContext) -> String + Send + Sync + 'static,
    {
        self.render = Some(Arc::new(render));
        self
    }

    pub fn budget(mut self, budget: TokenCount) -> Self {
        self.budget = Some(budget);
        self
    }

    pub fn max_items_per_chunk(mut self, max: Option<usize>) -> Self {
        self.max_items_per_chunk = max;
        self
    }

    pub fn oversize(mut self, policy: OversizePolicy) -> Self {
        self.oversize_policy = policy;
        self
    }

    pub fn budget_overhead_tokens(mut self, tokens: TokenCount) -> Self {
        self.budget_overhead_tokens = tokens;
        self
    }

    pub fn snapshot_excluded_items_limit(mut self, limit: usize) -> Self {
        self.snapshot_excluded_items_limit = limit;
        self
    }

    pub fn build(self) -> ChunkByBudgetTyped<In, Item> {
        ChunkByBudgetTyped {
            estimator: self.estimator,
            items: self.items.expect("chunk_by_budget: missing items(...)"),
            render: self.render.expect("chunk_by_budget: missing render(...)"),
            budget: self.budget.expect("chunk_by_budget: missing budget(...)"),
            max_items_per_chunk: self.max_items_per_chunk,
            oversize_policy: self.oversize_policy,
            budget_overhead_tokens: self.budget_overhead_tokens,
            snapshot_excluded_items_limit: self.snapshot_excluded_items_limit,
        }
    }
}

impl<In, Item> Default for ChunkByBudgetBuilder<In, Item> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct ChunkByBudgetTyped<In, Item> {
    estimator: Arc<dyn TokenEstimator>,
    items: ItemExtractor<In, Item>,
    render: ItemRenderer<Item>,
    budget: TokenCount,
    max_items_per_chunk: Option<usize>,
    oversize_policy: OversizePolicy,
    budget_overhead_tokens: TokenCount,
    snapshot_excluded_items_limit: usize,
}

impl<In, Item> fmt::Debug for ChunkByBudgetTyped<In, Item> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChunkByBudgetTyped")
            .field("estimator_source", &self.estimator.source())
            .field("budget", &self.budget)
            .field("max_items_per_chunk", &self.max_items_per_chunk)
            .field("oversize_policy", &self.oversize_policy)
            .field("budget_overhead_tokens", &self.budget_overhead_tokens)
            .finish()
    }
}

struct PlannedChunks<Item> {
    chunks: Vec<ChunkEnvelope<Item>>,
    summary: ChunkPlanningSummary,
    snapshot: AiChunkingSnapshot,
}

impl<In, Item> ChunkByBudgetTyped<In, Item> {
    fn plan_once(&self, input: In) -> Result<PlannedChunks<Item>, HandlerError> {
        let plan = plan_chunks_by_budget(
            self.estimator.as_ref(),
            (self.items)(&input),
            |item, context| (self.render)(item, context),
            ChunkPlanningConfig {
                budget: self.budget,
                max_items_per_chunk: self.max_items_per_chunk,
                oversize_policy: self.oversize_policy,
            },
        )
        .map_err(map_planning_error)?;

        let excluded_items = (self.snapshot_excluded_items_limit != 0).then(|| {
            plan.stats
                .excluded_item_ordinals
                .iter()
                .take(self.snapshot_excluded_items_limit)
                .copied()
                .collect::<Vec<_>>()
        });
        let exclusions_by_reason = plan
            .stats
            .exclusions_by_reason
            .iter()
            .map(|(reason, count)| {
                let name = match reason {
                    obzenflow_core::ai::ChunkExclusionReason::MaxDepthExceeded => {
                        "max_depth_exceeded"
                    }
                    obzenflow_core::ai::ChunkExclusionReason::NoProgress => "no_progress",
                };
                (name.to_string(), *count)
            })
            .collect();
        let snapshot = AiChunkingSnapshot {
            input_items_total: plan.summary.input_items_total,
            planned_items_total: plan.summary.planned_items_total,
            excluded_items_total: plan.summary.excluded_items_total,
            chunk_count: plan.stats.chunk_count,
            rerender_attempts_total: plan.stats.rerender_attempts_total,
            max_decomposition_depth_reached: plan.stats.max_decomposition_depth_reached,
            budget_overhead_tokens: self.budget_overhead_tokens.get(),
            oversize_policy: format!("{:?}", self.oversize_policy),
            exclusions_by_reason,
            excluded_items,
        };

        Ok(PlannedChunks {
            chunks: plan.chunks,
            summary: plan.summary,
            snapshot,
        })
    }
}

fn map_planning_error(error: ChunkPlanningError) -> HandlerError {
    match error {
        ChunkPlanningError::ZeroBudget => HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Configuration,
            StageFatalReason::ConfigurationInvariant,
            error.to_string(),
        )),
        ChunkPlanningError::OversizeItem {
            item_ordinal,
            estimated_tokens,
            budget,
        } => HandlerError::AiMapReducePlanning(
            obzenflow_core::ai::AiMapReducePlanningFailure::OversizeItem {
                item_ordinal,
                estimated_tokens,
                budget,
            },
        ),
        ChunkPlanningError::OversizeExhausted {
            item_ordinal,
            reason,
            last_estimated_tokens,
            budget,
        } => HandlerError::AiMapReducePlanning(
            obzenflow_core::ai::AiMapReducePlanningFailure::OversizeExhausted {
                item_ordinal,
                reason,
                last_estimated_tokens,
                budget,
            },
        ),
    }
}

fn snapshot_observability(
    snapshot: &AiChunkingSnapshot,
) -> Result<ObservabilityPayload, HandlerError> {
    let value = serde_json::to_value(snapshot).map_err(|error| {
        HandlerError::Validation(format!("ai_chunking.snapshot encode failed: {error}"))
    })?;
    Ok(ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
        name: "ai_chunking.snapshot".to_string(),
        value,
        tags: None,
    }))
}

impl<In, Item> TypedTransformHandler for ChunkByBudgetTyped<In, Item>
where
    In: TypedPayload + Send + Sync + 'static,
    Item: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Input = In;
    type Output = StageOutputs<ChunkEnvelope<Item>>;

    fn process(&self, input: In) -> Result<Self::Output, HandlerError> {
        self.plan_once(input)
            .map(|planned| StageOutputs::many(planned.chunks))
    }

    fn process_invocation(
        &self,
        input: In,
    ) -> Result<TypedTransformInvocation<Self::Output>, HandlerError> {
        let planned = self.plan_once(input)?;
        let observability = snapshot_observability(&planned.snapshot)?;
        Ok(TypedTransformInvocation::with_framework_observability(
            StageOutputs::many(planned.chunks),
            observability,
        ))
    }
}

/// Runtime-owned structural adapter for generated AI map-reduce chunking.
///
/// It accepts only the concrete typed planner, preserves the seed envelope's
/// composite activation, and authors the fixed snapshot/map-input/manifest
/// protocol from one invocation-local plan.
#[doc(hidden)]
#[derive(Clone)]
pub struct GeneratedAiChunkHandler<In, Item> {
    inner: ChunkByBudgetTyped<In, Item>,
    composite_id: CompositeId,
    lineage: obzenflow_core::config::LineagePolicy,
}

impl<In, Item> GeneratedAiChunkHandler<In, Item> {
    #[doc(hidden)]
    pub fn new(inner: ChunkByBudgetTyped<In, Item>, composite_id: CompositeId) -> Self {
        Self {
            inner,
            composite_id,
            lineage: obzenflow_core::config::LineagePolicy::default(),
        }
    }
}

impl<In, Item> fmt::Debug for GeneratedAiChunkHandler<In, Item> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedAiChunkHandler")
            .field("inner", &self.inner)
            .field("composite_id", &self.composite_id)
            .finish()
    }
}

fn protocol_fatal(detail: impl Into<String>) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    ))
}

#[async_trait]
impl<In, Item> TransformHandler for GeneratedAiChunkHandler<In, Item>
where
    In: TypedPayload + Send + Sync + 'static,
    Item: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let matching = event
            .composite_activations()
            .iter()
            .filter(|activation| activation.composite_id == self.composite_id)
            .collect::<Vec<_>>();
        let [activation] = matching.as_slice() else {
            return Err(protocol_fatal(format!(
                "AI map-reduce seed requires exactly one activation for composite '{}', found {}",
                self.composite_id,
                matching.len()
            )));
        };
        let job_key = activation.activation;

        let input = In::try_from_event(&event).map_err(|error| {
            protocol_fatal(format!("AI map-reduce seed decode failed: {error}"))
        })?;
        let planned = match self.inner.plan_once(input) {
            Ok(planned) => planned,
            Err(HandlerError::AiMapReducePlanning(cause)) => {
                let payload = serde_json::to_value(AiMapReducePlanningFailed { job_key, cause })
                    .map_err(|error| {
                        protocol_fatal(format!("planning failure serialization failed: {error}"))
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

        let observability = snapshot_observability(&planned.snapshot)?;
        let (seed_payload, seed_event_type) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (payload.clone(), event_type.clone()),
            _ => return Err(protocol_fatal("AI map-reduce seed is not Data")),
        };
        let chunk_count = planned.chunks.len();
        let mut outputs = Vec::with_capacity(chunk_count + 2);
        outputs.push(ChainEventFactory::derived_event(
            event.writer_id,
            &event,
            ChainEventContent::Observability(observability),
            self.lineage,
        ));

        for chunk in planned.chunks {
            let payload =
                serde_json::to_value(AiMapReduceMapInput { job_key, chunk }).map_err(|error| {
                    protocol_fatal(format!("generated map input serialization failed: {error}"))
                })?;
            outputs.push(ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                AiMapReduceMapInput::<ChunkEnvelope<Item>>::versioned_event_type(),
                payload,
                self.lineage,
            ));
        }

        let manifest = AiMapReducePlanningManifest {
            job_key,
            chunk_count,
            planning: planned.summary,
            seed_payload,
            seed_event_type,
        };
        let payload = serde_json::to_value(manifest).map_err(|error| {
            protocol_fatal(format!("planning manifest serialization failed: {error}"))
        })?;
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
        Ok(())
    }

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handlers::TypedTransformHandlerAdapter;
    use obzenflow_core::ai::{ChatRequest, EstimateSource, OversizeExhaustion, TokenEstimate};
    use obzenflow_core::event::context::CompositeActivationContext;
    use obzenflow_core::{EventId, StageId, WriterId};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Seed {
        items: Vec<String>,
    }

    impl TypedPayload for Seed {
        const EVENT_TYPE: &'static str = "ai.chunking.test.seed";
    }

    #[derive(Debug, Default)]
    struct CountingEstimator {
        text_calls: AtomicUsize,
    }

    impl CountingEstimator {
        fn calls(&self) -> usize {
            self.text_calls.load(Ordering::SeqCst)
        }
    }

    impl TokenEstimator for CountingEstimator {
        fn estimate_text(&self, text: &str) -> TokenEstimate {
            self.text_calls.fetch_add(1, Ordering::SeqCst);
            TokenEstimate {
                tokens: TokenCount::new(text.len() as u64),
                source: EstimateSource::Heuristic,
            }
        }

        fn estimate_chat_request(&self, _request: &ChatRequest) -> TokenEstimate {
            panic!("chunk planning must not estimate chat requests")
        }

        fn source(&self) -> EstimateSource {
            EstimateSource::Heuristic
        }
    }

    fn planner(estimator: Arc<CountingEstimator>) -> ChunkByBudgetTyped<Seed, String> {
        ChunkByBudgetBuilder::new()
            .estimator(estimator)
            .items(|seed: &Seed| seed.items.clone())
            .render(|item: &String, _context| item.clone())
            .budget(TokenCount::new(64))
            .max_items_per_chunk(Some(1))
            .build()
    }

    fn seed_event(items: Vec<String>) -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Seed::versioned_event_type(),
            serde_json::json!(Seed { items }),
        )
    }

    fn snapshot(event: &ChainEvent) -> AiChunkingSnapshot {
        let ChainEventContent::Observability(ObservabilityPayload::Metrics(
            MetricsLifecycle::Custom { name, value, .. },
        )) = &event.content
        else {
            panic!("expected ai_chunking.snapshot observability event")
        };
        assert_eq!(name, "ai_chunking.snapshot");
        serde_json::from_value(value.clone()).expect("snapshot decodes")
    }

    fn activated_seed_event(composite_id: &CompositeId, items: Vec<String>) -> ChainEvent {
        let mut event = seed_event(items);
        let activation = CompositeActivationContext::new(
            composite_id.clone(),
            event.id,
            "in",
            event.processing_info.event_time,
        );
        event
            .try_insert_composite_activation(activation)
            .expect("activation inserts");
        event
    }

    #[test]
    fn direct_chunking_emits_one_snapshot_then_zero_one_or_many_flat_chunks() {
        for items in [
            Vec::<String>::new(),
            vec!["one".to_string()],
            vec!["one".to_string(), "two".to_string(), "three".to_string()],
        ] {
            let expected_chunks = items.len();
            let estimator = Arc::new(CountingEstimator::default());
            let mut adapter = TypedTransformHandlerAdapter::new(planner(estimator.clone()));
            TransformHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
            let outputs = TransformHandler::process(&adapter, seed_event(items))
                .expect("direct planning succeeds");

            assert_eq!(outputs.len(), expected_chunks + 1);
            let observed = snapshot(&outputs[0]);
            assert_eq!(observed.chunk_count, expected_chunks);
            assert_eq!(observed.input_items_total, expected_chunks);
            assert_eq!(observed.planned_items_total, expected_chunks);
            assert_eq!(observed.excluded_items_total, 0);

            let chunks = outputs[1..]
                .iter()
                .map(|event| {
                    assert_eq!(
                        event.event_type(),
                        ChunkEnvelope::<String>::versioned_event_type()
                    );
                    ChunkEnvelope::<String>::try_from_event(event).expect("chunk decodes")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                chunks
                    .iter()
                    .map(|chunk| chunk.chunk_index)
                    .collect::<Vec<_>>(),
                (0..expected_chunks).collect::<Vec<_>>()
            );
            assert!(chunks
                .iter()
                .all(|chunk| chunk.chunk_count == expected_chunks));
            assert_eq!(
                estimator.calls(),
                expected_chunks,
                "one planning pass estimates each unique rendered item once"
            );
        }
    }

    #[test]
    fn direct_all_excluded_job_still_emits_exactly_one_snapshot() {
        let estimator = Arc::new(CountingEstimator::default());
        let planner = ChunkByBudgetBuilder::new()
            .estimator(estimator.clone())
            .items(|seed: &Seed| seed.items.clone())
            .render(|_item: &String, context| {
                format!(
                    "oversize-item-{}-depth-{}",
                    context.item_ordinal, context.decomposition_depth
                )
            })
            .budget(TokenCount::new(1))
            .oversize(OversizePolicy::Rerender {
                max_depth: 1,
                min_progress_tokens: TokenCount::new(1),
                exhaustion: OversizeExhaustion::Exclude,
            })
            .snapshot_excluded_items_limit(8)
            .build();
        let mut adapter = TypedTransformHandlerAdapter::new(planner);
        TransformHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));
        let outputs =
            TransformHandler::process(&adapter, seed_event(vec!["a".to_string(), "b".to_string()]))
                .expect("exclusions complete the plan");

        assert_eq!(outputs.len(), 1);
        let observed = snapshot(&outputs[0]);
        assert_eq!(observed.input_items_total, 2);
        assert_eq!(observed.planned_items_total, 0);
        assert_eq!(observed.excluded_items_total, 2);
        assert_eq!(observed.chunk_count, 0);
        assert_eq!(observed.excluded_items, Some(vec![0, 1]));
        assert_eq!(
            estimator.calls(),
            4,
            "two items at two render depths must be estimated once, not replanned"
        );
    }

    #[test]
    fn generated_chunking_uses_one_plan_for_snapshot_maps_and_manifest() {
        for items in [
            Vec::<String>::new(),
            vec!["one".to_string(), "two".to_string(), "three".to_string()],
        ] {
            let expected_chunks = items.len();
            let estimator = Arc::new(CountingEstimator::default());
            let composite_id = CompositeId::new(format!("test:generated:{expected_chunks}"));
            let parent = activated_seed_event(&composite_id, items);
            let job_key: EventId = parent.id;
            let handler = GeneratedAiChunkHandler::new(planner(estimator.clone()), composite_id);

            let outputs =
                TransformHandler::process(&handler, parent).expect("generated planning succeeds");
            assert_eq!(outputs.len(), expected_chunks + 2);
            let observed = snapshot(&outputs[0]);
            assert_eq!(observed.chunk_count, expected_chunks);

            let maps = outputs[1..=expected_chunks]
                .iter()
                .map(|event| {
                    AiMapReduceMapInput::<ChunkEnvelope<String>>::try_from_event(event)
                        .expect("map input decodes")
                })
                .collect::<Vec<_>>();
            assert_eq!(
                maps.iter()
                    .map(|input| input.chunk.chunk_index)
                    .collect::<Vec<_>>(),
                (0..expected_chunks).collect::<Vec<_>>()
            );
            assert!(maps.iter().all(|input| input.job_key == job_key));

            let manifest = AiMapReducePlanningManifest::try_from_event(
                outputs.last().expect("manifest is present"),
            )
            .expect("manifest decodes");
            assert_eq!(manifest.job_key, job_key);
            assert_eq!(manifest.chunk_count, expected_chunks);
            assert_eq!(manifest.planning.input_items_total, expected_chunks);
            assert_eq!(manifest.planning.planned_items_total, expected_chunks);
            assert_eq!(manifest.planning.excluded_items_total, 0);
            assert_eq!(observed.chunk_count, manifest.chunk_count);
            assert_eq!(
                estimator.calls(),
                expected_chunks,
                "snapshot, map inputs, and manifest must share one plan"
            );
        }
    }
}
