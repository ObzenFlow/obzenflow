// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed chunk-planning transform helpers (FLOWIP-086z, FLOWIP-134b).

use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::{TypedTransformHandler, TypedTransformInvocation};
use obzenflow_core::ai::{
    plan_chunks_by_budget, ChunkEnvelope, ChunkPlanningConfig, ChunkPlanningError,
    ChunkRenderContext, OversizePolicy, TokenCount, TokenEstimator,
};
use obzenflow_core::event::observability::ObservationRecord;
use obzenflow_core::event::payloads::execution_payload::{AiChunkingPlannedFact, ExecutionPayload};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{StageOutputs, TypedPayload};
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
            rerender_attempts_total: Arc::new(AtomicU64::new(0)),
            maximum_depth: Arc::new(AtomicU32::new(0)),
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
    rerender_attempts_total: Arc<AtomicU64>,
    maximum_depth: Arc<AtomicU32>,
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
    plan: AiChunkingPlannedFact,
    work: ObservationRecord,
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
        let factual_plan = AiChunkingPlannedFact {
            planning: plan.summary,
            chunk_count: plan.chunks.len(),
            oversize_policy: self.oversize_policy,
            exclusions_by_reason: plan.stats.exclusions_by_reason.clone(),
        };
        let rerenders = self
            .rerender_attempts_total
            .fetch_add(plan.stats.rerender_attempts_total, Ordering::Relaxed)
            .saturating_add(plan.stats.rerender_attempts_total);
        let work = ObservationRecord::AiChunkingWork {
            rerender_attempts_total: rerenders,
            max_decomposition_depth_reached: self
                .maximum_depth
                .fetch_max(
                    plan.stats.max_decomposition_depth_reached,
                    Ordering::Relaxed,
                )
                .max(plan.stats.max_decomposition_depth_reached),
            budget_overhead_tokens: self.budget_overhead_tokens.get(),
            excluded_items: excluded_items.unwrap_or_default(),
        };

        Ok(PlannedChunks {
            chunks: plan.chunks,
            plan: factual_plan,
            work,
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
        ChunkPlanningError::OversizeItem { .. } | ChunkPlanningError::OversizeExhausted { .. } => {
            HandlerError::Validation(error.to_string())
        }
    }
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
        Ok(TypedTransformInvocation::with_framework_execution(
            StageOutputs::many(planned.chunks),
            ExecutionPayload::AiChunkingPlanned(planned.plan),
            vec![planned.work],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handlers::{TransformHandler, TypedTransformHandlerAdapter};
    use obzenflow_core::ai::{ChatRequest, EstimateSource, OversizeExhaustion, TokenEstimate};
    use obzenflow_core::event::{ChainEventFactory, ChainPayload};
    use obzenflow_core::{ChainEvent, StageId, WriterId};
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
            Seed::event_type_name(),
            Seed::payload_schema_version(),
            serde_json::json!(Seed { items }),
        )
    }

    fn plan_fact(event: &ChainEvent) -> &AiChunkingPlannedFact {
        let ChainPayload::Execution(ExecutionPayload::AiChunkingPlanned(plan)) = &event.payload
        else {
            panic!("expected durable planning evidence");
        };
        plan
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
            let observed = plan_fact(&outputs[0]);
            assert_eq!(observed.chunk_count, expected_chunks);
            assert_eq!(observed.planning.input_items_total, expected_chunks);
            assert_eq!(observed.planning.planned_items_total, expected_chunks);
            assert_eq!(observed.planning.excluded_items_total, 0);

            let chunks = outputs[1..]
                .iter()
                .map(|event| {
                    assert_eq!(
                        event.event_type(),
                        ChunkEnvelope::<String>::event_type_name()
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
        let observed = plan_fact(&outputs[0]);
        assert_eq!(observed.planning.input_items_total, 2);
        assert_eq!(observed.planning.planned_items_total, 0);
        assert_eq!(observed.planning.excluded_items_total, 2);
        assert_eq!(observed.chunk_count, 0);
        assert_eq!(observed.exclusions_by_reason.values().sum::<u64>(), 2);
        assert_eq!(
            estimator.calls(),
            4,
            "two items at two render depths must be estimated once, not replanned"
        );
    }
}
