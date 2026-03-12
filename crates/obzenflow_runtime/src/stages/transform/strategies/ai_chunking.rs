// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed chunk-planning transform helpers (FLOWIP-086z).

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TransformHandler;
use crate::typing::TransformTyping;
use async_trait::async_trait;
use obzenflow_core::ai::{
    plan_chunks_by_budget, ChunkEnvelope, ChunkPlanningConfig, ChunkPlanningError,
    ChunkRenderContext, OversizePolicy, TokenCount, TokenEstimator,
};
use obzenflow_core::event::chain_event::{ChainEventContent, ChainEventFactory};
use obzenflow_core::event::observability::AiChunkingSnapshot;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::{ChainEvent, TypedPayload};
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
    _phantom: std::marker::PhantomData<(In, Item)>,
}

impl<In, Item> fmt::Debug for ChunkByBudgetBuilder<In, Item> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkByBudgetBuilder")
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
            _phantom: std::marker::PhantomData,
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

    /// Optional debug value used for `ai_chunking.snapshot` metrics.
    pub fn budget_overhead_tokens(mut self, tokens: TokenCount) -> Self {
        self.budget_overhead_tokens = tokens;
        self
    }

    /// Include at most `limit` excluded item ordinals in the `ai_chunking.snapshot` wide event.
    pub fn snapshot_excluded_items_limit(mut self, limit: usize) -> Self {
        self.snapshot_excluded_items_limit = limit;
        self
    }

    pub fn build(self) -> ChunkByBudgetTyped<In, Item> {
        let items = self.items.expect("chunk_by_budget: missing items(...)");
        let render = self.render.expect("chunk_by_budget: missing render(...)");
        let budget = self.budget.expect("chunk_by_budget: missing budget(...)");

        ChunkByBudgetTyped {
            estimator: self.estimator,
            items,
            render,
            budget,
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkByBudgetTyped")
            .field("estimator_source", &self.estimator.source())
            .field("budget", &self.budget)
            .field("max_items_per_chunk", &self.max_items_per_chunk)
            .field("oversize_policy", &self.oversize_policy)
            .field("budget_overhead_tokens", &self.budget_overhead_tokens)
            .finish()
    }
}

impl<In, Item> TransformTyping for ChunkByBudgetTyped<In, Item> {
    type Input = In;
    type Output = ChunkEnvelope<Item>;
}

#[async_trait]
impl<In, Item> TransformHandler for ChunkByBudgetTyped<In, Item>
where
    In: TypedPayload + Send + Sync + 'static,
    Item: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        if !In::event_type_matches(&event.event_type()) {
            return Ok(Vec::new());
        }

        let input = In::try_from_event(&event).map_err(|err| {
            HandlerError::Validation(format!("chunk_by_budget input decode failed: {err}"))
        })?;

        let items = (self.items)(&input);

        let config = ChunkPlanningConfig {
            budget: self.budget,
            max_items_per_chunk: self.max_items_per_chunk,
            oversize_policy: self.oversize_policy,
        };

        let plan = plan_chunks_by_budget(
            self.estimator.as_ref(),
            items,
            |item, ctx| (self.render)(item, ctx),
            config,
        )
        .map_err(|err| match err {
            ChunkPlanningError::ZeroBudget => HandlerError::Validation(err.to_string()),
            ChunkPlanningError::OversizeItem { .. }
            | ChunkPlanningError::OversizeExhausted { .. } => {
                HandlerError::Validation(err.to_string())
            }
        })?;

        let excluded_preview = if self.snapshot_excluded_items_limit == 0 {
            None
        } else {
            Some(
                plan.stats
                    .excluded_item_ordinals
                    .iter()
                    .take(self.snapshot_excluded_items_limit)
                    .copied()
                    .collect::<Vec<_>>(),
            )
        };

        let exclusions_by_reason = plan
            .stats
            .exclusions_by_reason
            .iter()
            .map(|(reason, count)| {
                let key = match reason {
                    obzenflow_core::ai::ChunkExclusionReason::MaxDepthExceeded => {
                        "max_depth_exceeded"
                    }
                    obzenflow_core::ai::ChunkExclusionReason::NoProgress => "no_progress",
                };
                (key.to_string(), *count)
            })
            .collect::<std::collections::HashMap<_, _>>();

        let snapshot_payload = AiChunkingSnapshot {
            input_items_total: plan.summary.input_items_total,
            planned_items_total: plan.summary.planned_items_total,
            excluded_items_total: plan.summary.excluded_items_total,
            chunk_count: plan.stats.chunk_count,
            rerender_attempts_total: plan.stats.rerender_attempts_total,
            max_decomposition_depth_reached: plan.stats.max_decomposition_depth_reached,
            budget_overhead_tokens: self.budget_overhead_tokens.get(),
            oversize_policy: format!("{:?}", self.oversize_policy),
            exclusions_by_reason,
            excluded_items: excluded_preview,
        };

        let snapshot_value = serde_json::to_value(&snapshot_payload).map_err(|err| {
            HandlerError::Validation(format!("ai_chunking.snapshot encode failed: {err}"))
        })?;

        let snapshot = ChainEventFactory::derived_event(
            event.writer_id,
            &event,
            ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::Custom {
                    name: "ai_chunking.snapshot".to_string(),
                    value: snapshot_value,
                    tags: None,
                },
            )),
        );

        let mut out = Vec::with_capacity(plan.chunks.len() + 1);
        out.push(snapshot);

        for chunk in plan.chunks {
            let payload = serde_json::to_value(&chunk).map_err(|err| {
                HandlerError::Validation(format!("chunk envelope encode failed: {err}"))
            })?;
            out.push(ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                ChunkEnvelope::<Item>::versioned_event_type(),
                payload,
            ));
        }

        Ok(out)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}
