// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Context-window-aware chunk planning helpers.
//!
//! This module provides a generic chunk envelope type and a token-budget-based planner used by
//! typed AI helpers (FLOWIP-086z).

use super::{TokenCount, TokenEstimator};
use crate::TypedPayload;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkPlanningSummary {
    pub input_items_total: usize,
    pub planned_items_total: usize,
    pub excluded_items_total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkEnvelope<T> {
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub estimated_tokens: TokenCount,
    pub decomposition_depth: u32,
    pub planning: ChunkPlanningSummary,
    pub item_ordinals: Vec<usize>,
    pub items: Vec<T>,
    pub rendered_items: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkInfo {
    pub decomposition_depth: u32,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub item_ordinals: Vec<usize>,
    pub rendered_items: Vec<String>,
}

impl ChunkInfo {
    pub fn citation_numbers_1_based(&self) -> impl Iterator<Item = usize> + '_ {
        self.item_ordinals
            .iter()
            .copied()
            .map(|ordinal| ordinal.saturating_add(1))
    }

    pub fn iter_rendered(&self) -> impl Iterator<Item = &str> + '_ {
        self.rendered_items.iter().map(String::as_str)
    }
}

impl<T> ChunkEnvelope<T> {
    pub fn chunk_info(&self) -> ChunkInfo {
        ChunkInfo {
            decomposition_depth: self.decomposition_depth,
            chunk_index: self.chunk_index,
            chunk_count: self.chunk_count,
            item_ordinals: self.item_ordinals.clone(),
            rendered_items: self.rendered_items.clone(),
        }
    }
}

// NOTE: This intentionally uses a single, stable event type. In practice this envelope is
// expected to be used within one flow for one logical item type at a time.
impl<T> TypedPayload for ChunkEnvelope<T>
where
    T: Serialize + DeserializeOwned,
{
    const EVENT_TYPE: &'static str = "ai.chunk_envelope";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OversizeExhaustion {
    Fail,
    Exclude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OversizePolicy {
    /// Treat any oversize item as a planning error.
    #[default]
    Error,
    /// Re-render oversize items at increasing decomposition depths.
    Rerender {
        max_depth: u32,
        min_progress_tokens: TokenCount,
        exhaustion: OversizeExhaustion,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRenderContext {
    pub decomposition_depth: u32,
    pub item_ordinal: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkExclusionReason {
    MaxDepthExceeded,
    NoProgress,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChunkPlanningStats {
    pub chunk_count: usize,
    pub rerender_attempts_total: u64,
    pub max_decomposition_depth_reached: u32,
    pub exclusions_by_reason: HashMap<ChunkExclusionReason, u64>,
    pub excluded_item_ordinals: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPlan<T> {
    pub chunks: Vec<ChunkEnvelope<T>>,
    pub summary: ChunkPlanningSummary,
    pub stats: ChunkPlanningStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkPlanningError {
    ZeroBudget,
    OversizeItem {
        item_ordinal: usize,
        estimated_tokens: TokenCount,
        budget: TokenCount,
    },
    OversizeExhausted {
        item_ordinal: usize,
        reason: ChunkExclusionReason,
        last_estimated_tokens: TokenCount,
        budget: TokenCount,
    },
}

impl std::fmt::Display for ChunkPlanningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBudget => f.write_str("chunk budget must be greater than zero"),
            Self::OversizeItem {
                item_ordinal,
                estimated_tokens,
                budget,
            } => write!(
                f,
                "item {item_ordinal} is oversize (estimated_tokens={estimated_tokens}, budget={budget})"
            ),
            Self::OversizeExhausted {
                item_ordinal,
                reason,
                last_estimated_tokens,
                budget,
            } => write!(
                f,
                "item {item_ordinal} could not be decomposed to fit (reason={reason:?}, last_estimated_tokens={last_estimated_tokens}, budget={budget})"
            ),
        }
    }
}

impl std::error::Error for ChunkPlanningError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPlanningConfig {
    pub budget: TokenCount,
    pub max_items_per_chunk: Option<usize>,
    pub oversize_policy: OversizePolicy,
}

impl ChunkPlanningConfig {
    pub fn new(budget: TokenCount) -> Self {
        Self {
            budget,
            max_items_per_chunk: None,
            oversize_policy: OversizePolicy::default(),
        }
    }
}

pub fn plan_chunks_by_budget<T, F>(
    estimator: &dyn TokenEstimator,
    items: Vec<T>,
    render: F,
    config: ChunkPlanningConfig,
) -> Result<ChunkPlan<T>, ChunkPlanningError>
where
    F: Fn(&T, ChunkRenderContext) -> String,
{
    if config.budget == TokenCount::ZERO {
        return Err(ChunkPlanningError::ZeroBudget);
    }

    let input_items_total = items.len();
    let mut planned_items_total: usize = 0;
    let mut excluded_items_total: usize = 0;

    let mut stats = ChunkPlanningStats::default();
    let mut estimate_cache: HashMap<String, TokenCount> = HashMap::new();

    #[derive(Debug)]
    struct PendingChunk<T> {
        depth: u32,
        estimated_tokens: TokenCount,
        ordinals: Vec<usize>,
        items: Vec<T>,
        rendered: Vec<String>,
    }

    impl<T> PendingChunk<T> {
        fn new(depth: u32) -> Self {
            Self {
                depth,
                estimated_tokens: TokenCount::ZERO,
                ordinals: Vec::new(),
                items: Vec::new(),
                rendered: Vec::new(),
            }
        }

        fn is_empty(&self) -> bool {
            self.items.is_empty()
        }

        fn len(&self) -> usize {
            self.items.len()
        }
    }

    fn cached_estimate(
        estimator: &dyn TokenEstimator,
        cache: &mut HashMap<String, TokenCount>,
        rendered: &str,
    ) -> TokenCount {
        if let Some(existing) = cache.get(rendered) {
            return *existing;
        }
        let tokens = estimator.estimate_text(rendered).tokens;
        cache.insert(rendered.to_string(), tokens);
        tokens
    }

    fn record_exclusion(
        stats: &mut ChunkPlanningStats,
        reason: ChunkExclusionReason,
        item_ordinal: usize,
    ) {
        *stats.exclusions_by_reason.entry(reason).or_insert(0) += 1;
        stats.excluded_item_ordinals.push(item_ordinal);
    }

    let mut pending: Option<PendingChunk<T>> = None;
    let mut planned_chunks: Vec<PendingChunk<T>> = Vec::new();

    for (ordinal, item) in items.into_iter().enumerate() {
        let mut depth: u32 = 0;
        let mut rendered = render(
            &item,
            ChunkRenderContext {
                decomposition_depth: depth,
                item_ordinal: ordinal,
            },
        );
        let mut tokens = cached_estimate(estimator, &mut estimate_cache, &rendered);

        if tokens > config.budget {
            match config.oversize_policy {
                OversizePolicy::Error => {
                    return Err(ChunkPlanningError::OversizeItem {
                        item_ordinal: ordinal,
                        estimated_tokens: tokens,
                        budget: config.budget,
                    });
                }
                OversizePolicy::Rerender {
                    max_depth,
                    min_progress_tokens,
                    exhaustion,
                } => {
                    let mut prev_tokens = tokens;
                    let mut attempts: u64 = 0;
                    let mut resolved: Option<(u32, String, TokenCount)> = None;
                    let mut exhausted_reason: Option<ChunkExclusionReason> = None;

                    for next_depth in 1..=max_depth {
                        attempts += 1;
                        let next_rendered = render(
                            &item,
                            ChunkRenderContext {
                                decomposition_depth: next_depth,
                                item_ordinal: ordinal,
                            },
                        );
                        let next_tokens =
                            cached_estimate(estimator, &mut estimate_cache, &next_rendered);

                        stats.max_decomposition_depth_reached =
                            stats.max_decomposition_depth_reached.max(next_depth);

                        if next_tokens <= config.budget {
                            resolved = Some((next_depth, next_rendered, next_tokens));
                            break;
                        }

                        let progress = prev_tokens.saturating_sub(next_tokens);
                        if progress < min_progress_tokens {
                            exhausted_reason = Some(ChunkExclusionReason::NoProgress);
                            prev_tokens = next_tokens;
                            break;
                        }

                        prev_tokens = next_tokens;
                    }

                    stats.rerender_attempts_total =
                        stats.rerender_attempts_total.saturating_add(attempts);

                    match resolved {
                        Some((d, r, t)) => {
                            depth = d;
                            rendered = r;
                            tokens = t;
                        }
                        None => {
                            let reason =
                                exhausted_reason.unwrap_or(ChunkExclusionReason::MaxDepthExceeded);
                            match exhaustion {
                                OversizeExhaustion::Exclude => {
                                    excluded_items_total += 1;
                                    record_exclusion(&mut stats, reason, ordinal);
                                    continue;
                                }
                                OversizeExhaustion::Fail => {
                                    return Err(ChunkPlanningError::OversizeExhausted {
                                        item_ordinal: ordinal,
                                        reason,
                                        last_estimated_tokens: prev_tokens,
                                        budget: config.budget,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Flush pending chunk if depth changes.
        if pending
            .as_ref()
            .is_some_and(|p| !p.is_empty() && p.depth != depth)
        {
            let p = pending.take().expect("pending exists");
            planned_chunks.push(p);
        }

        let max_items = config.max_items_per_chunk.unwrap_or(usize::MAX);
        if let Some(p) = pending.as_ref() {
            if !p.is_empty()
                && (p.len() >= max_items
                    || p.estimated_tokens.saturating_add(tokens) > config.budget)
            {
                let p = pending.take().expect("pending exists");
                planned_chunks.push(p);
            }
        }

        let p = pending.get_or_insert_with(|| PendingChunk::new(depth));
        debug_assert_eq!(p.depth, depth);
        p.estimated_tokens = p.estimated_tokens.saturating_add(tokens);
        p.ordinals.push(ordinal);
        p.items.push(item);
        p.rendered.push(rendered);
        planned_items_total += 1;
        stats.max_decomposition_depth_reached = stats.max_decomposition_depth_reached.max(depth);
    }

    if let Some(p) = pending.take() {
        if !p.is_empty() {
            planned_chunks.push(p);
        }
    }

    let chunk_count = planned_chunks.len();
    stats.chunk_count = chunk_count;

    let summary = ChunkPlanningSummary {
        input_items_total,
        planned_items_total,
        excluded_items_total,
    };

    let chunks = planned_chunks
        .into_iter()
        .enumerate()
        .map(|(chunk_index, p)| ChunkEnvelope {
            chunk_index,
            chunk_count,
            estimated_tokens: p.estimated_tokens,
            decomposition_depth: p.depth,
            planning: summary.clone(),
            item_ordinals: p.ordinals,
            items: p.items,
            rendered_items: p.rendered,
        })
        .collect();

    Ok(ChunkPlan {
        chunks,
        summary,
        stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::HeuristicTokenEstimator;

    fn est() -> HeuristicTokenEstimator {
        HeuristicTokenEstimator::new(1.0, 0, 0, 0).expect("valid")
    }

    #[test]
    fn packs_items_greedily_into_chunks() {
        let items = vec!["a".to_string(), "bb".to_string(), "ccc".to_string()];
        let plan = plan_chunks_by_budget(
            &est(),
            items.clone(),
            |item, _ctx| item.clone(),
            ChunkPlanningConfig::new(TokenCount::new(3)),
        )
        .expect("plan should succeed");

        assert_eq!(plan.summary.input_items_total, 3);
        assert_eq!(plan.summary.planned_items_total, 3);
        assert_eq!(plan.summary.excluded_items_total, 0);
        assert_eq!(plan.chunks.len(), 2);

        assert_eq!(plan.chunks[0].item_ordinals, vec![0, 1]);
        assert_eq!(plan.chunks[0].estimated_tokens, TokenCount::new(3));
        assert_eq!(plan.chunks[1].item_ordinals, vec![2]);
        assert_eq!(plan.chunks[1].estimated_tokens, TokenCount::new(3));
    }

    #[test]
    fn enforces_max_items_per_chunk() {
        let items = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let plan = plan_chunks_by_budget(
            &est(),
            items,
            |item, _ctx| item.clone(),
            ChunkPlanningConfig {
                budget: TokenCount::new(100),
                max_items_per_chunk: Some(2),
                oversize_policy: OversizePolicy::Error,
            },
        )
        .expect("plan should succeed");

        assert_eq!(plan.chunks.len(), 2);
        assert_eq!(plan.chunks[0].item_ordinals, vec![0, 1]);
        assert_eq!(plan.chunks[1].item_ordinals, vec![2, 3]);
    }

    #[test]
    fn oversize_policy_error_fails() {
        let items = vec!["abcdefghij".to_string()];
        let err = plan_chunks_by_budget(
            &est(),
            items,
            |item, _ctx| item.clone(),
            ChunkPlanningConfig::new(TokenCount::new(5)),
        )
        .expect_err("should fail");

        assert!(matches!(err, ChunkPlanningError::OversizeItem { .. }));
    }

    #[test]
    fn rerender_policy_can_shrink_oversize_item() {
        let items = vec!["abcdefghij".to_string()];
        let plan = plan_chunks_by_budget(
            &est(),
            items,
            |item, ctx| {
                let len = item
                    .len()
                    .saturating_sub((ctx.decomposition_depth as usize) * 2);
                item.chars().take(len).collect()
            },
            ChunkPlanningConfig {
                budget: TokenCount::new(6),
                max_items_per_chunk: None,
                oversize_policy: OversizePolicy::Rerender {
                    max_depth: 5,
                    min_progress_tokens: TokenCount::new(1),
                    exhaustion: OversizeExhaustion::Fail,
                },
            },
        )
        .expect("plan should succeed");

        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].decomposition_depth, 2);
        assert_eq!(plan.chunks[0].estimated_tokens, TokenCount::new(6));
        assert_eq!(plan.stats.rerender_attempts_total, 2);
    }

    #[test]
    fn rerender_no_progress_can_exclude_item() {
        let items = vec!["xxxxxxxxxx".to_string(), "yy".to_string()];
        let plan = plan_chunks_by_budget(
            &est(),
            items.clone(),
            |item, _ctx| item.clone(),
            ChunkPlanningConfig {
                budget: TokenCount::new(5),
                max_items_per_chunk: None,
                oversize_policy: OversizePolicy::Rerender {
                    max_depth: 5,
                    min_progress_tokens: TokenCount::new(1),
                    exhaustion: OversizeExhaustion::Exclude,
                },
            },
        )
        .expect("plan should succeed");

        assert_eq!(plan.summary.input_items_total, 2);
        assert_eq!(plan.summary.planned_items_total, 1);
        assert_eq!(plan.summary.excluded_items_total, 1);
        assert_eq!(plan.chunks.len(), 1);
        assert_eq!(plan.chunks[0].item_ordinals, vec![1]);
        assert_eq!(
            plan.stats
                .exclusions_by_reason
                .get(&ChunkExclusionReason::NoProgress)
                .copied()
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn rerender_no_progress_can_fail() {
        let items = vec!["xxxxxxxxxx".to_string()];
        let err = plan_chunks_by_budget(
            &est(),
            items,
            |item, _ctx| item.clone(),
            ChunkPlanningConfig {
                budget: TokenCount::new(5),
                max_items_per_chunk: None,
                oversize_policy: OversizePolicy::Rerender {
                    max_depth: 5,
                    min_progress_tokens: TokenCount::new(1),
                    exhaustion: OversizeExhaustion::Fail,
                },
            },
        )
        .expect_err("should fail");

        assert!(matches!(
            err,
            ChunkPlanningError::OversizeExhausted {
                reason: ChunkExclusionReason::NoProgress,
                ..
            }
        ));
    }

    #[test]
    fn chunk_info_preserves_prompt_relevant_fields() {
        let envelope = ChunkEnvelope {
            chunk_index: 2,
            chunk_count: 5,
            estimated_tokens: TokenCount::new(42),
            decomposition_depth: 1,
            planning: ChunkPlanningSummary {
                input_items_total: 10,
                planned_items_total: 9,
                excluded_items_total: 1,
            },
            item_ordinals: vec![4, 6],
            items: vec!["alpha".to_string(), "beta".to_string()],
            rendered_items: vec!["5. alpha".to_string(), "7. beta".to_string()],
        };

        let info = envelope.chunk_info();

        assert_eq!(
            info,
            ChunkInfo {
                decomposition_depth: 1,
                chunk_index: 2,
                chunk_count: 5,
                item_ordinals: vec![4, 6],
                rendered_items: vec!["5. alpha".to_string(), "7. beta".to_string()],
            }
        );
    }

    #[test]
    fn chunk_info_helpers_expose_citations_and_rendered_lines() {
        let info = ChunkInfo {
            decomposition_depth: 0,
            chunk_index: 0,
            chunk_count: 2,
            item_ordinals: vec![0, 2, 9],
            rendered_items: vec!["1. first".to_string(), "3. third".to_string()],
        };

        assert_eq!(
            info.citation_numbers_1_based().collect::<Vec<_>>(),
            vec![1, 3, 10]
        );
        assert_eq!(
            info.iter_rendered().collect::<Vec<_>>(),
            vec!["1. first", "3. third"]
        );
    }
}
