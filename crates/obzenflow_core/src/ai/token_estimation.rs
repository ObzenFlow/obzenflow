// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Token estimation primitives
//!
//! This module provides a lightweight token estimation layer that can be used
//! by higher-level components (context packing, map-reduce splitting, token
//! budgets) to make context-window-aware decisions.

use super::{AiProvider, ChatMessage, ChatParams, ChatRequest, ChatResponseFormat, ToolDefinition};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A token count, as a newtype over u64.
///
/// Distinguishes token counts from byte counts, indices, and other integer values.
/// Conversions are explicit so accidental mixing is harder.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct TokenCount(u64);

impl TokenCount {
    pub const ZERO: Self = Self(0);

    pub fn new(tokens: u64) -> Self {
        Self(tokens)
    }

    pub fn get(self) -> u64 {
        self.0
    }

    pub fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

impl std::fmt::Display for TokenCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Source of the estimate (for observability and trust calibration).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EstimateSource {
    /// Heuristic (e.g., bytes/4). Cheap, always available, low precision.
    Heuristic,
    /// Model-specific tokenizer (e.g., tiktoken for OpenAI models). Higher precision.
    Tokenizer,
}

/// Why a heuristic estimator was selected instead of a tokenizer-backed estimator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenEstimatorFallbackReason {
    /// Tokenizer support is not compiled into the current build.
    TokenizerFeatureUnavailable,
    /// A tokenizer backend exists, but it does not recognize the selected model label.
    ModelNotSupportedByTokenizer,
    /// A heuristic estimator was selected intentionally by the caller.
    ExplicitHeuristic,
}

impl TokenEstimatorFallbackReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TokenizerFeatureUnavailable => "tokenizer_feature_unavailable",
            Self::ModelNotSupportedByTokenizer => "model_not_supported_by_tokenizer",
            Self::ExplicitHeuristic => "explicit_heuristic",
        }
    }
}

impl std::fmt::Display for TokenEstimatorFallbackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One-time estimator-selection metadata for a specific model target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenEstimatorResolutionInfo {
    pub source: EstimateSource,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer_backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<TokenEstimatorFallbackReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_detail: Option<String>,
}

impl TokenEstimatorResolutionInfo {
    pub fn tokenizer(model: impl Into<String>, tokenizer_backend: impl Into<String>) -> Self {
        Self {
            source: EstimateSource::Tokenizer,
            model: model.into(),
            tokenizer_backend: Some(tokenizer_backend.into()),
            fallback_reason: None,
            fallback_detail: None,
        }
    }

    pub fn heuristic(
        model: impl Into<String>,
        fallback_reason: TokenEstimatorFallbackReason,
        fallback_detail: Option<String>,
    ) -> Self {
        Self {
            source: EstimateSource::Heuristic,
            model: model.into(),
            tokenizer_backend: None,
            fallback_reason: Some(fallback_reason),
            fallback_detail,
        }
    }

    pub fn with_tokenizer_backend(mut self, tokenizer_backend: impl Into<String>) -> Self {
        self.tokenizer_backend = Some(tokenizer_backend.into());
        self
    }
}

/// A shared estimator plus one-time resolution metadata.
#[derive(Clone)]
pub struct ResolvedTokenEstimator {
    estimator: Arc<dyn TokenEstimator>,
    info: TokenEstimatorResolutionInfo,
}

impl std::fmt::Debug for ResolvedTokenEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedTokenEstimator")
            .field("info", &self.info)
            .finish()
    }
}

impl ResolvedTokenEstimator {
    pub fn new(estimator: Arc<dyn TokenEstimator>, info: TokenEstimatorResolutionInfo) -> Self {
        Self { estimator, info }
    }

    pub fn estimator(&self) -> Arc<dyn TokenEstimator> {
        self.estimator.clone()
    }

    pub fn info(&self) -> &TokenEstimatorResolutionInfo {
        &self.info
    }

    pub fn source(&self) -> EstimateSource {
        self.info.source
    }

    pub fn into_parts(self) -> (Arc<dyn TokenEstimator>, TokenEstimatorResolutionInfo) {
        (self.estimator, self.info)
    }
}

/// A token count estimate with provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenEstimate {
    pub tokens: TokenCount,
    pub source: EstimateSource,
}

/// Errors that can occur during token estimation or budget splitting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenEstimationError {
    /// The budget_per_group argument to split_to_budget was zero.
    ZeroBudget,
    /// A configuration value was invalid (e.g., bytes_per_token <= 0).
    InvalidConfig { message: String },
}

impl std::fmt::Display for TokenEstimationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBudget => write!(f, "budget must be greater than zero"),
            Self::InvalidConfig { message } => write!(f, "invalid config: {message}"),
        }
    }
}

impl std::error::Error for TokenEstimationError {}

/// Estimates token counts for text and chat requests.
///
/// Implementations should account for tool definitions and response format schema
/// when present. Estimation does not require hashing-grade canonical serialisation.
pub trait TokenEstimator: Send + Sync {
    /// Estimate token count for a raw string.
    fn estimate_text(&self, text: &str) -> TokenEstimate;

    /// Estimate total input token count for a chat request
    /// (messages + tools + response_format overhead).
    fn estimate_chat_request(&self, req: &ChatRequest) -> TokenEstimate;

    /// The source type this estimator produces.
    fn source(&self) -> EstimateSource;
}

/// A zero-dependency heuristic estimator.
///
/// The estimator operates on UTF-8 byte length (`str::len()`), not Unicode character count.
#[derive(Debug, Clone)]
pub struct HeuristicTokenEstimator {
    bytes_per_token: f64,
    overhead_per_message: u64,
    request_overhead: u64,
    json_object_overhead_tokens: u64,
}

impl HeuristicTokenEstimator {
    pub fn new(
        bytes_per_token: f64,
        overhead_per_message: u64,
        request_overhead: u64,
        json_object_overhead_tokens: u64,
    ) -> Result<Self, TokenEstimationError> {
        if !bytes_per_token.is_finite() || bytes_per_token <= 0.0 {
            return Err(TokenEstimationError::InvalidConfig {
                message: "bytes_per_token must be a finite value greater than zero".to_string(),
            });
        }

        Ok(Self {
            bytes_per_token,
            overhead_per_message,
            request_overhead,
            json_object_overhead_tokens,
        })
    }

    fn estimate_bytes_as_tokens(&self, bytes: usize) -> u64 {
        ((bytes as f64) / self.bytes_per_token).ceil() as u64
    }
}

impl Default for HeuristicTokenEstimator {
    fn default() -> Self {
        Self {
            bytes_per_token: 4.0,
            overhead_per_message: 4,
            request_overhead: 8,
            json_object_overhead_tokens: 16,
        }
    }
}

impl TokenEstimator for HeuristicTokenEstimator {
    fn estimate_text(&self, text: &str) -> TokenEstimate {
        let tokens = self.estimate_bytes_as_tokens(text.len());
        TokenEstimate {
            tokens: TokenCount::new(tokens),
            source: EstimateSource::Heuristic,
        }
    }

    fn estimate_chat_request(&self, req: &ChatRequest) -> TokenEstimate {
        let mut bytes: usize = 0;
        let mut message_count: u64 = 0;

        for msg in &req.messages {
            bytes += msg.content.len();
            message_count += 1;
        }

        for tool in &req.tools {
            if let Ok(json) = serde_json::to_string(tool) {
                bytes += json.len();
            }
        }

        let response_format_overhead = match &req.response_format {
            Some(ChatResponseFormat::JsonSchema { schema }) => {
                serde_json::to_string(schema).map(|j| j.len()).unwrap_or(0)
            }
            Some(ChatResponseFormat::JsonObject) => 0,
            _ => 0,
        };
        bytes += response_format_overhead;

        let json_object_extra = match &req.response_format {
            Some(ChatResponseFormat::JsonObject) => self.json_object_overhead_tokens,
            _ => 0,
        };

        let text_tokens = self.estimate_bytes_as_tokens(bytes);
        let overhead =
            (message_count * self.overhead_per_message) + self.request_overhead + json_object_extra;

        TokenEstimate {
            tokens: TokenCount::new(text_tokens.saturating_add(overhead)),
            source: EstimateSource::Heuristic,
        }
    }

    fn source(&self) -> EstimateSource {
        EstimateSource::Heuristic
    }
}

/// Given a token budget and existing messages, estimate how many tokens remain for content.
///
/// This helper estimates tokens for the provided `messages` by building a minimal chat request
/// and calling `TokenEstimator::estimate_chat_request`.
pub fn remaining_budget(
    estimator: &dyn TokenEstimator,
    budget: TokenCount,
    messages: &[ChatMessage],
) -> TokenCount {
    let req = ChatRequest {
        provider: AiProvider::new("unknown"),
        model: "unknown".to_string(),
        messages: messages.to_vec(),
        params: ChatParams::default(),
        tools: Vec::<ToolDefinition>::new(),
        response_format: None,
    };

    budget.saturating_sub(estimator.estimate_chat_request(&req).tokens)
}

/// A group of items produced by `split_to_budget`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitGroup {
    /// Indices into the original items slice.
    pub indices: Vec<usize>,
    /// Total estimated tokens for items in this group.
    pub estimated_tokens: TokenCount,
    /// True when a single item exceeds the budget on its own.
    pub oversize: bool,
}

/// Split text items into groups that each fit within a token budget.
///
/// Items are packed greedily in order. Each group's total estimated tokens will not exceed
/// `budget_per_group`, except when a single item exceeds the budget on its own. Such items
/// are placed in a solo group with `oversize = true`.
pub fn split_to_budget(
    estimator: &dyn TokenEstimator,
    items: &[&str],
    budget_per_group: TokenCount,
) -> Result<Vec<SplitGroup>, TokenEstimationError> {
    if budget_per_group == TokenCount::ZERO {
        return Err(TokenEstimationError::ZeroBudget);
    }

    let mut groups = Vec::new();
    let mut current_indices: Vec<usize> = Vec::new();
    let mut current_tokens = TokenCount::ZERO;

    for (idx, item) in items.iter().enumerate() {
        let item_tokens = estimator.estimate_text(item).tokens;

        if item_tokens > budget_per_group {
            if !current_indices.is_empty() {
                groups.push(SplitGroup {
                    indices: std::mem::take(&mut current_indices),
                    estimated_tokens: current_tokens,
                    oversize: false,
                });
                current_tokens = TokenCount::ZERO;
            }

            groups.push(SplitGroup {
                indices: vec![idx],
                estimated_tokens: item_tokens,
                oversize: true,
            });
            continue;
        }

        if current_indices.is_empty() {
            current_indices.push(idx);
            current_tokens = item_tokens;
            continue;
        }

        let next_tokens = current_tokens.saturating_add(item_tokens);
        if next_tokens <= budget_per_group {
            current_indices.push(idx);
            current_tokens = next_tokens;
        } else {
            groups.push(SplitGroup {
                indices: std::mem::take(&mut current_indices),
                estimated_tokens: current_tokens,
                oversize: false,
            });
            current_indices.push(idx);
            current_tokens = item_tokens;
        }
    }

    if !current_indices.is_empty() {
        groups.push(SplitGroup {
            indices: current_indices,
            estimated_tokens: current_tokens,
            oversize: false,
        });
    }

    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn heuristic_new_rejects_non_positive_or_non_finite_bytes_per_token() {
        assert!(matches!(
            HeuristicTokenEstimator::new(0.0, 0, 0, 0),
            Err(TokenEstimationError::InvalidConfig { .. })
        ));
        assert!(matches!(
            HeuristicTokenEstimator::new(-1.0, 0, 0, 0),
            Err(TokenEstimationError::InvalidConfig { .. })
        ));
        assert!(matches!(
            HeuristicTokenEstimator::new(f64::NAN, 0, 0, 0),
            Err(TokenEstimationError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn token_count_saturating_math_and_display() {
        let a = TokenCount::new(10);
        let b = TokenCount::new(3);
        assert_eq!(a.saturating_sub(b), TokenCount::new(7));
        assert_eq!(b.saturating_sub(a), TokenCount::ZERO);
        assert_eq!(a.to_string(), "10");
    }

    #[test]
    fn token_count_serde_round_trip() {
        let value = TokenCount::new(42);
        let json_value = serde_json::to_string(&value).expect("serialize");
        let decoded: TokenCount = serde_json::from_str(&json_value).expect("deserialize");
        assert_eq!(decoded, value);
    }

    #[test]
    fn token_estimate_and_source_serde_round_trip() {
        let estimate = TokenEstimate {
            tokens: TokenCount::new(123),
            source: EstimateSource::Heuristic,
        };
        let json_value = serde_json::to_string(&estimate).expect("serialize");
        let decoded: TokenEstimate = serde_json::from_str(&json_value).expect("deserialize");
        assert_eq!(decoded, estimate);
    }

    #[test]
    fn token_estimator_fallback_reason_display_is_machine_readable() {
        assert_eq!(
            TokenEstimatorFallbackReason::ModelNotSupportedByTokenizer.to_string(),
            "model_not_supported_by_tokenizer"
        );
    }

    #[test]
    fn token_estimator_resolution_info_heuristic_captures_reason() {
        let info = TokenEstimatorResolutionInfo::heuristic(
            "llama3.1:8b",
            TokenEstimatorFallbackReason::ModelNotSupportedByTokenizer,
            Some("no encoding".to_string()),
        );

        assert_eq!(info.source, EstimateSource::Heuristic);
        assert_eq!(info.model, "llama3.1:8b");
        assert_eq!(info.tokenizer_backend, None);
        assert_eq!(
            info.fallback_reason,
            Some(TokenEstimatorFallbackReason::ModelNotSupportedByTokenizer)
        );
        assert_eq!(info.fallback_detail.as_deref(), Some("no encoding"));
    }

    #[test]
    fn heuristic_estimate_text_uses_utf8_byte_length() {
        let est = HeuristicTokenEstimator::new(4.0, 0, 0, 0).expect("valid");
        assert_eq!(est.estimate_text("abcd").tokens, TokenCount::new(1));
        assert_eq!(est.estimate_text("abcde").tokens, TokenCount::new(2));
        assert_eq!(est.estimate_text("").tokens, TokenCount::ZERO);
    }

    #[test]
    fn heuristic_estimate_chat_request_accounts_for_tools_and_response_format() {
        let est = HeuristicTokenEstimator::new(4.0, 4, 8, 16).expect("valid");

        let tool = ToolDefinition {
            name: "t".to_string(),
            description: None,
            parameters_schema: Some(
                json!({"type": "object", "properties": {"x": {"type": "number"}}}),
            ),
        };

        let req_base = ChatRequest {
            provider: AiProvider::new("ollama"),
            model: "llama3.1:8b".to_string(),
            messages: vec![ChatMessage::user("abcd")],
            params: ChatParams::default(),
            tools: vec![],
            response_format: None,
        };

        let base = est.estimate_chat_request(&req_base).tokens.get();

        let mut req_tools = req_base.clone();
        req_tools.tools = vec![tool.clone()];
        let with_tools = est.estimate_chat_request(&req_tools).tokens.get();
        assert!(with_tools > base);

        let mut req_json_object = req_base.clone();
        req_json_object.response_format = Some(ChatResponseFormat::JsonObject);
        let with_json_object = est.estimate_chat_request(&req_json_object).tokens.get();
        assert_eq!(with_json_object, base + 16);

        let mut req_json_schema = req_base;
        req_json_schema.response_format = Some(ChatResponseFormat::JsonSchema {
            schema: json!({"type": "object", "properties": {"x": {"type": "string"}}}),
        });
        let with_json_schema = est.estimate_chat_request(&req_json_schema).tokens.get();
        assert!(with_json_schema > base);
    }

    #[test]
    fn remaining_budget_subtracts_estimated_message_tokens() {
        let est = HeuristicTokenEstimator::new(1.0, 0, 0, 0).expect("valid");
        let remaining = remaining_budget(&est, TokenCount::new(10), &[ChatMessage::user("abc")]);
        assert_eq!(remaining, TokenCount::new(7));
    }

    #[test]
    fn split_to_budget_empty_input_returns_empty_groups() {
        let est = HeuristicTokenEstimator::default();
        let groups = split_to_budget(&est, &[], TokenCount::new(10)).expect("split should succeed");
        assert_eq!(groups, Vec::<SplitGroup>::new());
    }

    #[test]
    fn split_to_budget_packs_uniform_items_to_exact_budget() {
        let est = HeuristicTokenEstimator::new(1.0, 0, 0, 0).expect("valid");
        let groups = split_to_budget(&est, &["a", "b", "c", "d", "e", "f"], TokenCount::new(3))
            .expect("split should succeed");

        assert_eq!(
            groups,
            vec![
                SplitGroup {
                    indices: vec![0, 1, 2],
                    estimated_tokens: TokenCount::new(3),
                    oversize: false
                },
                SplitGroup {
                    indices: vec![3, 4, 5],
                    estimated_tokens: TokenCount::new(3),
                    oversize: false
                }
            ]
        );
    }

    #[test]
    fn split_to_budget_handles_mixed_sizes_without_oversize() {
        let est = HeuristicTokenEstimator::new(1.0, 0, 0, 0).expect("valid");
        let groups = split_to_budget(&est, &["a", "bbb", "cc", "dddd", "ee"], TokenCount::new(5))
            .expect("split should succeed");

        assert_eq!(
            groups,
            vec![
                SplitGroup {
                    indices: vec![0, 1],
                    estimated_tokens: TokenCount::new(4),
                    oversize: false
                },
                SplitGroup {
                    indices: vec![2],
                    estimated_tokens: TokenCount::new(2),
                    oversize: false
                },
                SplitGroup {
                    indices: vec![3],
                    estimated_tokens: TokenCount::new(4),
                    oversize: false
                },
                SplitGroup {
                    indices: vec![4],
                    estimated_tokens: TokenCount::new(2),
                    oversize: false
                }
            ]
        );
    }

    #[test]
    fn split_to_budget_packs_greedily_and_marks_oversize() {
        let est = HeuristicTokenEstimator::new(1.0, 0, 0, 0).expect("valid");
        let groups = split_to_budget(&est, &["aa", "bb", "cccc"], TokenCount::new(3))
            .expect("split should succeed");

        assert_eq!(
            groups,
            vec![
                SplitGroup {
                    indices: vec![0],
                    estimated_tokens: TokenCount::new(2),
                    oversize: false
                },
                SplitGroup {
                    indices: vec![1],
                    estimated_tokens: TokenCount::new(2),
                    oversize: false
                },
                SplitGroup {
                    indices: vec![2],
                    estimated_tokens: TokenCount::new(4),
                    oversize: true
                }
            ]
        );
    }

    #[test]
    fn split_to_budget_rejects_zero_budget() {
        let est = HeuristicTokenEstimator::default();
        let err = split_to_budget(&est, &["a"], TokenCount::ZERO).expect_err("should fail");
        assert_eq!(err, TokenEstimationError::ZeroBudget);
    }
}
