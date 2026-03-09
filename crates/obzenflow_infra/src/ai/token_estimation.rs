// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{
    HeuristicTokenEstimator, ResolvedTokenEstimator, TokenEstimator, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo,
};
use std::sync::Arc;

#[cfg(feature = "ai-tiktoken")]
const TOKENIZER_BACKEND_TIKTOKEN: &str = "tiktoken";

/// Best-available shared estimator for a model.
///
/// When the `ai-tiktoken` feature is enabled, this attempts to construct a
/// `TiktokenEstimator` for the provided model name and falls back to the
/// heuristic estimator when the model is not recognised. In the top-level
/// `obzenflow` crate, the `ai` umbrella feature includes tokenizer support, so
/// most callers do not need to reason about `ai-tiktoken` separately.
///
/// This is the primary caller-facing entry point. AI flows often reuse one
/// estimator across multiple stages and cloneable handlers, so the public
/// default should expose shared ownership rather than force callers to
/// translate a boxed trait object into `Arc` themselves.
pub fn estimator_for_model(model: &str) -> Arc<dyn TokenEstimator> {
    resolve_estimator_for_model(model).estimator()
}

/// Best-available shared estimator plus one-time resolution metadata for a model.
///
/// This should be preferred when operators or examples need to surface whether
/// the tokenizer path was actually exercised or why a heuristic fallback was
/// selected instead.
pub fn resolve_estimator_for_model(model: &str) -> ResolvedTokenEstimator {
    #[cfg(feature = "ai-tiktoken")]
    match super::tiktoken::TiktokenEstimator::for_model(model) {
        Ok(estimator) => ResolvedTokenEstimator::new(
            Arc::new(estimator),
            TokenEstimatorResolutionInfo::tokenizer(model, TOKENIZER_BACKEND_TIKTOKEN),
        ),
        Err(err) => {
            let fallback_detail = match err {
                obzenflow_core::ai::AiClientError::Unsupported { message }
                | obzenflow_core::ai::AiClientError::InvalidRequest { message }
                | obzenflow_core::ai::AiClientError::Other { message }
                | obzenflow_core::ai::AiClientError::Remote { message }
                | obzenflow_core::ai::AiClientError::Timeout { message }
                | obzenflow_core::ai::AiClientError::Auth { message } => message,
                obzenflow_core::ai::AiClientError::RateLimited { message, .. } => message,
            };
            ResolvedTokenEstimator::new(
                Arc::new(HeuristicTokenEstimator::default()),
                TokenEstimatorResolutionInfo::heuristic(
                    model,
                    TokenEstimatorFallbackReason::ModelNotSupportedByTokenizer,
                    Some(fallback_detail),
                )
                .with_tokenizer_backend(TOKENIZER_BACKEND_TIKTOKEN),
            )
        }
    }

    #[cfg(not(feature = "ai-tiktoken"))]
    {
        ResolvedTokenEstimator::new(
            Arc::new(HeuristicTokenEstimator::default()),
            TokenEstimatorResolutionInfo::heuristic(
                model,
                TokenEstimatorFallbackReason::TokenizerFeatureUnavailable,
                Some("ai-tiktoken feature not enabled".to_string()),
            ),
        )
    }
}

/// Best-available boxed estimator for a model.
///
/// This is the lower-level escape hatch for callers that explicitly want
/// unique ownership of the estimator trait object.
pub fn boxed_estimator_for_model(model: &str) -> Box<dyn TokenEstimator> {
    #[cfg(feature = "ai-tiktoken")]
    if let Ok(t) = super::tiktoken::TiktokenEstimator::for_model(model) {
        return Box::new(t);
    }

    #[cfg(not(feature = "ai-tiktoken"))]
    let _ = model;

    Box::new(HeuristicTokenEstimator::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{EstimateSource, TokenEstimatorFallbackReason};

    #[test]
    #[cfg(not(feature = "ai-tiktoken"))]
    fn estimator_for_model_defaults_to_heuristic_without_tiktoken() {
        let estimator = estimator_for_model("gpt-4o");
        assert_eq!(estimator.source(), EstimateSource::Heuristic);
    }

    #[test]
    #[cfg(not(feature = "ai-tiktoken"))]
    fn resolve_estimator_for_model_reports_feature_unavailable_without_tiktoken() {
        let resolved = resolve_estimator_for_model("gpt-4o");
        assert_eq!(resolved.source(), EstimateSource::Heuristic);
        assert_eq!(
            resolved.info().fallback_reason,
            Some(TokenEstimatorFallbackReason::TokenizerFeatureUnavailable)
        );
    }

    #[test]
    #[cfg(not(feature = "ai-tiktoken"))]
    fn boxed_estimator_for_model_defaults_to_heuristic_without_tiktoken() {
        let estimator = boxed_estimator_for_model("gpt-4o");
        assert_eq!(estimator.source(), EstimateSource::Heuristic);
    }

    #[test]
    #[cfg(feature = "ai-tiktoken")]
    fn estimator_for_model_prefers_tiktoken_for_known_models() {
        let estimator = estimator_for_model("gpt-4o");
        assert_eq!(estimator.source(), EstimateSource::Tokenizer);
    }

    #[test]
    #[cfg(feature = "ai-tiktoken")]
    fn resolve_estimator_for_model_reports_tiktoken_for_known_models() {
        let resolved = resolve_estimator_for_model("gpt-4o");
        assert_eq!(resolved.source(), EstimateSource::Tokenizer);
        assert_eq!(
            resolved.info().tokenizer_backend.as_deref(),
            Some("tiktoken")
        );
        assert_eq!(resolved.info().fallback_reason, None);
    }

    #[test]
    #[cfg(feature = "ai-tiktoken")]
    fn boxed_estimator_for_model_prefers_tiktoken_for_known_models() {
        let estimator = boxed_estimator_for_model("gpt-4o");
        assert_eq!(estimator.source(), EstimateSource::Tokenizer);
    }

    #[test]
    #[cfg(feature = "ai-tiktoken")]
    fn estimator_for_model_falls_back_to_heuristic_for_unknown_models() {
        let estimator = estimator_for_model("definitely-not-a-real-model");
        assert_eq!(estimator.source(), EstimateSource::Heuristic);
    }

    #[test]
    #[cfg(feature = "ai-tiktoken")]
    fn resolve_estimator_for_model_reports_unknown_model_fallback() {
        let resolved = resolve_estimator_for_model("definitely-not-a-real-model");
        assert_eq!(resolved.source(), EstimateSource::Heuristic);
        assert_eq!(
            resolved.info().tokenizer_backend.as_deref(),
            Some("tiktoken")
        );
        assert_eq!(
            resolved.info().fallback_reason,
            Some(TokenEstimatorFallbackReason::ModelNotSupportedByTokenizer)
        );
        assert!(resolved
            .info()
            .fallback_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("no tiktoken encoding")));
    }

    #[test]
    #[cfg(feature = "ai-tiktoken")]
    fn boxed_estimator_for_model_falls_back_to_heuristic_for_unknown_models() {
        let estimator = boxed_estimator_for_model("definitely-not-a-real-model");
        assert_eq!(estimator.source(), EstimateSource::Heuristic);
    }
}
