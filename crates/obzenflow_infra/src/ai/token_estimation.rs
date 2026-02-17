// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{HeuristicTokenEstimator, TokenEstimator};

/// Best-available estimator for a model.
///
/// When the `ai-tiktoken` feature is enabled, this attempts to construct a
/// `TiktokenEstimator` for the provided model name and falls back to the
/// heuristic estimator when the model is not recognised.
pub fn estimator_for_model(model: &str) -> Box<dyn TokenEstimator> {
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
    use obzenflow_core::ai::EstimateSource;

    #[test]
    #[cfg(not(feature = "ai-tiktoken"))]
    fn estimator_for_model_defaults_to_heuristic_without_tiktoken() {
        let estimator = estimator_for_model("gpt-4o");
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
    fn estimator_for_model_falls_back_to_heuristic_for_unknown_models() {
        let estimator = estimator_for_model("definitely-not-a-real-model");
        assert_eq!(estimator.source(), EstimateSource::Heuristic);
    }
}
