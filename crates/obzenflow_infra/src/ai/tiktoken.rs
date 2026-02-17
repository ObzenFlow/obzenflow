// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{
    AiClientError, ChatRequest, ChatResponseFormat, EstimateSource, TokenCount, TokenEstimate,
    TokenEstimator,
};
use tiktoken_rs::tokenizer::Tokenizer;

const TOKENS_PER_MESSAGE: u64 = 4;
const REQUEST_OVERHEAD: u64 = 8;
const JSON_OBJECT_OVERHEAD: u64 = 16;

#[derive(Clone)]
pub struct TiktokenEstimator {
    encoding: tiktoken_rs::CoreBPE,
}

impl std::fmt::Debug for TiktokenEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiktokenEstimator")
            .field("encoding", &"<CoreBPE>")
            .finish()
    }
}

impl TiktokenEstimator {
    pub fn for_model(model: &str) -> Result<Self, AiClientError> {
        let encoding =
            tiktoken_rs::get_bpe_from_model(model).map_err(|err| AiClientError::Unsupported {
                message: format!("no tiktoken encoding for model '{model}': {err}"),
            })?;

        Ok(Self { encoding })
    }

    pub fn from_encoding(encoding_name: &str) -> Result<Self, AiClientError> {
        let tokenizer = match encoding_name {
            "o200k_harmony" => Tokenizer::O200kHarmony,
            "o200k_base" => Tokenizer::O200kBase,
            "cl100k_base" => Tokenizer::Cl100kBase,
            "p50k_base" => Tokenizer::P50kBase,
            "r50k_base" => Tokenizer::R50kBase,
            "p50k_edit" => Tokenizer::P50kEdit,
            "gpt2" => Tokenizer::Gpt2,
            _ => {
                return Err(AiClientError::Unsupported {
                    message: format!("no tiktoken encoding named '{encoding_name}'"),
                });
            }
        };

        let encoding = tiktoken_rs::get_bpe_from_tokenizer(tokenizer).map_err(|err| {
            AiClientError::Unsupported {
                message: format!("failed to initialise tiktoken encoding '{encoding_name}': {err}"),
            }
        })?;

        Ok(Self { encoding })
    }

    fn count_tokens(&self, text: &str) -> u64 {
        self.encoding.encode_ordinary(text).len() as u64
    }
}

impl TokenEstimator for TiktokenEstimator {
    fn estimate_text(&self, text: &str) -> TokenEstimate {
        TokenEstimate {
            tokens: TokenCount::new(self.count_tokens(text)),
            source: EstimateSource::Tokenizer,
        }
    }

    fn estimate_chat_request(&self, req: &ChatRequest) -> TokenEstimate {
        let mut tokens: u64 = 0;

        for msg in &req.messages {
            tokens = tokens.saturating_add(self.count_tokens(&msg.content));
        }

        for tool in &req.tools {
            if let Ok(json) = serde_json::to_string(tool) {
                tokens = tokens.saturating_add(self.count_tokens(&json));
            }
        }

        match &req.response_format {
            Some(ChatResponseFormat::JsonSchema { schema }) => {
                if let Ok(json) = serde_json::to_string(schema) {
                    tokens = tokens.saturating_add(self.count_tokens(&json));
                }
            }
            Some(ChatResponseFormat::JsonObject) => {
                tokens = tokens.saturating_add(JSON_OBJECT_OVERHEAD);
            }
            _ => {}
        }

        tokens =
            tokens.saturating_add((req.messages.len() as u64).saturating_mul(TOKENS_PER_MESSAGE));
        tokens = tokens.saturating_add(REQUEST_OVERHEAD);

        TokenEstimate {
            tokens: TokenCount::new(tokens),
            source: EstimateSource::Tokenizer,
        }
    }

    fn source(&self) -> EstimateSource {
        EstimateSource::Tokenizer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{AiProvider, ChatMessage, ChatParams, HeuristicTokenEstimator};

    #[test]
    fn for_model_unknown_returns_unsupported() {
        let err = TiktokenEstimator::for_model("definitely-not-a-real-model")
            .expect_err("unknown model should fail");
        assert!(matches!(err, AiClientError::Unsupported { .. }));
    }

    #[test]
    fn heuristic_is_within_50_percent_of_tiktoken_for_english_samples() {
        let heuristic = HeuristicTokenEstimator::default();
        let tiktoken = TiktokenEstimator::from_encoding("cl100k_base").expect("encoding exists");

        let samples = [
            "The quick brown fox jumps over the lazy dog. ".repeat(200),
            "Rust emphasises memory safety without a garbage collector. \
             Ownership and borrowing rules help prevent data races and use-after-free bugs. "
                .repeat(120),
        ];

        for sample in samples {
            let heuristic_tokens = heuristic.estimate_text(&sample).tokens.get();
            let tiktoken_tokens = tiktoken.estimate_text(&sample).tokens.get();
            assert!(tiktoken_tokens > 0);

            let delta = heuristic_tokens.abs_diff(tiktoken_tokens) as f64;
            let relative = delta / (tiktoken_tokens as f64);
            assert!(
                relative <= 0.5,
                "heuristic estimate diverged too far: heuristic={heuristic_tokens}, tiktoken={tiktoken_tokens}, relative={relative}"
            );
        }
    }

    #[test]
    fn from_encoding_resolves_known_encoding() {
        let estimator = TiktokenEstimator::from_encoding("cl100k_base").expect("should resolve");
        let tokens = estimator.estimate_text("hello world").tokens.get();
        assert!(tokens > 0);
    }

    #[test]
    fn estimate_chat_request_adds_fixed_overhead() {
        let encoding =
            tiktoken_rs::get_bpe_from_tokenizer(Tokenizer::Cl100kBase).expect("encoding exists");
        let content_tokens = encoding.encode_ordinary("hello").len() as u64;

        let estimator = TiktokenEstimator::from_encoding("cl100k_base").expect("should resolve");
        let req = ChatRequest {
            provider: AiProvider::new("openai"),
            model: "gpt-4".to_string(),
            messages: vec![ChatMessage::user("hello")],
            params: ChatParams::default(),
            tools: vec![],
            response_format: None,
        };

        let est = estimator.estimate_chat_request(&req).tokens.get();
        assert_eq!(est, content_tokens + TOKENS_PER_MESSAGE + REQUEST_OVERHEAD);
    }

    #[test]
    fn json_object_adds_overhead_tokens() {
        let estimator = TiktokenEstimator::from_encoding("cl100k_base").expect("should resolve");
        let req_base = ChatRequest {
            provider: AiProvider::new("openai"),
            model: "gpt-4".to_string(),
            messages: vec![ChatMessage::user("hello")],
            params: ChatParams::default(),
            tools: vec![],
            response_format: None,
        };

        let base = estimator.estimate_chat_request(&req_base).tokens.get();

        let mut req_json = req_base;
        req_json.response_format = Some(ChatResponseFormat::JsonObject);
        let with_json = estimator.estimate_chat_request(&req_json).tokens.get();

        assert_eq!(with_json, base + JSON_OBJECT_OVERHEAD);
    }
}
