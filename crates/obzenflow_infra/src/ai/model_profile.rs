// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{ChatModelProfile, ContextWindowSource, TokenCount};

use super::resolve_estimator_for_model;

fn built_in_context_window_for_model(model: &str) -> Option<TokenCount> {
    let m = model.trim().to_ascii_lowercase();

    // OpenAI — more specific prefixes first to avoid shadowing by the gpt-5 catch-all.
    if m.starts_with("gpt-5.4") {
        return Some(TokenCount::new(1_000_000));
    }
    if m.starts_with("gpt-5.2") {
        return Some(TokenCount::new(256_000));
    }
    if m.starts_with("gpt-5") {
        // Conservative default for gpt-5, gpt-5-mini, and future gpt-5.x variants.
        return Some(TokenCount::new(256_000));
    }
    if m.starts_with("gpt-4.1") {
        return Some(TokenCount::new(1_000_000));
    }
    if m.starts_with("gpt-4o") {
        // Retired from ChatGPT (Feb 2026) but still available in the API.
        return Some(TokenCount::new(128_000));
    }

    // Anthropic — 200K base context; 1M is behind a beta header, so use the conservative value.
    if m.starts_with("claude-3") || m.starts_with("claude-4") {
        return Some(TokenCount::new(200_000));
    }

    // Meta — Llama 4 before Llama 3 to avoid prefix shadowing.
    if m.starts_with("llama4") || m.starts_with("llama-4") {
        // Scout is 10M, Maverick is ~1M; use 1M as a conservative default.
        return Some(TokenCount::new(1_000_000));
    }
    if m.starts_with("llama3.1") || m.starts_with("llama3.2") || m.starts_with("llama3.3") {
        return Some(TokenCount::new(128_000));
    }
    if m.starts_with("llama3") {
        return Some(TokenCount::new(8_192));
    }

    // Mistral family — Large 3 / Devstral are 256K; most others are 128K.
    // Use 128K as a conservative default for the whole family.
    if m.starts_with("mistral-large-3") || m.starts_with("devstral") {
        return Some(TokenCount::new(256_000));
    }
    if m.starts_with("mistral")
        || m.starts_with("mixtral")
        || m.starts_with("codestral")
        || m.starts_with("ministral")
    {
        return Some(TokenCount::new(128_000));
    }

    // Gemma family — Gemma 3 is 128K; earlier generations are 8K.
    if m.starts_with("gemma3") || m.starts_with("gemma-3") {
        return Some(TokenCount::new(128_000));
    }
    if m.starts_with("gemma") {
        return Some(TokenCount::new(8_192));
    }

    None
}

pub fn resolve_chat_model_profile(model: &str) -> ChatModelProfile {
    let estimator = resolve_estimator_for_model(model);
    let context_window = built_in_context_window_for_model(model);
    let source = if context_window.is_some() {
        ContextWindowSource::BuiltIn
    } else {
        ContextWindowSource::Unknown
    };

    ChatModelProfile {
        model: model.to_string(),
        estimator,
        context_window,
        context_window_source: source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_registry_recognises_common_families() {
        // OpenAI GPT-5.x
        assert_eq!(
            resolve_chat_model_profile("gpt-5.4").context_window,
            Some(TokenCount::new(1_000_000))
        );
        assert_eq!(
            resolve_chat_model_profile("gpt-5.2").context_window,
            Some(TokenCount::new(256_000))
        );
        assert_eq!(
            resolve_chat_model_profile("gpt-5-mini").context_window,
            Some(TokenCount::new(256_000))
        );

        // OpenAI GPT-4.x (legacy, still in API)
        assert_eq!(
            resolve_chat_model_profile("gpt-4.1-mini").context_window,
            Some(TokenCount::new(1_000_000))
        );
        assert_eq!(
            resolve_chat_model_profile("gpt-4o").context_window,
            Some(TokenCount::new(128_000))
        );

        // Anthropic
        assert_eq!(
            resolve_chat_model_profile("claude-3.5-sonnet").context_window,
            Some(TokenCount::new(200_000))
        );
        assert_eq!(
            resolve_chat_model_profile("claude-4-opus").context_window,
            Some(TokenCount::new(200_000))
        );

        // Meta Llama 4
        assert_eq!(
            resolve_chat_model_profile("llama4-scout").context_window,
            Some(TokenCount::new(1_000_000))
        );
        assert_eq!(
            resolve_chat_model_profile("llama4-maverick").context_window,
            Some(TokenCount::new(1_000_000))
        );

        // Meta Llama 3.x
        assert_eq!(
            resolve_chat_model_profile("llama3.1:8b").context_window,
            Some(TokenCount::new(128_000))
        );

        // Mistral family
        assert_eq!(
            resolve_chat_model_profile("mistral-large-3").context_window,
            Some(TokenCount::new(256_000))
        );
        assert_eq!(
            resolve_chat_model_profile("devstral-small-2").context_window,
            Some(TokenCount::new(256_000))
        );
        assert_eq!(
            resolve_chat_model_profile("mistral-large").context_window,
            Some(TokenCount::new(128_000))
        );
        assert_eq!(
            resolve_chat_model_profile("mistral-small-3.1").context_window,
            Some(TokenCount::new(128_000))
        );

        // Gemma family
        assert_eq!(
            resolve_chat_model_profile("gemma3:27b").context_window,
            Some(TokenCount::new(128_000))
        );
        assert_eq!(
            resolve_chat_model_profile("gemma2:9b").context_window,
            Some(TokenCount::new(8_192))
        );
    }

    #[test]
    fn unknown_models_return_unknown_context_window() {
        let profile = resolve_chat_model_profile("definitely-not-a-real-model");
        assert_eq!(profile.context_window, None);
        assert_eq!(profile.context_window_source, ContextWindowSource::Unknown);
    }
}
