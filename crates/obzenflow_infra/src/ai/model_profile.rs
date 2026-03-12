// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{ChatModelProfile, ContextWindowSource, TokenCount};

use super::resolve_estimator_for_model;

fn built_in_context_window_for_model(model: &str) -> Option<TokenCount> {
    let m = model.trim().to_ascii_lowercase();

    // OpenAI (best-effort; prefer conservative values)
    if m.starts_with("gpt-4o") || m.starts_with("gpt-4.1") {
        return Some(TokenCount::new(128_000));
    }

    // Anthropic
    if m.starts_with("claude-3") || m.starts_with("claude-4") {
        return Some(TokenCount::new(200_000));
    }

    // Meta / Ollama labels often look like "llama3.1:8b".
    if m.starts_with("llama3.1") || m.starts_with("llama3.2") || m.starts_with("llama3.3") {
        return Some(TokenCount::new(128_000));
    }
    if m.starts_with("llama3") {
        return Some(TokenCount::new(8_192));
    }

    // Mistral family (best-effort)
    if m.starts_with("mistral")
        || m.starts_with("mixtral")
        || m.starts_with("codestral")
        || m.starts_with("ministral")
    {
        return Some(TokenCount::new(32_768));
    }

    // Gemma family (best-effort)
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
        assert_eq!(
            resolve_chat_model_profile("gpt-4o").context_window,
            Some(TokenCount::new(128_000))
        );
        assert_eq!(
            resolve_chat_model_profile("gpt-4.1-mini").context_window,
            Some(TokenCount::new(128_000))
        );
        assert_eq!(
            resolve_chat_model_profile("claude-3.5-sonnet").context_window,
            Some(TokenCount::new(200_000))
        );
        assert_eq!(
            resolve_chat_model_profile("llama3.1:8b").context_window,
            Some(TokenCount::new(128_000))
        );
        assert_eq!(
            resolve_chat_model_profile("mistral-large").context_window,
            Some(TokenCount::new(32_768))
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
