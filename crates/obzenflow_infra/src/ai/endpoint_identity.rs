// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Credential-free endpoint identity for bound chat clients.

use obzenflow_core::ai::{chat_binding_fingerprint, AiProvider, ChatTarget};
use url::Url;

pub(crate) const DEFAULT_OLLAMA_BASE_URL: &str = "http://localhost:11434/";
pub(crate) const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1/";

pub(crate) fn default_ollama_base_url() -> Url {
    Url::parse(DEFAULT_OLLAMA_BASE_URL).expect("default Ollama base URL parses")
}

pub(crate) fn default_openai_base_url() -> Url {
    Url::parse(DEFAULT_OPENAI_BASE_URL).expect("default OpenAI base URL parses")
}

pub(crate) fn bound_chat_target(
    provider: impl Into<AiProvider>,
    model: impl Into<String>,
    endpoint: &Url,
) -> ChatTarget {
    let provider = provider.into();
    let model = model.into();
    let endpoint = normalised_endpoint_identity(endpoint);
    ChatTarget::with_binding_fingerprint(
        provider.clone(),
        model.clone(),
        chat_binding_fingerprint(&provider, &model, &endpoint),
    )
}

pub(crate) fn normalised_endpoint_identity(endpoint: &Url) -> String {
    let mut normalised = endpoint.clone();
    normalised.set_fragment(None);

    let path = normalised.path().trim_end_matches('/').to_string();
    normalised.set_path(if path.is_empty() { "/" } else { &path });

    // `Url` has already canonicalised scheme, host, and default ports. Rig
    // trims the path's trailing slash before appending provider routes, so do
    // the same for identity.
    normalised.as_str().trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_identity_ignores_fragment_and_trailing_slash() {
        let left = Url::parse("HTTP://EXAMPLE.COM:80/v1/#fragment").unwrap();
        let right = Url::parse("http://example.com/v1").unwrap();

        assert_eq!(
            normalised_endpoint_identity(&left),
            normalised_endpoint_identity(&right)
        );
        assert_eq!(
            bound_chat_target("openai_compatible", "model", &left),
            bound_chat_target("openai_compatible", "model", &right)
        );
    }
}
