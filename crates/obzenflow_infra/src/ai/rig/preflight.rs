// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::AiClientError;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use url::Url;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
struct NativeRootsStatus {
    cert_count: usize,
    error_count: usize,
    first_error: Option<String>,
    ssl_cert_file: Option<String>,
    ssl_cert_dir: Option<String>,
}

impl NativeRootsStatus {
    fn load() -> Self {
        let loaded = rustls_native_certs::load_native_certs();
        Self {
            cert_count: loaded.certs.len(),
            error_count: loaded.errors.len(),
            first_error: loaded.errors.first().map(ToString::to_string),
            ssl_cert_file: std::env::var_os("SSL_CERT_FILE")
                .map(|v| v.to_string_lossy().into_owned()),
            ssl_cert_dir: std::env::var_os("SSL_CERT_DIR")
                .map(|v| v.to_string_lossy().into_owned()),
        }
    }
}

static NATIVE_ROOTS_STATUS: OnceLock<NativeRootsStatus> = OnceLock::new();

fn ensure_native_roots_for_https(provider: &str, base_url: &Url) -> Result<(), AiClientError> {
    if base_url.scheme() != "https" {
        return Ok(());
    }

    let status = NATIVE_ROOTS_STATUS.get_or_init(NativeRootsStatus::load);
    if status.cert_count > 0 {
        return Ok(());
    }

    let mut message = String::new();
    message.push_str(
        "TLS prerequisites missing: no system CA certificates found for https:// requests.\n",
    );
    message.push_str(&format!("provider: {provider}\nbase_url: {base_url}\n"));
    message.push_str(
        "Fix: install your OS CA bundle (commonly the `ca-certificates` package), or set SSL_CERT_FILE/SSL_CERT_DIR to a PEM bundle/directory.\n",
    );

    if let Some(path) = &status.ssl_cert_file {
        message.push_str(&format!("SSL_CERT_FILE={path}\n"));
    }
    if let Some(path) = &status.ssl_cert_dir {
        message.push_str(&format!("SSL_CERT_DIR={path}\n"));
    }
    if let Some(err) = &status.first_error {
        message.push_str(&format!("native cert load error: {err}\n"));
    }
    if status.error_count > 1 {
        message.push_str(&format!(
            "native cert load errors: {} total\n",
            status.error_count
        ));
    }

    Err(AiClientError::Unsupported {
        message: message.trim_end().to_string(),
    })
}

#[derive(Debug, Deserialize)]
struct OllamaTags {
    #[serde(default)]
    models: Vec<OllamaModelTag>,
}

#[derive(Debug, Deserialize)]
struct OllamaModelTag {
    name: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiModelsList {
    #[serde(default)]
    data: Vec<OpenAiModelEntry>,
}

#[derive(Debug, Deserialize)]
struct OpenAiModelEntry {
    id: String,
}

pub(crate) fn normalize_base_url_for_join(mut base_url: Url) -> Url {
    if !base_url.path().ends_with('/') {
        let path = base_url.path().trim_end_matches('/');
        base_url.set_path(&format!("{path}/"));
    }
    base_url
}

pub(crate) async fn preflight_ollama(
    base_url: &Url,
    model: Option<&str>,
) -> Result<(), AiClientError> {
    ensure_native_roots_for_https("ollama", base_url)?;

    let base_url = normalize_base_url_for_join(base_url.clone());
    let tags_url = base_url
        .join("api/tags")
        .map_err(|err| AiClientError::InvalidRequest {
            message: format!("invalid ollama base url '{base_url}': {err}"),
        })?;

    let client = reqwest::Client::new();
    let resp = client
        .get(tags_url.clone())
        .timeout(DEFAULT_TIMEOUT)
        .send()
        .await
        .map_err(|err| map_reqwest_error(err, "ollama"))?;

    if resp.status() == StatusCode::NOT_FOUND {
        // Older/alternate deployments: accept reachability if /api/version exists.
        return preflight_ollama_version(&client, &base_url).await;
    }

    if !resp.status().is_success() {
        return Err(AiClientError::Remote {
            message: format!(
                "ollama preflight failed (GET {tags_url} -> HTTP {}): check that Ollama is running and reachable",
                resp.status()
            ),
        });
    }

    let body = resp.bytes().await.map_err(|err| AiClientError::Remote {
        message: format!("ollama preflight failed to read tags response: {err}"),
    })?;
    let tags: OllamaTags = serde_json::from_slice(&body).map_err(|err| AiClientError::Remote {
        message: format!("ollama preflight failed to parse tags response: {err}"),
    })?;

    if let Some(expected) = model {
        let found = tags.models.iter().any(|m| m.name == expected);
        if !found {
            return Err(AiClientError::InvalidRequest {
                message: format!(
                    "ollama model '{expected}' not found (GET {tags_url}). Try: `ollama pull {expected}`"
                ),
            });
        }
    }

    Ok(())
}

async fn preflight_ollama_version(
    client: &reqwest::Client,
    base_url: &Url,
) -> Result<(), AiClientError> {
    let version_url =
        base_url
            .join("api/version")
            .map_err(|err| AiClientError::InvalidRequest {
                message: format!("invalid ollama base url '{base_url}': {err}"),
            })?;

    let resp = client
        .get(version_url.clone())
        .timeout(DEFAULT_TIMEOUT)
        .send()
        .await
        .map_err(|err| map_reqwest_error(err, "ollama"))?;

    if resp.status().is_success() {
        return Ok(());
    }

    Err(AiClientError::Remote {
        message: format!(
            "ollama preflight failed (GET {version_url} -> HTTP {}): check that Ollama is running and reachable",
            resp.status()
        ),
    })
}

pub(crate) async fn preflight_openai_models(
    base_url: &Url,
    api_key: &str,
    model: Option<&str>,
) -> Result<(), AiClientError> {
    ensure_native_roots_for_https("openai", base_url)?;

    let base_url = normalize_base_url_for_join(base_url.clone());
    let models_url = base_url
        .join("models")
        .map_err(|err| AiClientError::InvalidRequest {
            message: format!("invalid openai base url '{base_url}': {err}"),
        })?;

    let client = reqwest::Client::new();
    let resp = client
        .get(models_url.clone())
        .header("authorization", format!("Bearer {}", api_key.trim()))
        .timeout(DEFAULT_TIMEOUT)
        .send()
        .await
        .map_err(|err| map_reqwest_error(err, "openai"))?;

    match resp.status() {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            return Err(AiClientError::Auth {
                message: format!(
                    "openai preflight failed (GET {models_url} -> HTTP {}): check your API key",
                    resp.status()
                ),
            });
        }
        StatusCode::NOT_FOUND => {
            return Err(AiClientError::InvalidRequest {
                message: format!(
                    "openai preflight failed (GET {models_url} -> HTTP 404): base_url should typically end with `/v1`"
                ),
            });
        }
        status if !status.is_success() => {
            return Err(AiClientError::Remote {
                message: format!(
                    "openai preflight failed (GET {models_url} -> HTTP {status}): provider returned an error"
                ),
            });
        }
        _ => {}
    }

    if model.is_none() {
        return Ok(());
    }

    // Best-effort model existence check. If parsing fails, we still consider the
    // provider reachable+authorized and let actual inference handle model errors.
    let body = match resp.bytes().await {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    // Some OpenAI-compatible endpoints may not follow the canonical response schema; treat parsing as best-effort.
    let list: OpenAiModelsList = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            // If the response isn't a `{\"data\":[...]}` list, we still know the endpoint is reachable+authorized.
            let _maybe_json: Result<Value, _> = serde_json::from_slice(&body);
            return Ok(());
        }
    };

    if let Some(expected) = model {
        let found = list.data.iter().any(|m| m.id == expected);
        if !found && !list.data.is_empty() {
            return Err(AiClientError::InvalidRequest {
                message: format!(
                    "openai model '{expected}' not found in /models listing at {base_url}"
                ),
            });
        }
    }

    Ok(())
}

fn map_reqwest_error(err: reqwest::Error, provider: &str) -> AiClientError {
    if err.is_timeout() {
        return AiClientError::Timeout {
            message: format!("{provider} preflight timed out: {err}"),
        };
    }

    if err.is_connect() {
        return AiClientError::Remote {
            message: format!(
                "{provider} preflight could not connect: {err} (is the provider running and reachable?)"
            ),
        };
    }

    AiClientError::Remote {
        message: format!("{provider} preflight failed: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_base_url_adds_trailing_slash() {
        let base = Url::parse("http://localhost:8080/v1").expect("url parses");
        let normalized = normalize_base_url_for_join(base);
        assert_eq!(normalized.as_str(), "http://localhost:8080/v1/");
    }
}
