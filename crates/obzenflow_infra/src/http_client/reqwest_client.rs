// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::http_client::{HttpClient, HttpClientError, HttpResponse, RequestSpec};
use obzenflow_core::web::HttpMethod;
use std::sync::OnceLock;

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

fn ensure_native_roots_for_https(url_scheme: &str) -> Result<(), HttpClientError> {
    if url_scheme != "https" {
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

    Err(HttpClientError::Transport(message.trim_end().to_string()))
}

#[derive(Debug, Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    pub fn new() -> Self {
        Self {
            client: std::panic::catch_unwind(reqwest::Client::new).unwrap_or_else(|_| {
                reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .expect("reqwest client build (no_proxy fallback)")
            }),
        }
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

fn map_method(method: HttpMethod) -> reqwest::Method {
    match method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Delete => reqwest::Method::DELETE,
        HttpMethod::Patch => reqwest::Method::PATCH,
        HttpMethod::Head => reqwest::Method::HEAD,
        HttpMethod::Options => reqwest::Method::OPTIONS,
    }
}

fn map_reqwest_error(err: reqwest::Error) -> HttpClientError {
    if err.is_timeout() {
        return HttpClientError::Timeout(err.to_string());
    }

    if err.is_connect() {
        return HttpClientError::Connection(err.to_string());
    }

    HttpClientError::Transport(err.to_string())
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn execute(&self, request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
        ensure_native_roots_for_https(request.url.scheme())?;

        let mut builder = self
            .client
            .request(map_method(request.method), request.url)
            .headers(request.headers);

        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        let response = builder.send().await.map_err(map_reqwest_error)?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.map_err(map_reqwest_error)?;

        Ok(HttpResponse::new(status, headers, body))
    }
}
