// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::clients::http::default_http_client;

use obzenflow_core::effect_binding::{ConfigurationEvidence, HttpEndpoint};
use obzenflow_core::event::observability::{HttpPullMeasurements, ObservationRecord};
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, Url};
use obzenflow_core::BindingEvidenceError;
use obzenflow_runtime::effects::EffectPortResolutionError;
use obzenflow_runtime::queries::{QueryContext, QueryError};
use obzenflow_runtime::systems::SystemConfig;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// `HttpApi` is the HTTP system that an application port can use to read JSON.
#[obzenflow_runtime::effects::system("http-api")]
pub struct HttpApi {
    base: Url,
    client: Arc<dyn HttpClient>,
    measurements: Mutex<HashMap<String, HttpPullMeasurements>>,
}

impl HttpApi {
    pub fn json(base: Url) -> Result<JsonEndpoint, BindingEvidenceError> {
        let endpoint = HttpEndpoint::from_url(&base)?;
        Ok(JsonEndpoint { base, endpoint })
    }

    fn with_client(base: Url, client: Arc<dyn HttpClient>) -> Self {
        Self {
            base,
            client,
            measurements: Mutex::default(),
        }
    }

    /// `get_json` reads the JSON document at `path` below the base URL. A failure
    /// carries the diagnostic that the reading source reports. A refused or
    /// unreadable reply ends the read. After a timeout, a rate limit or a server
    /// failure, the source can read again.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        ctx: &QueryContext,
    ) -> Result<T, QueryError> {
        let url = self.base.join(path).map_err(|_| {
            QueryError::terminal(
                SourcePollErrorKind::Validation,
                SourceDiagnosticReason::InvalidValue,
            )
        })?;
        let response = self.client.send(HttpRequest::get(url)).await;
        self.observe(&response, ctx);
        let response = response.map_err(unsent)?;
        if !response.is_success() {
            return Err(rejected(&response));
        }
        response.json().map_err(|_| {
            QueryError::terminal(
                SourcePollErrorKind::Deserialization,
                with_status(SourceDiagnosticReason::MalformedInput, response.status),
            )
        })
    }

    fn observe(&self, response: &Result<HttpResponse, HttpClientError>, ctx: &QueryContext) {
        let mut stages = self
            .measurements
            .lock()
            .expect("HTTP measurement lock poisoned");
        if !stages.contains_key(ctx.stage_key()) {
            stages.insert(ctx.stage_key().to_owned(), HttpPullMeasurements::default());
        }
        let Some(totals) = stages.get_mut(ctx.stage_key()) else {
            return;
        };
        totals.requests_total += 1;
        if let Ok(response) = response {
            match response.status {
                200..=299 => totals.responses_2xx += 1,
                429 => {
                    totals.responses_4xx += 1;
                    totals.rate_limited_total += 1;
                }
                400..=499 => totals.responses_4xx += 1,
                500..=599 => totals.responses_5xx += 1,
                _ => {}
            }
        }
        ctx.observe(ObservationRecord::HttpPull(totals.clone()));
    }
}

fn with_status(reason: SourceDiagnosticReason, status: u16) -> SourceDiagnostic {
    let diagnostic = SourceDiagnostic::new(reason);
    match SourceErrorCode::try_new("http.status", status.to_string()) {
        Ok(code) => diagnostic.code(code),
        Err(_) => diagnostic,
    }
}

fn unsent(error: HttpClientError) -> QueryError {
    match error {
        HttpClientError::Unencodable(_) => QueryError::terminal(
            SourcePollErrorKind::Validation,
            SourceDiagnosticReason::InvalidValue,
        ),
        HttpClientError::Timeout(_) => QueryError::recoverable(
            SourcePollErrorKind::Timeout,
            SourceDiagnosticReason::TimedOut,
        ),
        _ => QueryError::recoverable(
            SourcePollErrorKind::Transport,
            SourceDiagnosticReason::InputUnavailable,
        ),
    }
}

fn rejected(response: &HttpResponse) -> QueryError {
    let status = response.status;
    match status {
        429 => QueryError::recoverable(
            SourcePollErrorKind::Transport,
            with_status(SourceDiagnosticReason::RateLimited, status),
        )
        .with_retry_after(obzenflow_adapters::effects::http::retry_after(response)),
        408 => QueryError::recoverable(
            SourcePollErrorKind::Timeout,
            with_status(SourceDiagnosticReason::TimedOut, status),
        ),
        409 | 500..=599 => QueryError::recoverable(
            SourcePollErrorKind::Transport,
            with_status(SourceDiagnosticReason::RemoteFailed, status),
        ),
        _ => QueryError::terminal(
            SourcePollErrorKind::Validation,
            with_status(SourceDiagnosticReason::RemoteRejected, status),
        ),
    }
}

/// `JsonEndpoint` is a configured JSON endpoint. Its port resolves the HTTP client
/// on the first live call.
pub struct JsonEndpoint {
    base: Url,
    endpoint: HttpEndpoint,
}

impl SystemConfig for JsonEndpoint {
    type System = HttpApi;
    type Evidence = ConfigurationEvidence;

    fn evidence(&self) -> ConfigurationEvidence {
        ConfigurationEvidence::Json {
            endpoint: self.endpoint.clone(),
        }
    }

    fn configuration(&self) -> Result<ConfigurationEvidence, BindingEvidenceError> {
        Ok(self.evidence())
    }

    fn resolve(&self) -> Result<HttpApi, EffectPortResolutionError> {
        let client = default_http_client()
            .map_err(|_| EffectPortResolutionError::ClientConstructionFailed)?;
        Ok(HttpApi::with_client(self.base.clone(), client))
    }
}

impl JsonEndpoint {
    /// `list_detail` configures the endpoint to answer
    /// [`ListDetail`](obzenflow_adapters::sources::ListDetail). `list` is the path of the
    /// JSON array of item IDs below the base URL, and `detail` returns the path of one
    /// item from its ID. The system skips an item whose document is `null`.
    pub fn list_detail(
        self,
        list: impl Into<String>,
        detail: impl Fn(&str) -> String + Send + Sync + 'static,
    ) -> list_detail::ListDetailEndpoint {
        list_detail::ListDetailEndpoint {
            endpoint: self,
            list: list.into(),
            detail: Arc::new(detail),
        }
    }
}

mod list_detail {
    use super::{HttpApi, JsonEndpoint};
    use async_trait::async_trait;
    use obzenflow_adapters::sources::ListDetail;
    use obzenflow_core::effect_binding::ConfigurationEvidence;
    use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
    use obzenflow_core::event::SourceDiagnosticReason;
    use obzenflow_core::BindingEvidenceError;
    use obzenflow_runtime::effects::EffectPortResolutionError;
    use obzenflow_runtime::queries::{QueryContext, QueryError};
    use obzenflow_runtime::systems::SystemConfig;
    use serde_json::Value;
    use std::sync::Arc;

    type DetailPath = Arc<dyn Fn(&str) -> String + Send + Sync>;

    /// The configuration of an HTTP API that answers the list-and-detail port.
    pub struct ListDetailEndpoint {
        pub(super) endpoint: JsonEndpoint,
        pub(super) list: String,
        pub(super) detail: DetailPath,
    }

    impl SystemConfig for ListDetailEndpoint {
        type System = ListDetailApi;
        type Evidence = ConfigurationEvidence;

        fn evidence(&self) -> ConfigurationEvidence {
            self.endpoint.evidence()
        }

        fn configuration(&self) -> Result<ConfigurationEvidence, BindingEvidenceError> {
            Ok(self.evidence())
        }

        fn resolve(&self) -> Result<ListDetailApi, EffectPortResolutionError> {
            Ok(ListDetailApi {
                api: self.endpoint.resolve()?,
                list: self.list.clone(),
                detail: Arc::clone(&self.detail),
            })
        }
    }

    /// The HTTP API system that answers the list-and-detail port.
    #[obzenflow_runtime::effects::system("http-api")]
    pub struct ListDetailApi {
        api: HttpApi,
        list: String,
        detail: DetailPath,
    }

    #[async_trait]
    impl ListDetail for ListDetailApi {
        async fn list(&self, ctx: &QueryContext) -> Result<Vec<String>, QueryError> {
            let ids: Vec<Value> = self.api.get_json(&self.list, ctx).await?;
            ids.into_iter()
                .map(|id| match id {
                    Value::String(id) => Ok(id),
                    Value::Number(id) => Ok(id.to_string()),
                    _ => Err(QueryError::terminal(
                        SourcePollErrorKind::Deserialization,
                        SourceDiagnosticReason::MalformedInput,
                    )),
                })
                .collect()
        }

        async fn detail(&self, id: &str, ctx: &QueryContext) -> Result<Option<Value>, QueryError> {
            let item: Option<Value> = self.api.get_json(&(self.detail)(id), ctx).await?;
            if item.is_none() {
                tracing::info!(id = %id, "the API has no item at this ID; the source skips it");
            }
            Ok(item)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::execution_payload::SourcePollContinuation;
    use obzenflow_core::event::SourceReadFailure;

    use obzenflow_core::http_client::{HeaderMap, MockHttpClient};

    fn api(outcome: Result<HttpResponse, HttpClientError>) -> HttpApi {
        let client = MockHttpClient::new();
        match outcome {
            Ok(response) => client.enqueue_response(response),
            Err(error) => client.enqueue_error(error),
        }
        HttpApi::with_client(
            "https://api.example.test/".parse().unwrap(),
            Arc::new(client),
        )
    }

    async fn failure(outcome: Result<HttpResponse, HttpClientError>) -> SourceReadFailure {
        let error = api(outcome)
            .get_json::<serde_json::Value>("v0/item.json", &QueryContext::for_test("stories", 1))
            .await
            .expect_err("the read fails");
        *error.failure
    }

    fn response(status: u16, body: &str) -> Result<HttpResponse, HttpClientError> {
        Ok(HttpResponse::new(status, HeaderMap::new(), body.to_owned()))
    }

    fn status_code(failure: &SourceReadFailure) -> Option<String> {
        failure
            .diagnostic
            .error_code()
            .filter(|code| code.namespace() == "http.status")
            .map(|code| code.value().to_owned())
    }

    #[tokio::test]
    async fn unauthorized_read_fails_terminally() {
        let failure = failure(response(401, "token sk_live_canary expired")).await;
        assert_eq!(failure.category, SourcePollErrorKind::Validation);
        assert_eq!(failure.continuation, SourcePollContinuation::Terminal);
        assert_eq!(
            failure.diagnostic.reason(),
            SourceDiagnosticReason::RemoteRejected
        );
        assert_eq!(status_code(&failure).as_deref(), Some("401"));
        let text = format!("{} {:?}", failure.diagnostic, failure.diagnostic);
        assert!(!text.contains("sk_live_canary"), "{text}");
    }

    #[tokio::test]
    async fn malformed_json_read_fails_terminally() {
        let failure = failure(response(200, "<html>not json</html>")).await;
        assert_eq!(failure.category, SourcePollErrorKind::Deserialization);
        assert_eq!(failure.continuation, SourcePollContinuation::Terminal);
        assert_eq!(
            failure.diagnostic.reason(),
            SourceDiagnosticReason::MalformedInput
        );
        assert_eq!(status_code(&failure).as_deref(), Some("200"));
        let text = format!("{} {:?}", failure.diagnostic, failure.diagnostic);
        assert!(!text.contains("not json"), "{text}");
    }

    #[tokio::test]
    async fn retryable_read_failures_continue() {
        for (outcome, category, reason) in [
            (
                response(429, ""),
                SourcePollErrorKind::Transport,
                SourceDiagnosticReason::RateLimited,
            ),
            (
                response(503, ""),
                SourcePollErrorKind::Transport,
                SourceDiagnosticReason::RemoteFailed,
            ),
            (
                response(408, ""),
                SourcePollErrorKind::Timeout,
                SourceDiagnosticReason::TimedOut,
            ),
            (
                Err(HttpClientError::Connection("refused".into())),
                SourcePollErrorKind::Transport,
                SourceDiagnosticReason::InputUnavailable,
            ),
        ] {
            let failure = failure(outcome).await;
            assert_eq!(
                (
                    failure.category,
                    failure.continuation,
                    failure.diagnostic.reason()
                ),
                (category, SourcePollContinuation::Recoverable, reason)
            );
        }
    }

    #[tokio::test]
    async fn unencodable_read_request_fails_terminally() {
        let failure = failure(Err(HttpClientError::Unencodable("bad header".into()))).await;
        assert_eq!(
            (
                failure.category,
                failure.continuation,
                failure.diagnostic.reason()
            ),
            (
                SourcePollErrorKind::Validation,
                SourcePollContinuation::Terminal,
                SourceDiagnosticReason::InvalidValue
            )
        );
    }

    #[tokio::test]
    async fn stage_requests_counted_separately() {
        let client = MockHttpClient::new();
        client.enqueue_response(HttpResponse::new(200, HeaderMap::new(), "1"));
        client.enqueue_response(HttpResponse::new(200, HeaderMap::new(), "2"));
        let api = HttpApi::with_client(
            "https://api.example.test/".parse().unwrap(),
            Arc::new(client),
        );
        for stage in ["stories", "comments"] {
            api.get_json::<u32>("v0/x.json", &QueryContext::for_test(stage, 1))
                .await
                .expect("the read succeeds");
        }
        let stages = api.measurements.lock().unwrap();
        assert_eq!(stages["stories"].requests_total, 1);
        assert_eq!(stages["comments"].requests_total, 1);
    }
}
