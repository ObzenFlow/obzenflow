// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Every closed diagnostic reason except `Unclassified` is produced from real
//! input. The adapters crate proves the reasons that the CSV, YAML and channel
//! handlers produce. The other reasons come from the sources below.

use async_trait::async_trait;
use obzenflow::stages::sources::{
    AsyncFiniteSourceConnector, SourceReaderInitContext, TypedAsyncFiniteSourceHandler,
};
use obzenflow::stages::sources::{
    CursorlessPullDecoder, DecodeError, HttpPullConfig, HttpPullSource, HttpResponse,
    HttpRetryConfig, SourceDiagnosticReason, SourceError, SourcePollErrorKind,
};
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError, HttpRequest};
use obzenflow_core::{StageId, TypedPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Order {
    order_id: String,
    amount_cents: i64,
}

impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "flowip_084n.reason_order";
}

fn context() -> SourceReaderInitContext {
    SourceReaderInitContext {
        stage_id: StageId::new(),
        stage_name: "orders".into(),
        flow_name: "reasons".into(),
    }
}

#[derive(Clone, Debug)]
struct Items;

impl CursorlessPullDecoder for Items {
    type Output = Order;

    fn http_request(&self) -> HttpRequest {
        HttpRequest::get("http://example.invalid/orders".parse().unwrap())
    }

    fn decode_success(&self, response: &HttpResponse) -> Result<Vec<Order>, DecodeError> {
        Ok(response.json()?)
    }
}

#[derive(Debug)]
enum Reply {
    Status(u16, Option<&'static str>),
    TimedOut,
    Unencodable,
}

#[derive(Debug)]
struct Scripted(Reply);

#[async_trait]
impl HttpClient for Scripted {
    async fn send(&self, _request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        match &self.0 {
            Reply::TimedOut => Err(HttpClientError::Timeout("deadline".into())),
            Reply::Unencodable => Err(HttpClientError::Unencodable(
                "idempotency-key: failed to parse header value".into(),
            )),
            Reply::Status(status, retry_after) => {
                let mut headers = HeaderMap::new();
                if let Some(seconds) = retry_after {
                    headers.insert("Retry-After", seconds.parse().unwrap());
                }
                Ok(HttpResponse::new(*status, headers, "SECRET_BODY"))
            }
        }
    }
}

async fn http(reply: Reply) -> SourceError {
    let config = HttpPullConfig::builder()
        .client(Arc::new(Scripted(reply)))
        .retry(HttpRetryConfig {
            transient_max_retries: 0,
            transient_backoff: Vec::new(),
            rate_limit_max_wait: Duration::from_secs(1),
        })
        .build()
        .unwrap();
    let mut reader =
        AsyncFiniteSourceConnector::open(&HttpPullSource::new(Items, config), context())
            .await
            .unwrap();
    TypedAsyncFiniteSourceHandler::next(&mut reader)
        .await
        .expect_err("the fetch fails")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SkipsEmptyTags {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
}

impl TypedPayload for SkipsEmptyTags {
    const EVENT_TYPE: &'static str = "flowip_084l.skips_empty_tags";
}

/// Hosted admission's journal-form proof (FLOWIP-084l B4).
fn unrepresentable_value() -> SourceError {
    let refused =
        obzenflow_runtime::stages::prove_journal_form(SkipsEmptyTags { tags: Vec::new() })
            .expect_err("an empty skipped list cannot be recorded faithfully");
    SourceError::Validation(refused.into_diagnostic())
}

/// Exhaustive by construction: a new reason does not compile until it has a
/// real producer here, or is the explicit `Unclassified` escape.
async fn produce(reason: SourceDiagnosticReason) -> Option<SourceError> {
    use SourceDiagnosticReason::*;
    Some(match reason {
        TimedOut => http(Reply::TimedOut).await,
        RateLimited => http(Reply::Status(429, Some("3600"))).await,
        RemoteRejected => http(Reply::Status(401, None)).await,
        RemoteFailed => http(Reply::Status(503, None)).await,
        UnrepresentableValue => unrepresentable_value(),
        InputUnavailable | InputClosed | InputChanged | MalformedInput | UnsupportedConstruct
        | DuplicateKey | SizeLimitExceeded | SelectionNotFound | UnexpectedShape | MissingField
        | UnknownField | InvalidValue | InvalidRecord | Unclassified => return None,
    })
}

#[tokio::test]
async fn every_reason_has_a_real_producer() {
    use SourceDiagnosticReason::*;
    let reasons = [
        InputUnavailable,
        InputClosed,
        InputChanged,
        TimedOut,
        RateLimited,
        RemoteRejected,
        RemoteFailed,
        MalformedInput,
        UnsupportedConstruct,
        DuplicateKey,
        SizeLimitExceeded,
        SelectionNotFound,
        UnexpectedShape,
        MissingField,
        UnknownField,
        InvalidValue,
        InvalidRecord,
        UnrepresentableValue,
        Unclassified,
    ];
    let mut produced = 0;
    for reason in reasons {
        let Some(error) = produce(reason).await else {
            continue;
        };
        produced += 1;
        assert_eq!(error.diagnostic().reason(), reason, "{error:?}");
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains("SECRET"), "{rendered}");
        assert!(
            !rendered.contains("flowip-084n"),
            "no filesystem path: {rendered}"
        );
    }
    assert_eq!(produced, 5);
}

#[tokio::test]
async fn http_categories_keep_their_health_meaning() {
    let rejected = http(Reply::Status(401, None)).await;
    assert!(rejected.is_terminal());
    assert_eq!(rejected.kind(), SourcePollErrorKind::Validation);
    let failed = http(Reply::Status(503, None)).await;
    assert_eq!(failed.kind(), SourcePollErrorKind::Transport);
    let code = failed.diagnostic().error_code().unwrap();
    assert_eq!((code.namespace(), code.value()), ("http.status", "503"));
}

#[tokio::test]
async fn http_source_unencodable_request_returns_terminal() {
    let refused = http(Reply::Unencodable).await;
    assert!(refused.is_terminal());
    assert_eq!(refused.kind(), SourcePollErrorKind::Validation);
    assert_eq!(
        refused.diagnostic().reason(),
        SourceDiagnosticReason::InvalidValue
    );
}
