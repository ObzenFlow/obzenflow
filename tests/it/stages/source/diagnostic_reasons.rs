// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084n B6 acceptance: every closed diagnostic reason except
//! `Unclassified` is produced by a real connector from real input.

use async_trait::async_trait;
use obzenflow::stages::sources::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    PositionedFiniteReader, SourceReaderInitContext, TypedAsyncFiniteSourceHandler,
    TypedAsyncInfiniteSourceHandler,
};
use obzenflow::stages::sources::{
    ChannelSource, CsvDecoder, CsvSource, CursorlessPullDecoder, DecodeError, HttpPullConfig,
    HttpPullSource, HttpResponse, HttpRetryConfig, SourceDiagnosticReason, SourceError,
    SourcePollErrorKind, YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection, YamlSource,
};
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError, HttpRequest};
use obzenflow_core::{StageId, TypedPayload};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Order {
    order_id: String,
    amount_cents: i64,
}

impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "flowip_084n.reason_order";
}

#[derive(Clone)]
struct OrderYaml;

impl YamlDecoder for OrderYaml {
    type Output = Order;

    fn decode(&self, record: YamlRecord<'_>) -> Result<Order, YamlDecodeError> {
        record.deserialize()
    }
}

fn context() -> SourceReaderInitContext {
    SourceReaderInitContext {
        stage_id: StageId::new(),
        stage_name: "orders".into(),
        flow_name: "reasons".into(),
    }
}

fn input(text: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(text).unwrap();
    file
}

fn yaml_open(text: &[u8], selection: YamlSelection, max_bytes: usize) -> SourceError {
    let file = input(text);
    YamlSource::builder(OrderYaml)
        .path(file.path())
        .selection(selection)
        .max_bytes(max_bytes)
        .build()
        .unwrap()
        .open(context())
        .expect_err("opening fails")
}

fn yaml_record(text: &[u8]) -> SourceError {
    let file = input(text);
    let mut reader = YamlSource::builder(OrderYaml)
        .path(file.path())
        .selection(YamlSelection::Sequence)
        .build()
        .unwrap()
        .open(context())
        .unwrap();
    reader.next().expect_err("the record is rejected")
}

fn orders() -> YamlSelection {
    YamlSelection::SequenceAt("/orders".into())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Tier {
    Gold,
}

#[derive(Debug, Serialize, Deserialize)]
struct TierRow {
    tier: Tier,
}

impl TypedPayload for TierRow {
    const EVENT_TYPE: &'static str = "flowip_084n.reason_tier";
}

#[derive(Clone)]
struct TierCsv;

impl CsvDecoder for TierCsv {
    type Output = TierRow;
}

fn csv_missing() -> SourceError {
    CsvSource::builder(TierCsv)
        .path("/nonexistent/flowip-084n/input.csv")
        .build()
        .unwrap()
        .open(context())
        .expect_err("missing file")
}

fn csv_unknown_variant() -> SourceError {
    let file = input(b"tier\nPlatinum\n");
    let mut reader = CsvSource::builder(TierCsv)
        .path(file.path())
        .build()
        .unwrap()
        .open(context())
        .unwrap();
    reader.next().expect_err("unknown variant is rejected")
}

fn csv_appended_between_polls() -> SourceError {
    let mut file = input(b"tier\nGold\nGold\n");
    let mut reader = CsvSource::builder(TierCsv)
        .path(file.path())
        .chunk_size(1)
        .build()
        .unwrap()
        .open(context())
        .unwrap();
    reader.next().expect("the first row reads");
    file.write_all(b"Gold\n").unwrap();
    reader.next().expect_err("the input changed")
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

async fn closed_channel() -> SourceError {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Order>(1);
    drop(sender);
    let mut reader = AsyncInfiniteSourceConnector::open(&ChannelSource::new(receiver), context())
        .await
        .unwrap();
    TypedAsyncInfiniteSourceHandler::next(&mut reader)
        .await
        .expect_err("a closed channel cannot continue")
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
        InputUnavailable => csv_missing(),
        InputClosed => closed_channel().await,
        InputChanged => csv_appended_between_polls(),
        TimedOut => http(Reply::TimedOut).await,
        RateLimited => http(Reply::Status(429, Some("3600"))).await,
        RemoteRejected => http(Reply::Status(401, None)).await,
        RemoteFailed => http(Reply::Status(503, None)).await,
        MalformedInput => yaml_open(b"orders: [\n", orders(), 1024),
        UnsupportedConstruct => yaml_open(b"a: &x 1\nb: *x\n", orders(), 1024),
        DuplicateKey => yaml_open(b"orders: []\norders: []\n", orders(), 1024),
        SizeLimitExceeded => yaml_open(b"orders: []\n", orders(), 4),
        SelectionNotFound => yaml_open(b"returns: []\n", orders(), 1024),
        UnexpectedShape => yaml_open(b"orders: 3\n", orders(), 1024),
        MissingField => yaml_record(b"- {order_id: \"a\"}\n"),
        UnknownField => yaml_record(b"- {order_id: \"a\", amount_cents: 1, extra: 2}\n"),
        InvalidValue => yaml_record(b"- {order_id: \"a\", amount_cents: lots}\n"),
        InvalidRecord => csv_unknown_variant(),
        UnrepresentableValue => unrepresentable_value(),
        Unclassified => return None,
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
    assert_eq!(produced, reasons.len() - 1);
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
