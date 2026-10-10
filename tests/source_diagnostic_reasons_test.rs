// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084n B6 acceptance: every closed diagnostic reason except
//! `Unclassified` is produced by a real connector from real input, and record
//! rejections leave an attached source breaker closed (B4).

mod replay_testkit;

use async_trait::async_trait;
use obzenflow::middleware::circuit_breaker;
use obzenflow::stages::sources::{
    AsyncFiniteSourceConnector, FiniteSourceConnector, SourceReaderInitContext,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
};
use obzenflow::stages::sources::{
    ChannelSource, CsvDecoder, CsvSource, CursorlessPullDecoder, DecodeError, HttpPullConfig,
    HttpPullSource, HttpResponse, HttpRetryConfig, SourceDiagnosticReason, SourceError,
    SourcePollErrorKind, YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection, YamlSource,
};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEvent, ChainPayload};
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError, RequestSpec};
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
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

#[derive(Clone, Debug)]
struct Items;

impl CursorlessPullDecoder for Items {
    type Output = Order;

    fn request_spec(&self) -> RequestSpec {
        RequestSpec::get("http://example.invalid/orders".parse().unwrap())
    }

    fn decode_success(&self, response: &HttpResponse) -> Result<Vec<Order>, DecodeError> {
        Ok(response.json()?)
    }
}

#[derive(Debug)]
enum Reply {
    Status(u16, Option<&'static str>),
    TimedOut,
}

#[derive(Debug)]
struct Scripted(Reply);

#[async_trait]
impl HttpClient for Scripted {
    async fn execute(&self, _request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
        match &self.0 {
            Reply::TimedOut => Err(HttpClientError::Timeout("deadline".into())),
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
    let mut source = ChannelSource::new(receiver);
    TypedAsyncInfiniteSourceHandler::next(&mut source)
        .await
        .expect_err("a closed channel cannot continue")
}

/// Exhaustive by construction: a new reason does not compile until it has a
/// real producer here, or is the explicit `Unclassified` escape.
async fn produce(reason: SourceDiagnosticReason) -> Option<SourceError> {
    use SourceDiagnosticReason::*;
    Some(match reason {
        InputUnavailable => csv_missing(),
        InputClosed => closed_channel().await,
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
        Unclassified => return None,
    })
}

#[tokio::test]
async fn every_reason_has_a_real_producer() {
    use SourceDiagnosticReason::*;
    let reasons = [
        InputUnavailable,
        InputClosed,
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

#[tokio::test(flavor = "multi_thread")]
async fn adjacent_rejections_open_no_breaker_and_reach_natural_eof() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().join("journals");
    let fixture = input(
        b"orders:\n  - {order_id: \"a\", amount_cents: 1}\n  - {order_id: \"b\", amount_cents: x}\n  - {order_id: \"c\", amount_cents: y}\n  - {order_id: \"d\", amount_cents: z}\n  - {order_id: \"e\", amount_cents: 2}\n",
    );
    let path = fixture.path().to_path_buf();
    let flow_base = base.clone();
    let definition = FlowDefinition::materialize(move |_| {
        let orders = YamlSource::builder(OrderYaml)
            .path(path.clone())
            .selection(YamlSelection::SequenceAt("/orders".into()))
            .batch_size(1)
            .build()
            .unwrap();
        let output = replay_testkit::Discard::<Order>::default();
        Ok(flow! {
            name: "yaml_breaker",
            journals: disk_journals(flow_base.clone()),
            stages: {
                orders = source!(Order => orders with {
                    circuit_breaker().consecutive_failures(1)
                });
                output = sink!(Order => output);
            },
            topology: { orders |> output; }
        })
    });
    FlowApplication::builder()
        .with_cli_args(vec![std::ffi::OsString::from("obzenflow")])
        .run_async(definition)
        .await
        .expect("rejections do not fail the flow");

    let run = replay_testkit::latest_run_dir(&base);
    let data = replay_testkit::read_stage_envelopes_appended(&run, "orders").await;
    let manifest = replay_testkit::archive_manifest(&run);
    let errors = replay_testkit::read_journal_envelopes_appended::<ChainEvent>(
        &run.join(
            manifest["stages"]["orders"]["error_journal_file"]
                .as_str()
                .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        data.iter().filter(|row| row.consumes_data_credit()).count(),
        2
    );
    assert!(data.iter().any(|row| matches!(
        row.payload,
        ChainPayload::FlowControl(FlowControlPayload::Eof {
            kind: EofKind::Natural,
            ..
        })
    )));
    assert!(!data.iter().chain(&errors).any(|row| matches!(
        row.payload,
        ChainPayload::Execution(ExecutionPayload::CircuitBreaker(_))
    )));
    let rejections: Vec<_> = errors
        .iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::SourcePollError(failure)) => {
                Some(failure.diagnostic.location().record_index())
            }
            _ => None,
        })
        .collect();
    assert_eq!(rejections, [Some(1), Some(2), Some(3)]);
}
