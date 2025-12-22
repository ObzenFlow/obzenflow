//! Integration test for error-kind metrics on the prometheus_100k_demo-style flow.
//!
//! This mirrors the high-volume source + error_prone_transform pipeline from
//! `examples/prometheus_100k_demo.rs`, but runs entirely under `cargo test`.
//! It asserts that:
//! - `error_processor` reports exactly 1000 Domain errors, and
//! - there are no Unknown errors for that stage.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    event::status::processing_status::ErrorKind,
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime_services::stages::transform::Map;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::{sleep, Duration};

/// Source that generates 100,000 events with a deterministic error pattern.
#[derive(Clone, Debug)]
struct HighVolumeSource {
    count: usize,
    writer_id: WriterId,
    total_events: usize,
}

impl HighVolumeSource {
    fn new(total_events: usize) -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
            total_events,
        }
    }
}

impl FiniteSourceHandler for HighVolumeSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.count >= self.total_events {
            return Ok(None);
        }

        let current_id = self.count;
        self.count += 1;

        let should_fail = current_id % 100 == 0;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            &DataRequest::versioned_event_type(),
            json!({
                "id": current_id,
                "should_fail": should_fail,
                "batch": current_id / 100,
            }),
        )]))
    }
}

/// Data request event from the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataRequest {
    id: usize,
    should_fail: bool,
    batch: usize,
}

impl TypedPayload for DataRequest {
    const EVENT_TYPE: &'static str = "data.request";
    const SCHEMA_VERSION: u32 = 1;
}

/// Successful processed event (matches example, though not inspected by this test).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedEvent {
    id: usize,
    should_fail: bool,
    batch: usize,
    processed: bool,
    processing_stage: String,
}

impl TypedPayload for ProcessedEvent {
    const EVENT_TYPE: &'static str = "processed.event";
    const SCHEMA_VERSION: u32 = 1;
}

/// Error event emitted on simulated failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorEvent {
    id: usize,
    should_fail: bool,
    batch: usize,
    error: String,
    error_code: u32,
}

impl TypedPayload for ErrorEvent {
    const EVENT_TYPE: &'static str = "error.event";
    const SCHEMA_VERSION: u32 = 1;
}

/// Transform that routes every 100th event to an error event with ErrorKind::Domain.
fn error_prone_transform() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event| {
        let mut payload = event.payload();

        if payload["should_fail"].as_bool().unwrap_or(false) {
            payload["error"] = json!("Simulated processing error");
            payload["error_code"] = json!(500);

            event.derive_error_event(
                &ErrorEvent::versioned_event_type(),
                payload,
                "Simulated processing error",
                ErrorKind::Domain,
            )
        } else {
            payload["processed"] = json!(true);
            payload["processing_stage"] = json!("error_prone_transform");

            ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                &ProcessedEvent::versioned_event_type(),
                payload,
            )
        }
    })
}

/// Simple sink that acknowledges all events.
#[derive(Clone, Debug)]
struct CompletionSink;

impl CompletionSink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for CompletionSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "completion_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::test]
async fn prometheus_100k_error_processor_error_kinds_are_domain_only() -> Result<()> {
    // Use a dedicated journal directory for this test run.
    let journal_root = std::path::PathBuf::from("target/prometheus_100k_error_kinds_test_journal");

    // Build a minimal flow that mirrors the prometheus_100k_demo core path:
    // high_volume_source -> error_processor -> completion_sink.
    let source = HighVolumeSource::new(100_000);
    let transform = error_prone_transform();
    let sink = CompletionSink::new();

    let flow_handle = flow! {
        name: "prometheus_100k_demo",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            src = source!("high_volume_source" => source);
            processor = transform!("error_processor" => transform);
            completion = sink!("completion_sink" => sink);
        },

        topology: {
            src |> processor;
            processor |> completion;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;

    // Run the flow and obtain the metrics exporter.
    let metrics_exporter = flow_handle
        .run_with_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))?
        .expect("Metrics exporter should be configured");

    // Give the metrics aggregator a brief moment to perform its final export
    // after observing pipeline completion.
    sleep(Duration::from_secs(1)).await;

    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;

    // Extract obzenflow_errors_total for stage="error_processor" by error_kind.
    let mut domain_errors: Option<u64> = None;
    let mut unknown_errors: Option<u64> = None;

    for line in metrics_text.lines() {
        if !line.starts_with("obzenflow_errors_total{") {
            continue;
        }
        if !line.contains("stage=\"error_processor\"") {
            continue;
        }

        let value_str = match line.split_whitespace().last() {
            Some(v) => v,
            None => continue,
        };

        let parsed_value: u64 = match value_str.parse() {
            Ok(v) => v,
            Err(_) => continue,
        };

        if line.contains("error_kind=\"domain\"") {
            domain_errors = Some(parsed_value);
        } else if line.contains("error_kind=\"unknown\"") {
            unknown_errors = Some(parsed_value);
        }
    }

    // We generated 100,000 events and marked every 100th as a Domain error.
    assert_eq!(
        domain_errors,
        Some(1000),
        "error_processor should report exactly 1000 domain errors"
    );

    // Unknown errors should not be present (regression check for the old 'unknown' bucket bug).
    assert!(
        unknown_errors.unwrap_or(0) == 0,
        "error_processor should not report any unknown errors, found {:?}",
        unknown_errors
    );

    Ok(())
}
