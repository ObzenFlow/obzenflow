// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for error-kind metrics on the prometheus volume-demo-style flow.
//!
//! This mirrors the high-volume source + error_prone_transform pipeline from
//! `examples/prometheus_demo/main.rs`, but runs entirely under `cargo test`.
//! It asserts that the typed `try_map` uses its fixed terminal-error path:
//! `error_processor` reports exactly 100 Unknown errors and no Domain errors.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::transform::TryMapTyped;
use serde::{Deserialize, Serialize};
use serde_json::json;

const TOTAL_EVENTS: usize = 10_000;
const ERROR_EVERY: usize = 100;
const EXPECTED_DOMAIN_ERRORS: u64 = (TOTAL_EVENTS / ERROR_EVERY) as u64;

/// Source that generates a high-volume stream with a deterministic error pattern.
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
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.count >= self.total_events {
            return Ok(None);
        }

        let current_id = self.count;
        self.count += 1;

        let should_fail = current_id.is_multiple_of(ERROR_EVERY);

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            DataRequest::versioned_event_type(),
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

/// Typed conversion that fails every 100th input. The supervisor owns the
/// error-marked parent and error-journal routing.
fn error_prone_transform() -> TryMapTyped<
    DataRequest,
    ProcessedEvent,
    String,
    impl Fn(DataRequest) -> Result<ProcessedEvent, String> + Send + Sync + Clone,
> {
    TryMapTyped::new(|request: DataRequest| {
        if request.should_fail {
            Err("Simulated processing error".to_string())
        } else {
            Ok(ProcessedEvent {
                id: request.id,
                should_fail: request.should_fail,
                batch: request.batch,
                processed: true,
                processing_stage: "error_prone_transform".to_string(),
            })
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
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::test]
async fn prometheus_100k_typed_try_map_errors_are_unknown_only() -> Result<()> {
    // Use a dedicated journal directory for this test run.
    let journal_root = std::path::PathBuf::from("target/prometheus_100k_error_kinds_test_journal");

    let flow_handle = FlowDefinition::materialize(move |_runtime_config| {
        // Build a minimal flow that mirrors the prometheus_100k_demo core path:
        // high_volume_source -> error_processor -> completion_sink.
        let source = HighVolumeSource::new(TOTAL_EVENTS);
        let transform = error_prone_transform();
        let sink = CompletionSink::new();

        Ok(flow! {
            name: "prometheus_100k_demo",
            journals: disk_journals(journal_root),
            middleware: [],

            stages: {
                high_volume_source = source!(DataRequest => source);
                error_processor = transform!(DataRequest -> ProcessedEvent => transform);
                completion_sink = sink!(ProcessedEvent => sink);
            },

            topology: {
                high_volume_source |> error_processor;
                error_processor |> completion_sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    // Run the flow and obtain the metrics exporter.
    let metrics_exporter = flow_handle
        .run_with_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?
        .expect("Metrics exporter should be configured");

    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

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

    // The fixed typed try-map path classifies converter failures as Unknown.
    assert_eq!(
        unknown_errors,
        Some(EXPECTED_DOMAIN_ERRORS),
        "error_processor should report exactly {EXPECTED_DOMAIN_ERRORS} unknown errors"
    );

    assert!(
        domain_errors.unwrap_or(0) == 0,
        "error_processor should not report domain errors, found {domain_errors:?}"
    );

    Ok(())
}
