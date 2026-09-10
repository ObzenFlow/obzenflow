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
use obzenflow_core::{event::payloads::delivery_payload::DeliveryMethod, TypedPayload};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::transform::TryMapTyped;
use serde::{Deserialize, Serialize};

#[cfg(all(feature = "web-host", feature = "prometheus"))]
#[allow(dead_code)]
#[path = "../examples/prometheus_demo/main.rs"]
mod prometheus_demo;

#[cfg(all(feature = "web-host", feature = "prometheus"))]
#[path = "test_support/exported_jsonl.rs"]
mod exported_jsonl;

const TOTAL_EVENTS: usize = 10_000;
const ERROR_EVERY: usize = 100;
const EXPECTED_DOMAIN_ERRORS: u64 = (TOTAL_EVENTS / ERROR_EVERY) as u64;

/// Source that generates a high-volume stream with a deterministic error pattern.
#[derive(Clone, Debug)]
struct HighVolumeSource {
    count: usize,
    total_events: usize,
}

impl HighVolumeSource {
    fn new(total_events: usize) -> Self {
        Self {
            count: 0,
            total_events,
        }
    }
}

impl TypedFiniteSourceHandler for HighVolumeSource {
    type Output = DataRequest;

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.count >= self.total_events {
            return Ok(None);
        }

        let current_id = self.count;
        self.count += 1;

        let should_fail = current_id.is_multiple_of(ERROR_EVERY);

        Ok(Some(vec![DataRequest {
            id: current_id,
            should_fail,
            batch: current_id / 100,
        }]))
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
impl InlineSink for CompletionSink {
    type Input = ProcessedEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: ProcessedEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        )))
    }
}

#[tokio::test]
async fn prometheus_100k_typed_try_map_errors_are_unknown_only() -> Result<()> {
    let metrics_model =
        std::sync::Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
    let metrics_context = obzenflow_runtime::run_context::FlowBuildContext::for_tests()
        .with_metrics_exporter(metrics_model.clone());
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
    .build(metrics_context)
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    // Run the flow and obtain the metrics exporter.
    flow_handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?;
    let metrics_exporter = metrics_model.clone();

    let metrics_text = obzenflow_adapters::monitoring::projections::PrometheusProjection::new()
        .render(&metrics_exporter.snapshot())
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

/// FLOWIP-140j: run the shipped example's definition through FlowApplication
/// twice in this test executable, then compare the supported durable projection.
#[cfg(all(feature = "web-host", feature = "prometheus"))]
#[test]
fn prometheus_demo_host_preserves_data_errors_and_delivery_receipts() {
    use obzenflow_core::event::chain_event::ChainEventContent;
    use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    use obzenflow_core::WriterId;
    use obzenflow_infra::application::{FlowApplication, LogLevel};
    use serde_json::{json, Value};
    use std::collections::BTreeMap;

    let root = tempfile::tempdir_in("target").expect("example test fixture");
    let mut runs = Vec::new();
    for hosted in [false, true] {
        let directory = root.path().join(if hosted { "hosted" } else { "plain" });
        std::fs::create_dir(&directory).unwrap();
        let config = directory.join("obzenflow.toml");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        std::fs::write(
            &config,
            format!(
                r#"
[server]
enabled = {hosted}
host = "127.0.0.1"
port = {port}
startup_mode = "auto"
on_terminal = "exit"
[metrics]
# Keep reporting disabled in both runs so only hosting changes. Prometheus
# reporting requires a listener and is covered by the existing metrics tests.
enabled = false
"#
            ),
        )
        .unwrap();
        let journals = directory.join("journals");
        FlowApplication::builder()
            .with_config_file(config)
            .with_cli_args(["prometheus-host-journal-test"])
            .with_log_level(LogLevel::Error)
            .run_blocking(prometheus_demo::flow_definition(1_000, journals.clone()))
            .expect("the finite example must complete in either host mode");
        let archives: Vec<_> = std::fs::read_dir(journals.join("flows"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_dir())
            .collect();
        assert_eq!(archives.len(), 1);
        let export = directory.join("export.jsonl");
        obzenflow_infra::journal::disk::inspect::export_jsonl(&archives[0], Some(&export)).unwrap();
        let jsonl = std::fs::read_to_string(export).unwrap();
        let terminal: Vec<_> = jsonl
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter_map(|row| row["event"]["pipeline_event"].as_str().map(str::to_owned))
            .filter(|state| matches!(state.as_str(), "completed" | "cancelled" | "failed"))
            .collect();
        assert_eq!(terminal, ["completed"]);

        let manifest: Value = serde_json::from_str(
            &std::fs::read_to_string(archives[0].join("run_manifest.json")).unwrap(),
        )
        .unwrap();
        let mut final_contracts = BTreeMap::<String, usize>::new();

        let mut projection: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut data_types = BTreeMap::<String, usize>::new();
        let mut deliveries = 0;
        let mut errors = 0;
        for event in exported_jsonl::chain_events(&jsonl) {
            if matches!(event.content, ChainEventContent::FlowControl(_)) {
                let context = &event.flow_context;
                let stage = &manifest["stages"][&context.stage_name];
                assert!(
                    stage.is_object(),
                    "unknown local stage context: {context:?}"
                );
                assert_eq!(context.flow_name, manifest["flow_name"].as_str().unwrap());
                assert_eq!(context.flow_id, manifest["flow_id"].as_str().unwrap());
                assert_eq!(
                    context.stage_id.to_string(),
                    stage["stage_id"].as_str().unwrap()
                );
                assert_eq!(
                    format!("{:?}", context.stage_type),
                    stage["stage_type"].as_str().unwrap()
                );
                if matches!(
                    event.content,
                    ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal { .. })
                ) && event.writer_id == WriterId::from(context.stage_id)
                {
                    *final_contracts
                        .entry(context.stage_name.clone())
                        .or_default() += 1;
                }
            }
            if let Some(runtime) = &event.runtime_context {
                assert_ne!(
                    runtime.fsm_state, "Created",
                    "emitted snapshot from {}",
                    event.flow_context.stage_name
                );
                if event.is_eof()
                    && event.writer_id == WriterId::from(event.flow_context.stage_id)
                    && event.flow_context.stage_name == "high_volume_source"
                {
                    assert_eq!(runtime.fsm_state, "Drained");
                }
            }
            let mut content = serde_json::to_value(&event.content).unwrap();
            match &event.content {
                ChainEventContent::Data { event_type, .. } => {
                    *data_types.entry(event_type.clone()).or_default() += 1;
                }
                ChainEventContent::Delivery(_) => {
                    deliveries += 1;
                    content.as_object_mut().unwrap().remove("processed_at");
                }
                _ => continue,
            }
            errors += usize::from(!event.processing_info.status.is_success());
            projection
                .entry(event.flow_context.stage_name)
                .or_default()
                .push(
                    json!({
                        "content": content,
                        "status": event.processing_info.status,
                        "error_hops_remaining": event.processing_info.error_hops_remaining,
                    })
                    .to_string(),
                );
        }
        for rows in projection.values_mut() {
            rows.sort();
        }
        for name in [
            "high_volume_source",
            "error_processor",
            "event_counter",
            "completion_sink",
            "summary_sink",
        ] {
            assert!(
                final_contracts.get(name).copied().unwrap_or_default() > 0,
                "{name} must author a final contract with its own context"
            );
        }
        assert_eq!(
            deliveries, 991,
            "both sinks must retain every delivery receipt"
        );
        assert!(
            errors >= 10,
            "the deterministic input failures must be present"
        );
        assert!(
            !data_types
                .keys()
                .any(|kind| kind.starts_with("obzenflow.effect_")),
            "this pure example uses sink receipts and must not invent effect invocations"
        );
        runs.push((projection, data_types, deliveries, errors));
    }
    assert_eq!(
        runs[0], runs[1],
        "hosting must preserve the finite example's durable results"
    );
}

#[cfg(all(feature = "web-host", feature = "prometheus"))]
mod managed_lifecycle_regressions {
    use futures::FutureExt;
    use obzenflow_infra::application::{ApplicationError, FlowApplication, LogLevel};
    use obzenflow_runtime::__private::lifecycle;
    use obzenflow_runtime::pipeline::FlowHandle;
    use std::sync::{Arc, Mutex};

    use super::prometheus_demo;

    struct ObservedExporter {
        application: Arc<dyn obzenflow_core::metrics::MetricsSnapshotExporter>,
        observed: Arc<obzenflow_adapters::monitoring::MetricsReadModel>,
    }

    impl obzenflow_core::metrics::MetricsSnapshotExporter for ObservedExporter {
        fn publish_app_snapshot(&self, snapshot: obzenflow_core::metrics::AppMetricsSnapshot) {
            self.application.publish_app_snapshot(snapshot.clone());
            self.observed.publish_app_snapshot(snapshot);
        }
        fn publish_infra_snapshot(&self, snapshot: obzenflow_core::metrics::InfraMetricsSnapshot) {
            self.application.publish_infra_snapshot(snapshot.clone());
            self.observed.publish_infra_snapshot(snapshot);
        }
    }

    fn observe_exports(
        definition: obzenflow_dsl::FlowDefinition,
        observed: Arc<obzenflow_adapters::monitoring::MetricsReadModel>,
    ) -> obzenflow_dsl::FlowDefinition {
        obzenflow_dsl::FlowDefinition::new(move |context| async move {
            let application = context
                .metrics_exporter()
                .expect("application metrics are enabled")
                .clone();
            definition
                .build(context.with_metrics_exporter(Arc::new(ObservedExporter {
                    application,
                    observed,
                })))
                .await
        })
    }

    fn assert_final_example_metrics(model: &obzenflow_adapters::monitoring::MetricsReadModel) {
        let view = model.snapshot();
        let snapshot = view
            .app
            .as_ref()
            .expect("live aggregator published a snapshot");
        let id = |name| {
            *snapshot
                .stage_metadata
                .iter()
                .find(|(_, metadata)| metadata.name == name)
                .unwrap()
                .0
        };
        assert_eq!(snapshot.pipeline_state, "completed");
        assert_eq!(
            snapshot.events_emitted_total[&id("high_volume_source")],
            100
        );
        assert_eq!(snapshot.error_counts[&id("error_processor")], 1);
        assert_eq!(snapshot.events_accumulated_total[&id("event_counter")], 99);
        assert_eq!(snapshot.events_emitted_total[&id("event_counter")], 1);
        let text = obzenflow_adapters::monitoring::projections::PrometheusProjection::new()
            .render(&view)
            .unwrap();
        assert!(text
            .lines()
            .any(|line| line.starts_with("obzenflow_errors_total{")
                && line.contains("stage=\"error_processor\"")
                && line.ends_with(" 1")));
    }

    /// FLOWIP-142a: the shipped example, real Play route, supported export and
    /// certified current-build replay all traverse the application lifecycle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_prometheus_example_exports_100_inputs_and_replays_without_a_host() {
        prometheus_example_journal_and_metrics_proof(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhosted_prometheus_example_exports_100_inputs_and_replays_with_final_metrics() {
        prometheus_example_journal_and_metrics_proof(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopped_prometheus_example_journals_runtime_admission_and_final_metrics() {
        use obzenflow_core::event::{
            JournalEvent, PipelineCancellationCause, PipelineLifecycleEvent, PipelineStopAdmission,
            SystemEvent, SystemEventType,
        };
        use obzenflow_infra::journal::disk::log_record::LogRecord;
        use obzenflow_runtime::pipeline::{FlowStopMode, PipelineEvent};
        use obzenflow_runtime::supervised_base::SupervisorHandle;
        use std::time::Duration;

        let dir = tempfile::tempdir_in("target").unwrap();
        let config = dir.path().join("stopped.toml");
        std::fs::write(
            &config,
            "[server]\nenabled = false\n[metrics]\nenabled = true\n",
        )
        .unwrap();
        let captured = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
        let capture = captured.clone();
        let model = Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
        let application = FlowApplication::builder()
            .with_config_file(config)
            .with_cli_args(["prometheus-stopped-proof"])
            .with_log_level(LogLevel::Error)
            .with_flow_handle_hook(move |flow| {
                *capture.lock().unwrap() = Some(flow.clone());
                let flow = flow.clone();
                tokio::spawn(async move {
                    let mut reader = flow.system_journal().unwrap().reader().await.unwrap();
                    loop {
                        if let Some(row) = reader.next().await.unwrap() {
                            if row.event.writer_id == flow.pipeline_writer_id()
                                && row.event.event_type_name() == "system.pipeline.running"
                            {
                                break;
                            }
                        } else {
                            tokio::task::yield_now().await;
                        }
                    }
                    // A zero budget asks Runtime to expire immediately. This
                    // witnesses the shipped flow's facts, not elapsed timing.
                    flow.send_event(PipelineEvent::StopRequested {
                        mode: FlowStopMode::Graceful {
                            timeout: Duration::ZERO,
                        },
                        reason: None,
                    })
                    .await
                    .unwrap();
                })
            })
            .run_async(observe_exports(
                prometheus_demo::flow_definition(100_000, dir.path().join("stopped")),
                model.clone(),
            ));
        tokio::time::timeout(Duration::from_secs(10), application)
            .await
            .unwrap()
            .unwrap();
        let flow = captured.lock().unwrap().take().unwrap();
        let archive = flow.run_substrate().locator().unwrap().path();
        let export = dir.path().join("stopped.jsonl");
        obzenflow_infra::journal::disk::inspect::export_jsonl(archive, Some(&export)).unwrap();
        let jsonl = std::fs::read_to_string(export).unwrap();
        let systems: Vec<_> = jsonl
            .lines()
            .filter_map(|line| serde_json::from_str::<LogRecord<SystemEvent>>(line).ok())
            .collect();
        let admissions: Vec<_> = systems
            .iter()
            .filter_map(|row| match &row.event.event {
                SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted {
                    admission,
                }) => Some(admission),
                _ => None,
            })
            .collect();
        assert_eq!(
            admissions,
            [
                &PipelineStopAdmission::Graceful {
                    timeout_ms: obzenflow_core::event::types::DurationMs(0)
                },
                &PipelineStopAdmission::Cancel {
                    cause: PipelineCancellationCause::GracefulTimeout
                },
            ]
        );
        let terminal = systems
            .iter()
            .position(|row| row.event.event_type_name() == "system.pipeline.cancelled")
            .unwrap();
        let metrics = systems
            .iter()
            .position(|row| row.event.event_type_name() == "system.metrics.drained")
            .unwrap();
        let drained = systems
            .iter()
            .position(|row| row.event.event_type_name() == "system.pipeline.drained")
            .unwrap();
        assert!(terminal < metrics && metrics < drained);
        assert!(!systems.iter().any(|row| matches!(
            row.event.event_type_name(),
            "system.pipeline.completed" | "system.pipeline.failed" | "system.pipeline.not_started"
        )));
        assert_eq!(
            model.snapshot().app.as_ref().unwrap().pipeline_state,
            "cancelled"
        );
    }

    async fn prometheus_example_journal_and_metrics_proof(hosted: bool) {
        use obzenflow_core::event::{chain_event::ChainEventContent, JournalEvent, SystemEvent};
        use obzenflow_infra::journal::disk::log_record::LogRecord;
        use std::collections::BTreeSet;
        use std::ffi::OsString;
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir_in("target").unwrap();
        let config = dir.path().join("hosted.toml");
        let address = if hosted {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        } else {
            "127.0.0.1:9999".parse().unwrap()
        };
        std::fs::write(
            &config,
            format!(
                r#"
[server]
enabled = {hosted}
host = "127.0.0.1"
port = {}
startup_mode = "manual"
on_terminal = "exit"
[metrics]
enabled = true
"#,
                address.port()
            ),
        )
        .unwrap();
        let (flow_tx, flow_rx) = tokio::sync::oneshot::channel();
        let flow_tx = Mutex::new(Some(flow_tx));
        let model = Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
        let app = FlowApplication::builder()
            .with_config_file(config)
            .with_cli_args(["prometheus-lifecycle-proof"])
            .with_log_level(LogLevel::Error)
            .with_flow_handle_hook(move |flow| {
                assert!(flow_tx
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(flow.clone())
                    .is_ok());
                tokio::spawn(async {})
            });
        let application = tokio::spawn(app.run_async(observe_exports(
            prometheus_demo::flow_definition(100, dir.path().join("live")),
            model.clone(),
        )));
        let flow = match flow_rx.await {
            Ok(flow) => flow,
            Err(error) => panic!(
                "flow construction failed: {error}; application: {:?}",
                application.await
            ),
        };
        if hosted {
            flow.wait_for_ready().await.unwrap();
            let mut socket = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Ok(socket) = tokio::net::TcpStream::connect(address).await {
                        break socket;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            socket.write_all(b"POST /api/flow/control HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 17\r\nConnection: close\r\n\r\n{\"action\":\"play\"}").await.unwrap();
            let mut response = String::new();
            socket.read_to_string(&mut response).await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }
        tokio::time::timeout(Duration::from_secs(10), application)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _rebound = hosted.then(|| std::net::TcpListener::bind(address).unwrap());
        assert_final_example_metrics(&model);
        let archive = flow.run_substrate().locator().unwrap().path().to_path_buf();
        let export = dir.path().join("hosted.jsonl");
        obzenflow_infra::journal::disk::inspect::export_jsonl(&archive, Some(&export)).unwrap();
        let jsonl = std::fs::read_to_string(export).unwrap();
        let mut inputs = BTreeSet::new();
        let mut successes = BTreeSet::new();
        let mut errors = BTreeSet::new();
        let mut summaries = Vec::new();
        for event in super::exported_jsonl::chain_events(&jsonl) {
            if let ChainEventContent::Data { payload, .. } = &event.content {
                // Error routing retains the failed parent's source context.
                // Count its payload identity independently of the producing stage.
                if !event.processing_info.status.is_success() {
                    errors.insert(payload["id"].as_u64().unwrap());
                    continue;
                }
                match event.flow_context.stage_name.as_str() {
                    "high_volume_source" => {
                        inputs.insert(payload["id"].as_u64().unwrap());
                    }
                    "error_processor" => {
                        successes.insert(payload["id"].as_u64().unwrap());
                    }
                    "event_counter" => summaries.push(payload["event_count"].as_u64().unwrap()),
                    _ => {}
                }
            }
        }
        assert_eq!(inputs, (0..100).collect());
        assert_eq!(successes, (1..100).collect());
        assert_eq!(errors, BTreeSet::from([0]));
        assert_eq!(summaries, [99]);
        let systems: Vec<_> = jsonl
            .lines()
            .filter_map(|line| serde_json::from_str::<LogRecord<SystemEvent>>(line).ok())
            .collect();
        let terminals: Vec<_> = systems
            .iter()
            .map(|row| row.event.event_type_name())
            .filter(|kind| {
                matches!(
                    *kind,
                    "system.pipeline.completed"
                        | "system.pipeline.cancelled"
                        | "system.pipeline.failed"
                )
            })
            .collect();
        assert_eq!(terminals, ["system.pipeline.completed"]);
        assert!(systems
            .iter()
            .any(|row| row.event.event_type_name() == "system.contract.pass"));
        assert!(!systems.iter().any(|row| matches!(
            row.event.event_type_name(),
            "system.contract.fail" | "system.contract.result.failed"
        )));
        let position = |name| {
            systems
                .iter()
                .position(|row| row.event.event_type_name() == name)
                .unwrap()
        };
        let terminal = position("system.pipeline.completed");
        let drained = position("system.metrics.drained");
        assert!(terminal < drained);
        assert!(systems[terminal + 1..drained]
            .iter()
            .any(|row| row.event.event_type_name() == "system.metrics.exported"));
        assert!(drained < position("system.pipeline.drained"));
        drop(flow);

        let replay_config = dir.path().join("replay.toml");
        std::fs::write(
            &replay_config,
            "[server]\nenabled = false\n[metrics]\nenabled = true\n",
        )
        .unwrap();
        // Keeping the old port bound also proves replay needs no listener.
        // This example performs no live external I/O; zero configured source
        // inputs force the proof to reconstruct the recorded 100-input archive.
        let replay_model = Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
        FlowApplication::builder()
            .with_config_file(replay_config)
            .with_cli_args(vec![
                OsString::from("prometheus-replay-proof"),
                OsString::from("--replay-from"),
                archive.into_os_string(),
                OsString::from("--verify"),
            ])
            .with_log_level(LogLevel::Error)
            .run_async(observe_exports(
                prometheus_demo::flow_definition(0, dir.path().join("replay")),
                replay_model.clone(),
            ))
            .await
            .expect("certified replay must report zero differences (verification exit code 0)");
        assert_final_example_metrics(&replay_model);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_cors_returns_startup_error_and_stops_the_materialised_flow() {
        use obzenflow_core::event::JournalEvent;
        for startup in ["auto", "manual"] {
            for from_cli in [false, true] {
                let dir = tempfile::tempdir_in("target").unwrap();
                let config = dir.path().join("invalid-cors.toml");
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let port = listener.local_addr().unwrap().port();
                drop(listener);
                std::fs::write(
                    &config,
                    r#"
[server]
enabled = true
host = "127.0.0.1"
port = PROBE_PORT
startup_mode = "STARTUP_MODE"
on_terminal = "exit"
[server.cors]
mode = "allow-list"
allow_origins = ["CORS_ORIGIN"]
[metrics]
enabled = false
"#
                    .replace("PROBE_PORT", &port.to_string())
                    .replace("STARTUP_MODE", startup)
                    .replace(
                        "CORS_ORIGIN",
                        if from_cli {
                            "https://example.com"
                        } else {
                            "not-an-origin"
                        },
                    ),
                )
                .unwrap();
                let mut args = vec!["cors-regression"];
                if from_cli {
                    args.extend(["--cors-allow-origin", "not-an-origin"]);
                }
                let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
                let hook_observed = observed.clone();
                let app = FlowApplication::builder()
                    .with_config_file(config)
                    .with_cli_args(args)
                    .with_log_level(LogLevel::Error)
                    .with_flow_handle_hook(move |flow| {
                        *hook_observed.lock().unwrap() = Some(flow.clone());
                        tokio::spawn(async {})
                    });
                let outcome = std::panic::AssertUnwindSafe(app.run_async(
                    prometheus_demo::flow_definition(10, dir.path().join("journals")),
                ))
                .catch_unwind()
                .await;
                let flow = observed.lock().unwrap().take();
                let still_running = flow.as_ref().is_some_and(|flow| flow.is_running());
                // Preserve cleanup if this regression reintroduces an admission panic.
                if let Some(flow) = flow.as_ref().filter(|_| still_running) {
                    flow.stop_cancel().await.unwrap();
                    let _ = lifecycle::wait(flow).await;
                }
                assert!(outcome.is_ok(), "CORS admission panicked; materialised supervisor still running: {still_running}");
                assert!(
                    matches!(outcome, Ok(Err(ApplicationError::ServerStartFailed(_)))),
                    "unexpected result: {outcome:?}"
                );
                assert!(
                    !still_running,
                    "startup failure left the materialised supervisor running"
                );
                let flow = flow.expect("admission follows materialisation");
                let events = flow
                    .system_journal()
                    .unwrap()
                    .read_all_unordered()
                    .await
                    .unwrap();
                assert!(!events
                    .iter()
                    .any(|event| event.event.event_type_name() == "system.pipeline.running"));
                let _rebound = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn framework_terminal_wait_in_a_hook_does_not_fail_a_successful_application() {
        let dir = tempfile::tempdir_in("target").unwrap();
        let config = dir.path().join("terminal-wait.toml");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        std::fs::write(
            &config,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {}
startup_mode = "manual"
on_terminal = "exit"
[metrics]
enabled = false
"#,
                address.port()
            ),
        )
        .unwrap();
        let (handle_tx, handle_rx) = tokio::sync::oneshot::channel();
        let handle_tx = Arc::new(Mutex::new(Some(handle_tx)));
        let app = FlowApplication::builder()
            .with_config_file(config)
            .with_cli_args(["inspection-probe"])
            .with_log_level(LogLevel::Error)
            .with_flow_handle_hook(move |flow| {
                let flow = flow.clone();
                let tx = handle_tx.lock().unwrap().take().unwrap();
                tokio::spawn(async move {
                    let mut wait = Box::pin(lifecycle::wait(&flow));
                    // Poll the framework wait before releasing the handle to this test.
                    tokio::select! {
                        biased;
                        result = &mut wait => panic!("flow ended before manual Run: {result:?}"),
                        _ = async { assert!(tx.send(flow.clone()).is_ok()); } => {}
                    }
                    wait.await
                        .expect("the hook observes successful terminal publication");
                })
            });
        let app = tokio::spawn(app.run_async(prometheus_demo::flow_definition(
            10,
            dir.path().join("journals"),
        )));
        let flow = handle_rx.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if tokio::net::TcpStream::connect(address).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        flow.start().await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), app)
            .await
            .unwrap()
            .unwrap();
        assert!(
            result.is_ok(),
            "successful flow became an application error: {result:?}"
        );
        lifecycle::wait(&flow).await.unwrap();
        lifecycle::wait(&flow).await.unwrap();
    }
}
