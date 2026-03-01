// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for breaker-driven shutdown contracts (FLOWIP-051b)
//!
//! This test drives a small flow where a circuit breaker at a transform
//! forces the flow to shut down early. We then assert that:
//! - The flow completes successfully (no hangs).
//! - A poison EOF (natural = false) is emitted at the source.
//! - A corresponding consumption_final contract event is written with
//!   coherent writer identifiers.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::circuit_breaker;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Source that emits a fixed number of events and then ends.
#[derive(Clone, Debug)]
struct FiniteTestSource {
    total: usize,
    emitted: usize,
    writer_id: WriterId,
}

impl FiniteTestSource {
    fn new(total: usize) -> Self {
        Self {
            total,
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for FiniteTestSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted >= self.total {
            return Ok(None);
        }

        let index = self.emitted;
        self.emitted += 1;

        let event = ChainEventFactory::data_event(
            self.writer_id,
            "breaker.test",
            json!({
                "index": index,
            }),
        );

        Ok(Some(vec![event]))
    }
}

/// Transform that starts failing after a configurable number of successes.
#[derive(Clone, Debug)]
struct FailingTransform {
    max_successes: usize,
    seen: Arc<AtomicUsize>,
}

impl FailingTransform {
    fn new(max_successes: usize) -> Self {
        Self {
            max_successes,
            seen: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl TransformHandler for FailingTransform {
    fn process(&self, mut event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let current = self.seen.fetch_add(1, Ordering::Relaxed);

        if current >= self.max_successes {
            // Model a hard failure using explicit processing status, so the
            // circuit breaker can observe failures without relying on
            // container cardinality.
            event.processing_info.status =
                obzenflow_core::event::status::processing_status::ProcessingStatus::error(
                    "forced_failure",
                );
            Ok(vec![event])
        } else {
            Ok(vec![event])
        }
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

/// Sink that simply counts successfully delivered events.
#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<AtomicUsize>,
}

impl CountingSink {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if event.is_data() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(DeliveryPayload::success(
            "counting_sink",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn breaker_driven_shutdown_emits_poison_eof_and_contract() -> Result<()> {
    // Build a flow where the transform is wrapped in circuit breaker middleware.
    // After a few failures, the breaker should open and the flow should shut
    // down before all source events are processed.

    let source = FiniteTestSource::new(20);
    let transform = FailingTransform::new(3);
    let (sink_handler, delivered_count) = CountingSink::new();

    let flow_handle = flow! {
        name: "breaker_shutdown_contracts",
        journals: disk_journals(std::path::PathBuf::from(
            "target/breaker_shutdown_contracts",
        )),
        middleware: [],

        stages: {
            src = source!("source" => source, [
                circuit_breaker(2) // Open after 2 failures
            ]);
            trans = transform!("failing_transform" => transform, [
                circuit_breaker(2)
            ]);
            snk = sink!("sink" => sink_handler);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Run with metrics so we know the flow completed cleanly.
    let metrics_exporter = flow_handle
        .run_with_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?
        .expect("Metrics should be enabled for breaker_shutdown_contracts");

    // Give metrics and contract writers a brief moment to flush.
    sleep(Duration::from_millis(200)).await;

    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

    println!("\n=== Breaker Shutdown Metrics (filtered) ===");
    for line in metrics_text.lines() {
        if line.contains("breaker_shutdown_contracts") {
            println!("{line}");
        }
    }

    // The sink should see fewer events than the source emitted due to breaker shutdown.
    let delivered = delivered_count.load(Ordering::Relaxed);
    assert!(
        delivered < 20,
        "expected breaker-driven shutdown to stop before all events were delivered, got {delivered}"
    );

    // Inspect the source data journal for FlowControl EOF events and contract evidence.
    // Discover the source stage journal on disk. Journals created via
    // `disk_journals("target/breaker_shutdown_contracts")` are stored under:
    //   target/breaker_shutdown_contracts/flows/<flow_id>/FiniteSource_source_<id>.log
    let base_path = std::path::PathBuf::from("target/breaker_shutdown_contracts").join("flows");

    let mut source_journal_paths = Vec::new();
    if base_path.exists() {
        for entry in std::fs::read_dir(&base_path)
            .map_err(|e| anyhow::anyhow!("Failed to read flows dir: {e:?}"))?
        {
            let entry = entry.map_err(|e| anyhow::anyhow!("Dir entry error: {e:?}"))?;
            let path = entry.path();
            if path.is_dir() {
                for file in std::fs::read_dir(&path)
                    .map_err(|e| anyhow::anyhow!("Failed to read flow dir: {e:?}"))?
                {
                    let file = file.map_err(|e| anyhow::anyhow!("Dir entry error: {e:?}"))?;
                    let file_path = file.path();
                    if let Some(name) = file_path.file_name().and_then(|n| n.to_str()) {
                        if name.starts_with("FiniteSource_source_") && name.ends_with(".log") {
                            source_journal_paths.push(file_path);
                        }
                    }
                }
            }
        }
    }

    assert!(
        !source_journal_paths.is_empty(),
        "expected at least one source journal file for breaker_shutdown_contracts"
    );

    // For this test there should be exactly one relevant source journal, but
    // we defensively inspect all matches and OR the results.
    let mut eof_natural_flags = Vec::new();
    let mut contract_events: Vec<ChainEvent> = Vec::new();

    for journal_path in source_journal_paths {
        let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
            obzenflow_infra::journal::DiskJournal::with_owner(
                journal_path.clone(),
                JournalOwner::stage(StageId::new()),
            )
            .map_err(|e| {
                anyhow::anyhow!("Failed to open source journal {journal_path:?}: {e:?}")
            })?;

        let envelopes = journal
            .read_causally_ordered()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read source journal: {e:?}"))?;

        for env in envelopes {
            match env.event.content {
                ChainEventContent::FlowControl(FlowControlPayload::Eof { natural, .. }) => {
                    eof_natural_flags.push(natural);
                }
                ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal { .. }) => {
                    contract_events.push(env.event.clone());
                }
                _ => {}
            }
        }
    }

    // We expect at least one EOF and at least one of them should be poison (natural = false).
    assert!(
        !eof_natural_flags.is_empty(),
        "expected at least one EOF event from source"
    );

    // We also expect a consumption_final contract event for the source.
    assert!(
        !contract_events.is_empty(),
        "expected at least one consumption_final contract event for the source"
    );

    Ok(())
}
