// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for DeliveryContract wiring (FLOWIP-090f).
//!
//! This test runs a minimal finite flow (source -> sink) and asserts that the
//! sink edge emits a `system.contract_result` for `DeliveryContract` with
//! `status = "passed"`, proving:
//! - per-event delivery receipts are journalled by the sink supervisor, and
//! - receipts are bridged back into the upstream edge `ContractChain` via
//!   `UpstreamSubscription::notify_delivery_receipt`.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::system_event::{ContractResultStatusLabel, SystemEvent};
use obzenflow_core::event::SystemEventType;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{DeliveryContract, EventId, StageId, SystemId, WriterId};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::traits::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    CommitReceipt, FiniteSourceHandler, SinkConsumeReport, SinkHandler, SinkLifecycleReport,
    TransformHandler,
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// Source that generates a fixed number of events.
#[derive(Clone, Debug)]
struct TestEventSource {
    count: usize,
    emitted: usize,
    writer_id: WriterId,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted >= self.count {
            return Ok(None);
        }

        let index = self.emitted;
        self.emitted += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            "delivery_contract.test",
            json!({ "index": index }),
        )]))
    }
}

/// Source that generates a fixed number of data events and assigns a correlation root per event.
#[derive(Clone, Debug)]
struct CorrelatedTestEventSource {
    count: usize,
    emitted: usize,
    writer_id: WriterId,
}

impl CorrelatedTestEventSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for CorrelatedTestEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted >= self.count {
            return Ok(None);
        }

        let index = self.emitted;
        self.emitted += 1;

        let event = ChainEventFactory::data_event(
            self.writer_id,
            "delivery_contract.test",
            json!({ "index": index }),
        )
        .with_new_correlation("correlated_source");

        Ok(Some(vec![event]))
    }
}

/// Transform that fans out each input data event into N derived data events.
#[derive(Clone, Debug)]
struct FanOutTransform {
    fan_out: usize,
}

impl FanOutTransform {
    fn new(fan_out: usize) -> Self {
        Self { fan_out }
    }
}

#[async_trait]
impl TransformHandler for FanOutTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        if !event.is_data() {
            return Ok(vec![event]);
        }

        let mut out = Vec::with_capacity(self.fan_out);
        for index in 0..self.fan_out {
            out.push(ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                "delivery_contract.fan_out",
                json!({ "fan_out_index": index }),
            ));
        }

        Ok(out)
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

/// Sink that counts data events and always reports success.
#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<AtomicU64>,
}

impl CountingSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
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

/// Sink that buffers data-event acknowledgements until flush.
#[derive(Clone, Debug)]
struct BufferedCountingSink {
    count: Arc<AtomicU64>,
    pending: Arc<Mutex<Vec<EventId>>>,
}

impl BufferedCountingSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (
            Self {
                count: count.clone(),
                pending: Arc::new(Mutex::new(Vec::new())),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for BufferedCountingSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(self.consume_report(event).await?.primary)
    }

    async fn consume_report(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<SinkConsumeReport, HandlerError> {
        if event.is_data() {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.pending
                .lock()
                .expect("pending receipt buffer poisoned")
                .push(event.id);
        }

        Ok(SinkConsumeReport {
            primary: DeliveryPayload::buffered(
                "buffered_counting_sink",
                DeliveryMethod::Custom("BufferedCount".to_string()),
                None,
            ),
            commit_receipts: Vec::new(),
        })
    }

    async fn flush_report(&mut self) -> std::result::Result<SinkLifecycleReport, HandlerError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| HandlerError::Other("BufferedCountingSink mutex poisoned".to_string()))?;

        let commit_receipts = pending
            .drain(..)
            .map(|parent_event_id| CommitReceipt {
                parent_event_id,
                payload: DeliveryPayload::success(
                    "buffered_counting_sink",
                    DeliveryMethod::Custom("BufferedCount".to_string()),
                    None,
                ),
            })
            .collect();

        Ok(SinkLifecycleReport {
            audit_payload: Some(DeliveryPayload::success(
                "buffered_counting_sink",
                DeliveryMethod::Custom("BufferedCount".to_string()),
                None,
            )),
            commit_receipts,
        })
    }
}

async fn assert_delivery_contract_pass(base_path: &Path) -> Result<()> {
    sleep(Duration::from_millis(200)).await;

    let flows_dir = base_path.join("flows");
    assert!(flows_dir.exists(), "expected flows dir at {flows_dir:?}");

    let mut system_journal_paths = Vec::new();
    for entry in std::fs::read_dir(&flows_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            let system_log = path.join("system.log");
            if system_log.exists() {
                system_journal_paths.push(system_log);
            }
        }
    }

    assert!(
        !system_journal_paths.is_empty(),
        "expected at least one system.log under {flows_dir:?}"
    );

    let mut seen_delivery_contract_pass = false;
    let mut seen_delivery_contract_fail = false;

    for system_log in system_journal_paths {
        let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
            obzenflow_infra::journal::DiskJournal::with_owner(
                system_log.clone(),
                JournalOwner::system(SystemId::new()),
            )?;

        let envelopes = journal.read_causally_ordered().await?;
        for env in envelopes {
            match &env.event.event {
                SystemEventType::ContractResult {
                    contract_name,
                    status,
                    ..
                } if contract_name == DeliveryContract::NAME => match status.as_str() {
                    s if s == ContractResultStatusLabel::Passed.as_str() => {
                        seen_delivery_contract_pass = true
                    }
                    s if s == ContractResultStatusLabel::Failed.as_str() => {
                        seen_delivery_contract_fail = true
                    }
                    s if s == ContractResultStatusLabel::Healthy.as_str() => {}
                    s if s == ContractResultStatusLabel::Pending.as_str() => {}
                    other => {
                        return Err(anyhow::anyhow!(
                            "unexpected DeliveryContract status in system.log: {other}"
                        ));
                    }
                },
                _ => {}
            }
        }
    }

    assert!(
        seen_delivery_contract_pass,
        "expected a passed DeliveryContract result in system.log"
    );
    assert!(
        !seen_delivery_contract_fail,
        "expected no failed DeliveryContract results in system.log"
    );

    Ok(())
}

#[tokio::test]
async fn sink_edge_emits_passed_delivery_contract_result() -> Result<()> {
    let (sink_handler, delivered_count) = CountingSink::new();

    // Use a unique base path to avoid interference when tests run in parallel.
    let base_path = PathBuf::from(format!(
        "target/delivery_contract_wiring_{}",
        fastrand::u64(..)
    ));
    let journals_base = base_path.clone();

    let handle = flow! {
        name: "delivery_contract_wiring",
        journals: disk_journals(journals_base),
        middleware: [],

        stages: {
            source = source!(TestEventSource::new(10));
            sink = sink!(sink_handler);
        },

        topology: {
            source |> sink;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    let final_count = delivered_count.load(Ordering::Relaxed);
    assert_eq!(
        final_count, 10,
        "expected sink to consume all source events"
    );
    assert_delivery_contract_pass(&base_path).await
}

#[tokio::test]
async fn buffered_sink_edge_emits_passed_delivery_contract_result_after_flush() -> Result<()> {
    let (sink_handler, delivered_count) = BufferedCountingSink::new();

    let base_path = PathBuf::from(format!(
        "target/delivery_contract_wiring_buffered_{}",
        fastrand::u64(..)
    ));
    let journals_base = base_path.clone();

    let handle = flow! {
        name: "delivery_contract_wiring_buffered",
        journals: disk_journals(journals_base),
        middleware: [],

        stages: {
            source = source!(TestEventSource::new(10));
            sink = sink!(sink_handler);
        },

        topology: {
            source |> sink;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    let final_count = delivered_count.load(Ordering::Relaxed);
    assert_eq!(
        final_count, 10,
        "expected buffered sink to consume all source events"
    );

    assert_delivery_contract_pass(&base_path).await
}

#[tokio::test]
async fn fan_out_before_buffered_sink_emits_passed_delivery_contract_result() -> Result<()> {
    let fan_out = 3;
    let source_events = 10;
    let expected_events = (fan_out * source_events) as u64;

    let (sink_handler, delivered_count) = BufferedCountingSink::new();

    let base_path = PathBuf::from(format!(
        "target/delivery_contract_wiring_fanout_buffered_{}",
        fastrand::u64(..)
    ));
    let journals_base = base_path.clone();

    let handle = flow! {
        name: "delivery_contract_wiring_fanout_buffered",
        journals: disk_journals(journals_base),
        middleware: [],

        stages: {
            source = source!(CorrelatedTestEventSource::new(source_events));
            transform = transform!(FanOutTransform::new(fan_out));
            sink = sink!(sink_handler);
        },

        topology: {
            source |> transform;
            transform |> sink;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    let final_count = delivered_count.load(Ordering::Relaxed);
    assert_eq!(
        final_count, expected_events,
        "expected buffered sink to consume all fanned-out events"
    );

    assert_delivery_contract_pass(&base_path).await
}
