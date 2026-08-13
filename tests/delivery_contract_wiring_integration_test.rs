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
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::system_event::{ContractResultStatusLabel, SystemEvent};
use obzenflow_core::event::SystemEventType;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_core::{DeliveryContract, StageOutputs, SystemId};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::traits::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, PendingSinkInput, SinkAuditOutcome, SinkBufferedOutcome, SinkCommitReceipt,
    SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    SinkWriterLifecycleReport, TypedFiniteSourceHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};

/// File-local payload for the delivery-contract wiring test. The JSON
/// shape matches what `TestEventSource` / `CorrelatedTestEventSource`
/// emit; the type fingerprints the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeliveryTestEvent {
    index: u64,
}

impl TypedPayload for DeliveryTestEvent {
    const EVENT_TYPE: &'static str = "delivery_contract.test_event";
}

/// The fan-out transform emits a different shape (`{ "fan_out_index": ... }`)
/// so its output is a distinct typed payload from the input.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct FanOutTestEvent {
    fan_out_index: u64,
}

impl TypedPayload for FanOutTestEvent {
    const EVENT_TYPE: &'static str = "delivery_contract.fan_out_event";
}
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Source that generates a fixed number of events.
#[derive(Clone, Debug)]
struct TestEventSource {
    count: usize,
    emitted: usize,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self { count, emitted: 0 }
    }
}

impl TypedFiniteSourceHandler for TestEventSource {
    type Output = DeliveryTestEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted >= self.count {
            return Ok(None);
        }

        let index = self.emitted;
        self.emitted += 1;

        Ok(Some(vec![DeliveryTestEvent {
            index: index as u64,
        }]))
    }
}

/// Source that generates a fixed number of data events and assigns a correlation root per event.
#[derive(Clone, Debug)]
struct CorrelatedTestEventSource {
    count: usize,
    emitted: usize,
}

impl CorrelatedTestEventSource {
    fn new(count: usize) -> Self {
        Self { count, emitted: 0 }
    }
}

impl TypedFiniteSourceHandler for CorrelatedTestEventSource {
    type Output = DeliveryTestEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted >= self.count {
            return Ok(None);
        }

        let index = self.emitted;
        self.emitted += 1;

        Ok(Some(vec![DeliveryTestEvent {
            index: index as u64,
        }]))
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

impl TypedTransformHandler for FanOutTransform {
    type Input = DeliveryTestEvent;
    type Output = StageOutputs<FanOutTestEvent>;

    fn process(
        &self,
        _event: DeliveryTestEvent,
    ) -> std::result::Result<StageOutputs<FanOutTestEvent>, HandlerError> {
        let mut out = Vec::with_capacity(self.fan_out);
        for index in 0..self.fan_out {
            out.push(FanOutTestEvent {
                fan_out_index: index as u64,
            });
        }
        Ok(StageOutputs::many(out))
    }
}

/// Sink that counts data events and always reports success.
#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<AtomicU64>,
}

impl CountingSink {
    fn new(count: Arc<AtomicU64>) -> Self {
        Self { count }
    }
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = DeliveryTestEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: DeliveryTestEvent,
        _context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Count".to_string()),
            None,
        )))
    }
}

/// Sink that buffers data-event acknowledgements until flush.
#[derive(Debug)]
struct BufferedCountingSink<T> {
    count: Arc<AtomicU64>,
    pending: Arc<Mutex<Vec<PendingSinkInput>>>,
    _input: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for BufferedCountingSink<T> {
    fn clone(&self) -> Self {
        Self {
            count: Arc::clone(&self.count),
            pending: Arc::new(Mutex::new(Vec::new())),
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> BufferedCountingSink<T> {
    fn new(count: Arc<AtomicU64>) -> Self {
        Self {
            count,
            pending: Arc::new(Mutex::new(Vec::new())),
            _input: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> InlineSink for BufferedCountingSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: T,
        context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.pending
            .lock()
            .expect("pending receipt buffer poisoned")
            .push(context.defer());
        Ok(SinkWriteReport::buffered(
            SinkBufferedOutcome::accepted_via(
                DeliveryMethod::Custom("BufferedCount".to_string()),
                None,
            ),
        ))
    }

    async fn flush(&mut self) -> std::result::Result<SinkWriterLifecycleReport, HandlerError> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| HandlerError::Other("BufferedCountingSink mutex poisoned".to_string()))?;

        let commit_receipts: Vec<_> = pending
            .drain(..)
            .map(|pending| {
                SinkCommitReceipt::new(
                    pending,
                    SinkTerminalOutcome::success_via(
                        DeliveryMethod::Custom("BufferedCount".to_string()),
                        None,
                    ),
                )
            })
            .collect();

        Ok(
            SinkWriterLifecycleReport::audit(SinkAuditOutcome::success_via(
                DeliveryMethod::Custom("BufferedCount".to_string()),
                None,
            ))
            .with_commit_receipts(commit_receipts),
        )
    }
}

async fn assert_delivery_contract_pass(base_path: &Path) -> Result<()> {
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
                } if contract_name.as_str() == DeliveryContract::NAME => match status {
                    ContractResultStatusLabel::Passed => seen_delivery_contract_pass = true,
                    ContractResultStatusLabel::Failed => seen_delivery_contract_fail = true,
                    ContractResultStatusLabel::Healthy => {}
                    ContractResultStatusLabel::Pending => {}
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
    let delivered_count = Arc::new(AtomicU64::new(0));
    let delivered_count_for_flow = delivered_count.clone();

    // Use a unique base path to avoid interference when tests run in parallel.
    let base_path = PathBuf::from(format!(
        "target/delivery_contract_wiring_{}",
        fastrand::u64(..)
    ));
    let journals_base = base_path.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new(10);
        let sink_handler = CountingSink::new(delivered_count_for_flow);

        Ok(flow! {
            name: "delivery_contract_wiring",
            journals: disk_journals(journals_base),

            stages: {
                source = source!(DeliveryTestEvent => source_handler);
                sink = sink!(DeliveryTestEvent => sink_handler);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
    let delivered_count = Arc::new(AtomicU64::new(0));
    let delivered_count_for_flow = delivered_count.clone();

    let base_path = PathBuf::from(format!(
        "target/delivery_contract_wiring_buffered_{}",
        fastrand::u64(..)
    ));
    let journals_base = base_path.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new(10);
        let sink_handler = BufferedCountingSink::<DeliveryTestEvent>::new(delivered_count_for_flow);

        Ok(flow! {
            name: "delivery_contract_wiring_buffered",
            journals: disk_journals(journals_base),

            stages: {
                source = source!(DeliveryTestEvent => source_handler);
                sink = sink!(DeliveryTestEvent => sink_handler);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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

    let delivered_count = Arc::new(AtomicU64::new(0));
    let delivered_count_for_flow = delivered_count.clone();

    let base_path = PathBuf::from(format!(
        "target/delivery_contract_wiring_fanout_buffered_{}",
        fastrand::u64(..)
    ));
    let journals_base = base_path.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = CorrelatedTestEventSource::new(source_events);
        let transform_handler = FanOutTransform::new(fan_out);
        let sink_handler = BufferedCountingSink::<FanOutTestEvent>::new(delivered_count_for_flow);

        Ok(flow! {
            name: "delivery_contract_wiring_fanout_buffered",
            journals: disk_journals(journals_base),

            stages: {
                source = source!(DeliveryTestEvent => source_handler);
                transform = transform!(DeliveryTestEvent -> FanOutTestEvent => transform_handler);
                sink = sink!(FanOutTestEvent => sink_handler);
            },

            topology: {
                source |> transform;
                transform |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
