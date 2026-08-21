// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::source_poll_observer;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::SystemEventType;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{async_source, flow, sink, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    SourceObservationSink, TypedAsyncFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{SourcePollObserver, SourcePollObserverContext};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

/// File-local payload for the async-finite source stage test. The JSON
/// shape matches what `TestAsyncEventSource` emits; the type fingerprints
/// the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AsyncTestEvent {
    index: u64,
}

impl TypedPayload for AsyncTestEvent {
    const EVENT_TYPE: &'static str = "async_finite_source.event";
}
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

#[derive(Clone, Debug)]
struct TestAsyncEventSource {
    emitted: usize,
    drain_calls: Arc<AtomicU64>,
}

impl TestAsyncEventSource {
    fn new(drain_calls: Arc<AtomicU64>) -> Self {
        Self {
            emitted: 0,
            drain_calls,
        }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for TestAsyncEventSource {
    type Output = AsyncTestEvent;

    async fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted < 2 {
            let index = self.emitted;
            self.emitted += 1;
            tokio::task::yield_now().await;
            Ok(Some(vec![AsyncTestEvent {
                index: index as u64,
            }]))
        } else {
            Ok(None)
        }
    }

    async fn drain(&mut self) -> std::result::Result<(), SourceError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl CollectSink {
    fn new(events: Arc<Mutex<Vec<ChainEvent>>>) -> Self {
        Self { events }
    }
}

#[async_trait]
impl InlineSink for CollectSink {
    type Input = AsyncTestEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        event: AsyncTestEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.events
            .lock()
            .unwrap()
            .push(event.to_event(WriterId::from(StageId::new())));
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Collect".to_string()),
            None,
        )))
    }
}

#[derive(Clone, Debug)]
struct CountSourcePollObserver {
    calls: Arc<AtomicU64>,
}

impl SourcePollObserver for CountSourcePollObserver {
    fn after_source_poll(&self, _ctx: &SourcePollObserverContext<'_>, outputs: &[ChainEvent]) {
        self.calls.fetch_add(
            outputs.iter().filter(|event| event.is_data()).count() as u64,
            Ordering::Relaxed,
        );
    }
}

#[tokio::test]
async fn async_finite_source_emits_events_and_calls_drain() -> Result<()> {
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let journal_root = unique_journal_dir("async_finite_source_basic");
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let events_for_flow = Arc::clone(&events);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestAsyncEventSource::new(drain_calls_for_flow);
        let sink = CollectSink::new(events_for_flow);

        Ok(flow! {
            name: "async_finite_source_basic_test",
            journals: disk_journals(journal_root),

            stages: {
                source = async_source!(AsyncTestEvent => source);
                sink = sink!(AsyncTestEvent => sink);
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

    let data_events: Vec<ChainEvent> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.is_data())
        .cloned()
        .collect();
    assert_eq!(
        data_events.len(),
        2,
        "expected two data events to reach the sink"
    );
    assert_eq!(
        drain_calls.load(Ordering::Relaxed),
        1,
        "expected async source drain() to be called once"
    );

    Ok(())
}

#[tokio::test]
async fn async_finite_source_applies_stage_middleware() -> Result<()> {
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let observer_calls = Arc::new(AtomicU64::new(0));
    let journal_root = unique_journal_dir("async_finite_source_middleware");
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let events_for_flow = Arc::clone(&events);
    let observer_calls_for_flow = Arc::clone(&observer_calls);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestAsyncEventSource::new(drain_calls_for_flow);
        let sink = CollectSink::new(events_for_flow);

        Ok(flow! {
            name: "async_finite_source_middleware_test",
            journals: disk_journals(journal_root),

            stages: {
                source = async_source!(AsyncTestEvent => source, observers: [
                    source_poll_observer(
                        "count_source_poll_data",
                        CountSourcePollObserver { calls: observer_calls_for_flow }
                    )
                ]);
                sink = sink!(AsyncTestEvent => sink);
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

    let data_events: Vec<ChainEvent> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.is_data())
        .cloned()
        .collect();

    assert_eq!(
        data_events.len(),
        2,
        "expected two data events to reach the sink"
    );
    assert_eq!(
        observer_calls.load(Ordering::Relaxed),
        data_events.len() as u64,
        "the source-poll observer sees every live data event"
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct CleanupFailureSource {
    drain_calls: Arc<AtomicU64>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for CleanupFailureSource {
    type Output = AsyncTestEvent;

    async fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }

    async fn drain(&mut self) -> std::result::Result<(), SourceError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Err(SourceError::Other("cleanup exploded".to_string()))
    }
}

#[tokio::test]
async fn cleanup_failure_is_durable_and_does_not_block_eof_or_completion() -> Result<()> {
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let journal_root = unique_journal_dir("async_finite_source_cleanup_failure");
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let events_for_flow = Arc::clone(&events);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = CleanupFailureSource {
            drain_calls: drain_calls_for_flow,
        };
        let sink = CollectSink::new(events_for_flow);
        Ok(flow! {
            name: "async_finite_source_cleanup_failure_test",
            journals: disk_journals(journal_root),
            stages: {
                source = async_source!(AsyncTestEvent => source);
                sink = sink!(AsyncTestEvent => sink);
            },
            topology: { source |> sink; }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;
    let system_journal = handle
        .system_journal()
        .expect("cleanup evidence requires the system journal");

    handle.run().await?;

    assert_eq!(drain_calls.load(Ordering::Relaxed), 1);
    assert!(events
        .lock()
        .expect("sink events lock")
        .iter()
        .all(|event| !event.is_data()));
    let cleanup_failures = system_journal
        .read_causally_ordered()
        .await?
        .into_iter()
        .filter_map(|envelope| match envelope.event.event {
            SystemEventType::SourceCleanupFailed {
                stage_name, error, ..
            } => Some((stage_name, error)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(cleanup_failures.len(), 1);
    assert_eq!(cleanup_failures[0].0, "source");
    assert!(cleanup_failures[0].1.contains("cleanup exploded"));
    Ok(())
}

#[derive(Clone, Debug)]
struct FatalObservationSource {
    next_calls: Arc<AtomicU64>,
    drain_calls: Arc<AtomicU64>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for FatalObservationSource {
    type Output = AsyncTestEvent;

    async fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        self.next_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(vec![AsyncTestEvent { index: 1 }]))
    }

    async fn drain(&mut self) -> std::result::Result<(), SourceError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn install_source_observation_sink(&mut self, sink: SourceObservationSink) {
        sink.report_http_pull(Default::default());
    }
}

#[tokio::test]
async fn fatal_poll_path_attempts_cleanup_once_without_authoring_data() -> Result<()> {
    let next_calls = Arc::new(AtomicU64::new(0));
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let journal_root = unique_journal_dir("async_finite_source_fatal_cleanup");
    let next_calls_for_flow = Arc::clone(&next_calls);
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let events_for_flow = Arc::clone(&events);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = FatalObservationSource {
            next_calls: next_calls_for_flow,
            drain_calls: drain_calls_for_flow,
        };
        let sink = CollectSink::new(events_for_flow);
        Ok(flow! {
            name: "async_finite_source_fatal_cleanup_test",
            journals: disk_journals(journal_root),
            stages: {
                source = async_source!(AsyncTestEvent => source);
                sink = sink!(AsyncTestEvent => sink);
            },
            topology: { source |> sink; }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    let _completion = tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .expect("fatal flow terminates without hanging");
    assert_eq!(next_calls.load(Ordering::Relaxed), 0);
    assert_eq!(drain_calls.load(Ordering::Relaxed), 1);
    assert!(events
        .lock()
        .expect("sink events lock")
        .iter()
        .all(|event| !event.is_data()));
    Ok(())
}
