// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::source_poll_observer;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{async_source, flow, sink, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{perform, EffectError, Port};
use obzenflow_runtime::stages::common::handlers::TypedAsyncFiniteSourceHandler;
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

#[derive(Clone, Debug, Serialize)]
struct Received;

struct CountReceived {
    received: Arc<AtomicU64>,
}

#[obzenflow_runtime::effects::port(system = "async-finite-counter")]
impl CountReceived {
    #[effect(type = "async_finite_source.receive", at_least_once, records = reply)]
    fn receive(&self, _: &Received) -> Result<(), EffectError> {
        self.received.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
struct CountSourcePollObserver {
    calls: Arc<AtomicU64>,
}

impl SourcePollObserver for CountSourcePollObserver {
    fn after_source_poll(&self, _ctx: &SourcePollObserverContext<'_>, outputs: &[ChainEvent]) {
        self.calls.fetch_add(
            outputs
                .iter()
                .filter(|event| event.consumes_data_credit())
                .count() as u64,
            Ordering::Relaxed,
        );
    }
}

#[tokio::test]
async fn async_finite_source_applies_stage_middleware() -> Result<()> {
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(AtomicU64::new(0));
    let sink = perform(|_: &AsyncTestEvent| Received).through(Port::to(CountReceived {
        received: events.clone(),
    }));
    let observer_calls = Arc::new(AtomicU64::new(0));
    let journal_root = unique_journal_dir("async_finite_source_middleware");
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let observer_calls_for_flow = Arc::clone(&observer_calls);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestAsyncEventSource::new(drain_calls_for_flow);

        Ok(flow! {
            name: "async_finite_source_middleware_test",
            journals: disk_journals(journal_root),

            stages: {
                source = async_source!(AsyncTestEvent with { source_poll_observer(
                        "count_source_poll_data",
                        CountSourcePollObserver { calls: observer_calls_for_flow }
                    ) } => source);
                sink = sink!(AsyncTestEvent => sink);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(crate::replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    let received = events.load(Ordering::Relaxed);
    assert_eq!(received, 2, "expected two data events to reach the sink");
    assert_eq!(
        observer_calls.load(Ordering::Relaxed),
        received,
        "the source-poll observer sees every live data event"
    );

    Ok(())
}
