// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{
    ControlMiddlewareRole, Middleware, MiddlewareContext, MiddlewareFactory, MiddlewareOverrideKey,
    MiddlewarePlanContribution, SourceMiddlewarePhase, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{async_source, flow, sink};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{AsyncFiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;

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
    writer_id: WriterId,
    drain_calls: Arc<AtomicU64>,
}

impl TestAsyncEventSource {
    fn new() -> (Self, Arc<AtomicU64>) {
        let drain_calls = Arc::new(AtomicU64::new(0));
        (
            Self {
                emitted: 0,
                writer_id: WriterId::from(StageId::new()),
                drain_calls: drain_calls.clone(),
            },
            drain_calls,
        )
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for TestAsyncEventSource {
    async fn next(&mut self) -> std::result::Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted < 2 {
            let index = self.emitted;
            self.emitted += 1;
            tokio::task::yield_now().await;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                <AsyncTestEvent as TypedPayload>::EVENT_TYPE,
                json!({ "index": index }),
            )]))
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
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        self.events.lock().unwrap().push(event);
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Collect".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct InjectFieldMiddleware;

impl Middleware for InjectFieldMiddleware {
    fn label(&self) -> &'static str {
        "inject_field"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_write(&self, event: &mut ChainEvent, _ctx: &MiddlewareContext) {
        if let ChainEventContent::Data { payload, .. } = &mut event.content {
            if payload.is_object() {
                payload["mw"] = json!(true);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct InjectFieldFactory;

impl MiddlewareFactory for InjectFieldFactory {
    fn label(&self) -> &'static str {
        "inject_field"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<InjectFieldFactory>("inject_field")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::None
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(InjectFieldMiddleware))
    }
}

#[tokio::test]
async fn async_finite_source_emits_events_and_calls_drain() -> Result<()> {
    let (source, drain_calls) = TestAsyncEventSource::new();
    let (sink, events) = CollectSink::new();

    let journal_root = unique_journal_dir("async_finite_source_basic");

    let handle = flow! {
        name: "async_finite_source_basic_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = async_source!(AsyncTestEvent => source);
            sink = sink!(AsyncTestEvent => sink);
        },

        topology: {
            source |> sink;
        }
    }
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
    let (source, _drain_calls) = TestAsyncEventSource::new();
    let (sink, events) = CollectSink::new();

    let journal_root = unique_journal_dir("async_finite_source_middleware");

    let handle = flow! {
        name: "async_finite_source_middleware_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = async_source!(AsyncTestEvent => source, [
                InjectFieldFactory
            ]);
            sink = sink!(AsyncTestEvent => sink);
        },

        topology: {
            source |> sink;
        }
    }
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
    for event in data_events {
        assert_eq!(
            event.payload().get("mw").and_then(|v| v.as_bool()),
            Some(true),
            "expected middleware to set payload.mw=true"
        );
    }

    Ok(())
}
