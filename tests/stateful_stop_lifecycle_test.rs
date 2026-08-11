// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-051j regression tests: Stop should deterministically terminate the pipeline.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{PipelineLifecycleEvent, SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, infinite_source, sink, source, FlowDefinition};
use serde::{Deserialize, Serialize};

/// File-local payload for the stop-lifecycle test. The JSON shape matches
/// what `SlowInfiniteSource` / `SlowFiniteSource` emit; the type
/// fingerprints the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LifecycleEvent {
    n: u64,
}

impl TypedPayload for LifecycleEvent {
    const EVENT_TYPE: &'static str = "stateful.lifecycle_event";
}
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler, TypedSinkConsumeReport, TypedSinkHandler,
};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl TypedSinkHandler for NoopSink {
    type Input = LifecycleEvent;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::undeclared()
    }

    async fn consume(
        &mut self,
        _event: LifecycleEvent,
        _context: SinkInputContext,
    ) -> std::result::Result<TypedSinkConsumeReport, HandlerError> {
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(DeliveryMethod::Custom("Noop".to_string()), None),
        ))
    }
}

#[derive(Clone, Debug)]
struct SlowSink {
    sleep: Duration,
}

impl SlowSink {
    fn new(sleep: Duration) -> Self {
        Self { sleep }
    }
}

#[async_trait]
impl TypedSinkHandler for SlowSink {
    type Input = LifecycleEvent;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::undeclared()
    }

    async fn consume(
        &mut self,
        _event: LifecycleEvent,
        _context: SinkInputContext,
    ) -> std::result::Result<TypedSinkConsumeReport, HandlerError> {
        tokio::time::sleep(self.sleep).await;
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(DeliveryMethod::Custom("Noop".to_string()), None),
        ))
    }
}

#[derive(Clone, Debug)]
struct SlowInfiniteSource {
    counter: u64,
    sleep: Duration,
}

impl SlowInfiniteSource {
    fn new(sleep: Duration) -> Self {
        Self { counter: 0, sleep }
    }
}

impl TypedInfiniteSourceHandler for SlowInfiniteSource {
    type Output = LifecycleEvent;

    fn next(
        &mut self,
    ) -> Result<
        Vec<Self::Output>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        std::thread::sleep(self.sleep);
        self.counter += 1;
        Ok(vec![LifecycleEvent { n: self.counter }])
    }
}

#[derive(Clone, Debug)]
struct SlowFiniteSource {
    emitted: usize,
    max: usize,
    sleep: Duration,
}

impl SlowFiniteSource {
    fn new(max: usize, sleep: Duration) -> Self {
        Self {
            emitted: 0,
            max,
            sleep,
        }
    }
}

impl TypedFiniteSourceHandler for SlowFiniteSource {
    type Output = LifecycleEvent;

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted >= self.max {
            return Ok(None);
        }

        std::thread::sleep(self.sleep);
        let idx = self.emitted;
        self.emitted += 1;

        Ok(Some(vec![LifecycleEvent { n: idx as u64 }]))
    }
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))?
}

async fn wait_for_draining(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Draining) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Draining"))?
}

async fn terminal_lifecycle_event(
    journal: Arc<dyn Journal<SystemEvent>>,
) -> Result<Option<PipelineLifecycleEvent>> {
    // Read a small tail: most recent first.
    let tail = journal
        .read_last_n(64)
        .await
        .map_err(|e| anyhow!("failed to read system journal tail: {e}"))?;

    for envelope in tail {
        if let SystemEventType::PipelineLifecycle(ev) = &envelope.event.event {
            if matches!(
                ev,
                PipelineLifecycleEvent::Completed { .. }
                    | PipelineLifecycleEvent::Failed { .. }
                    | PipelineLifecycleEvent::Cancelled { .. }
            ) {
                return Ok(Some(ev.clone()));
            }
        }
    }

    Ok(None)
}

#[tokio::test]
async fn stop_infinite_source_reports_cancelled() -> Result<()> {
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = SlowInfiniteSource::new(Duration::from_millis(5));
        let sink_handler = NoopSink;

        Ok(flow! {
            name: "stateful_stop_infinite_source",
            journals: disk_journals(journal_root.clone()),

            stages: {
                src = infinite_source!(LifecycleEvent => source_handler);
                snk = sink!(LifecycleEvent => sink_handler);
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("Failed to create flow: {e:?}"))?;

    let system_journal = handle
        .system_journal()
        .ok_or_else(|| anyhow!("flow handle did not expose system journal"))?;

    wait_for_running(&handle).await?;

    handle.stop().await?;

    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;

    let terminal = terminal_lifecycle_event(system_journal).await?;
    match terminal {
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) if reason == "user_stop" => Ok(()),
        Some(PipelineLifecycleEvent::Completed { .. }) => Err(anyhow!(
            "expected pipeline_cancelled(user_stop), got pipeline_completed"
        )),
        Some(PipelineLifecycleEvent::Failed { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(user_stop), got pipeline_failed reason={reason}"
        )),
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(user_stop), got pipeline_cancelled({reason})"
        )),
        None => Err(anyhow!(
            "expected terminal pipeline lifecycle event, found none"
        )),
        _ => Err(anyhow!("unexpected non-terminal pipeline lifecycle event")),
    }
}

#[tokio::test]
async fn stop_finite_source_reports_cancelled() -> Result<()> {
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = SlowFiniteSource::new(10_000, Duration::from_millis(5));
        let sink_handler = NoopSink;

        Ok(flow! {
            name: "stateful_stop_finite_source",
            journals: disk_journals(journal_root.clone()),

            stages: {
                // Large upper bound so the source is still active when Stop is issued.
                src = source!(LifecycleEvent => source_handler);
                snk = sink!(LifecycleEvent => sink_handler);
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("Failed to create flow: {e:?}"))?;

    let system_journal = handle
        .system_journal()
        .ok_or_else(|| anyhow!("flow handle did not expose system journal"))?;

    wait_for_running(&handle).await?;

    handle.stop().await?;

    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;

    let terminal = terminal_lifecycle_event(system_journal).await?;
    match terminal {
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) if reason == "user_stop" => Ok(()),
        Some(PipelineLifecycleEvent::Completed { .. }) => Err(anyhow!(
            "expected pipeline_cancelled(user_stop), got pipeline_completed"
        )),
        Some(PipelineLifecycleEvent::Failed { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(user_stop), got pipeline_failed({reason})"
        )),
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(user_stop), got pipeline_cancelled({reason})"
        )),
        None => Err(anyhow!(
            "expected terminal pipeline lifecycle event, found none"
        )),
        _ => Err(anyhow!("unexpected non-terminal pipeline lifecycle event")),
    }
}

#[tokio::test]
async fn stop_cancel_timeout_overrides_cancel_reason() -> Result<()> {
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = SlowInfiniteSource::new(Duration::from_millis(1));
        let sink_handler = SlowSink::new(Duration::from_millis(250));

        Ok(flow! {
            name: "stateful_stop_cancel_timeout_reason",
            journals: disk_journals(journal_root.clone()),

            stages: {
                src = infinite_source!(LifecycleEvent => source_handler);
                snk = sink!(LifecycleEvent => sink_handler);
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("Failed to create flow: {e:?}"))?;

    let system_journal = handle
        .system_journal()
        .ok_or_else(|| anyhow!("flow handle did not expose system journal"))?;

    wait_for_running(&handle).await?;

    // First request a graceful stop so the pipeline records stop intent as user_stop.
    handle.stop_graceful(Duration::from_secs(60)).await?;
    wait_for_draining(&handle).await?;

    // Then simulate a process-level timeout escalation and ensure the terminal lifecycle reason
    // reflects stop_timeout (not user_stop).
    handle.stop_cancel_timeout().await?;

    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;

    let terminal = terminal_lifecycle_event(system_journal).await?;
    match terminal {
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) if reason == "stop_timeout" => {
            Ok(())
        }
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(stop_timeout), got pipeline_cancelled({reason})"
        )),
        Some(PipelineLifecycleEvent::Completed { .. }) => Err(anyhow!(
            "expected pipeline_cancelled(stop_timeout), got pipeline_completed"
        )),
        Some(PipelineLifecycleEvent::Failed { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(stop_timeout), got pipeline_failed({reason})"
        )),
        None => Err(anyhow!(
            "expected terminal pipeline lifecycle event, found none"
        )),
        _ => Err(anyhow!("unexpected non-terminal pipeline lifecycle event")),
    }
}
