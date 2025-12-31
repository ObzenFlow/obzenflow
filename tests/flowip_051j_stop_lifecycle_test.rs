//! FLOWIP-051j regression tests: Stop should deterministically terminate the pipeline.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::{PipelineLifecycleEvent, SystemEvent, SystemEventType};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl_infra::{flow, infinite_source, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, InfiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl SinkHandler for NoopSink {
    async fn consume(&mut self, _event: ChainEvent) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "noop_sink",
            DeliveryMethod::Custom("Noop".to_string()),
            None,
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
impl SinkHandler for SlowSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        tokio::time::sleep(self.sleep).await;
        Ok(DeliveryPayload::success(
            "slow_sink",
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct SlowInfiniteSource {
    writer_id: WriterId,
    counter: u64,
    sleep: Duration,
}

impl SlowInfiniteSource {
    fn new(sleep: Duration) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            counter: 0,
            sleep,
        }
    }
}

impl InfiniteSourceHandler for SlowInfiniteSource {
    fn next(
        &mut self,
    ) -> Result<
        Vec<ChainEvent>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        std::thread::sleep(self.sleep);
        self.counter += 1;
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "tick",
            serde_json::json!({ "n": self.counter }),
        )])
    }
}

#[derive(Clone, Debug)]
struct SlowFiniteSource {
    writer_id: WriterId,
    emitted: usize,
    max: usize,
    sleep: Duration,
}

impl SlowFiniteSource {
    fn new(max: usize, sleep: Duration) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            emitted: 0,
            max,
            sleep,
        }
    }
}

impl FiniteSourceHandler for SlowFiniteSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted >= self.max {
            return Ok(None);
        }

        std::thread::sleep(self.sleep);
        let idx = self.emitted;
        self.emitted += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "tick",
            serde_json::json!({ "n": idx }),
        )]))
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

    let handle = flow! {
        name: "flowip_051j_stop_infinite_source",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            src = infinite_source!("src" => SlowInfiniteSource::new(Duration::from_millis(5)));
            snk = sink!("snk" => NoopSink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Failed to create flow: {:?}", e))?;

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
        None => Err(anyhow!("expected terminal pipeline lifecycle event, found none")),
        _ => Err(anyhow!("unexpected non-terminal pipeline lifecycle event")),
    }
}

#[tokio::test]
async fn stop_finite_source_reports_cancelled() -> Result<()> {
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");

    let handle = flow! {
        name: "flowip_051j_stop_finite_source",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            // Large upper bound so the source is still active when Stop is issued.
            src = source!("src" => SlowFiniteSource::new(10_000, Duration::from_millis(5)));
            snk = sink!("snk" => NoopSink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Failed to create flow: {:?}", e))?;

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
        None => Err(anyhow!("expected terminal pipeline lifecycle event, found none")),
        _ => Err(anyhow!("unexpected non-terminal pipeline lifecycle event")),
    }
}

#[tokio::test]
async fn stop_cancel_timeout_overrides_cancel_reason() -> Result<()> {
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");

    let handle = flow! {
        name: "flowip_051j_stop_cancel_timeout_reason",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            src = infinite_source!("src" => SlowInfiniteSource::new(Duration::from_millis(1)));
            snk = sink!("snk" => SlowSink::new(Duration::from_millis(250)));
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("Failed to create flow: {:?}", e))?;

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
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) if reason == "stop_timeout" => Ok(()),
        Some(PipelineLifecycleEvent::Cancelled { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(stop_timeout), got pipeline_cancelled({reason})"
        )),
        Some(PipelineLifecycleEvent::Completed { .. }) => Err(anyhow!(
            "expected pipeline_cancelled(stop_timeout), got pipeline_completed"
        )),
        Some(PipelineLifecycleEvent::Failed { reason, .. }) => Err(anyhow!(
            "expected pipeline_cancelled(stop_timeout), got pipeline_failed({reason})"
        )),
        None => Err(anyhow!("expected terminal pipeline lifecycle event, found none")),
        _ => Err(anyhow!("unexpected non-terminal pipeline lifecycle event")),
    }
}
