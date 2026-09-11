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
use obzenflow_core::event::{PipelineCancellationCause, PipelineStopAdmission};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::__private::lifecycle;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = LifecycleEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: LifecycleEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
    }
}

#[derive(Clone, Debug)]
struct SlowSink {
    sleep: Duration,
    entered: Arc<tokio::sync::Notify>,
}

impl SlowSink {
    fn new(sleep: Duration, entered: Arc<tokio::sync::Notify>) -> Self {
        Self { sleep, entered }
    }
}

#[async_trait]
impl InlineSink for SlowSink {
    type Input = LifecycleEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: LifecycleEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.entered.notify_one();
        tokio::time::sleep(self.sleep).await;
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
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
async fn graceful_finite_stop_completes_admitted_work_without_exhausting_input() -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone, Debug)]
    struct GatedSink {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        delivered: Arc<AtomicU64>,
    }
    #[async_trait]
    impl InlineSink for GatedSink {
        type Input = LifecycleEvent;
        fn describe(&self) -> SinkDescription {
            SinkDescription::unspecified()
        }
        async fn write(
            &mut self,
            _: LifecycleEvent,
            _: SinkWriteContext,
        ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
            if self.delivered.load(Ordering::SeqCst) == 0 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.delivered.fetch_add(1, Ordering::SeqCst);
            Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
                DeliveryMethod::Custom("GatedSink".into()),
                None,
            )))
        }
    }
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let delivered = Arc::new(AtomicU64::new(0));
    let sink = GatedSink {
        entered: entered.clone(),
        release: release.clone(),
        delivered: delivered.clone(),
    };
    let handle = FlowDefinition::materialize(move |_| {
        let source = SlowFiniteSource::new(10_000, Duration::from_millis(5));
        Ok(flow! {
            name: "graceful_finite_admitted_work", journals: disk_journals(journal_root.clone()),
            stages: { src = source!(LifecycleEvent => source); snk = sink!(LifecycleEvent => sink); },
            topology: { src |> snk; }
        })
    }).build(obzenflow_runtime::run_context::FlowBuildContext::for_tests()).await?;
    let journal = handle.system_journal().unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified()).await?;
    handle.stop_graceful(Duration::from_secs(2)).await?;
    let mut reader = journal.reader().await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(envelope) = reader.next().await? {
                if matches!(
                    envelope.event.event,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted {
                        admission: PipelineStopAdmission::Graceful { .. }
                    })
                ) {
                    return Ok::<(), obzenflow_core::journal::JournalError>(());
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await??;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), lifecycle::wait(&handle)).await??;
    match terminal_lifecycle_event(journal).await? {
        Some(PipelineLifecycleEvent::Completed { metrics, .. }) => {
            assert!(metrics.events_in_total > 0 && metrics.events_in_total < 10_000);
            assert_eq!(metrics.events_in_total, metrics.events_out_total);
            assert_eq!(metrics.events_out_total, delivered.load(Ordering::SeqCst));
            assert_eq!(metrics.errors_total, 0);
        }
        terminal => {
            return Err(anyhow!(
                "expected completed admitted work, got {terminal:?}"
            ))
        }
    }
    Ok(())
}

#[tokio::test]
async fn runtime_timeout_is_admitted_once_despite_duplicate_graceful_requests() -> Result<()> {
    let dir = tempdir()?;
    let journal_root = dir.path().join("journals");
    let entered = Arc::new(tokio::sync::Notify::new());
    let sink_entered = entered.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = SlowInfiniteSource::new(Duration::from_millis(1));
        let sink_handler = SlowSink::new(Duration::from_secs(1), sink_entered);

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

    // Ensure real work is pending before establishing a short graceful deadline.
    tokio::time::timeout(Duration::from_secs(5), entered.notified()).await?;
    handle.stop_graceful(Duration::from_millis(50)).await?;
    handle.stop_graceful(Duration::from_secs(60)).await?;

    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;

    let facts = system_journal.read_all_unordered().await?;
    let cancel_facts = facts
        .iter()
        .filter(|fact| {
            matches!(
                &fact.event.event,
                SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted {
                    admission: PipelineStopAdmission::Cancel {
                        cause: PipelineCancellationCause::GracefulTimeout
                    }
                })
            )
        })
        .count();
    assert_eq!(
        cancel_facts, 1,
        "timeout cancellation must be admitted once"
    );
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
