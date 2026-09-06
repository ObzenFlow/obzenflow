// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One writer, coalesced observations, and bounded finalisation (FLOWIP-140h B2).
//!
//! An owned child performs the potentially blocking stdout write. Parent-side
//! pipes are asynchronous; shutdown terminates and reaps the child. A timed-out
//! Tokio stdout or spawn_blocking write cannot provide this ownership contract.
use obzenflow_adapters::monitoring::{
    projections::ConsoleProjection, MetricsReadModel, MetricsReadView,
};
use obzenflow_core::event::{MetricsCoordinationEvent, SystemEvent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_runtime::pipeline::FlowHandle;
use std::io;
use std::process::Stdio;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{timeout_at, Instant};

const CLOSE_ALLOWANCE: Duration = Duration::from_secs(1);
const REAP_ALLOWANCE: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Default)]
struct Control {
    deadline: Option<Instant>,
    finish: bool,
    cleanup_complete: bool,
}

pub(super) struct ConsoleOwner {
    control: watch::Sender<Control>,
    task: Option<JoinHandle<()>>,
}

impl ConsoleOwner {
    pub(super) fn start(
        model: Arc<MetricsReadModel>,
        flow: Option<Weak<FlowHandle>>,
        journal: Option<Arc<dyn Journal<SystemEvent>>>,
        collector: Option<AbortHandle>,
    ) -> Self {
        let (control, receiver) = watch::channel(Control::default());
        Self {
            control,
            task: Some(tokio::spawn(run(model, flow, journal, collector, receiver))),
        }
    }

    pub(super) fn shutdown_deadline(&self, deadline: Instant) {
        self.control.send_modify(|value| {
            value.deadline = Some(value.deadline.map_or(deadline, |old| old.min(deadline)));
        });
    }

    pub(super) async fn finish(mut self, deadline: Instant, cleanup_complete: bool) {
        self.control.send_modify(|value| {
            value.deadline = Some(value.deadline.map_or(deadline, |old| old.min(deadline)));
            value.finish = true;
            value.cleanup_complete = cleanup_complete;
        });
        if let Some(task) = self.task.take() {
            // The task owns its absolute deadline, including child termination.
            let _ = task.await;
        }
    }
}

impl Drop for ConsoleOwner {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct ConsoleTransport {
    child: Child,
    input: Option<ChildStdin>,
}

impl ConsoleTransport {
    fn start() -> io::Result<Self> {
        #[cfg(unix)]
        let mut command = Command::new("/bin/cat");
        #[cfg(windows)]
        let mut command = {
            let mut command = Command::new("powershell.exe");
            command.args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::OpenStandardInput().CopyTo([Console]::OpenStandardOutput())",
            ]);
            command
        };
        #[cfg(not(any(unix, windows)))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "console transport unsupported on this platform",
        ));
        #[cfg(any(unix, windows))]
        {
            let mut child = command
                .stdin(Stdio::piped())
                .stdout(Stdio::inherit())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()?;
            let input = child.stdin.take();
            Ok(Self { child, input })
        }
    }

    async fn finish(&mut self, deadline: Instant) -> io::Result<bool> {
        // EOF makes the child flush its complete report before successful exit.
        self.input.take();
        let write_deadline = deadline.checked_sub(REAP_ALLOWANCE).unwrap_or(deadline);
        if let Ok(status) = timeout_at(write_deadline, self.child.wait()).await {
            return status.and_then(|status| {
                if status.success() {
                    Ok(true)
                } else {
                    Err(io::Error::other(format!(
                        "console writer exited with {status}"
                    )))
                }
            });
        }
        self.child.start_kill()?;
        timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "console child reap deadline expired",
                )
            })??;
        Ok(false)
    }

    async fn terminate(&mut self, deadline: Instant) -> io::Result<()> {
        self.input.take();
        self.child.start_kill()?;
        timeout_at(deadline, self.child.wait())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "console child reap deadline expired",
                )
            })??;
        Ok(())
    }
}

fn render(
    view: &MetricsReadView,
    closing: bool,
    drained: bool,
    deadline: Instant,
) -> io::Result<String> {
    let app_time = view
        .app
        .as_ref()
        .map(|s| s.timestamp.to_rfc3339())
        .unwrap_or_else(|| "unavailable".into());
    let infra_time = view
        .infra
        .as_ref()
        .map(|s| s.timestamp.to_rfc3339())
        .unwrap_or_else(|| "unavailable".into());
    let label = if closing {
        "closing summary"
    } else {
        "summary"
    };
    let mut text = format!("ObzenFlow {label} (latest available)\nApplication observed: {app_time}\nInfrastructure observed: {infra_time}\n");
    if closing {
        text.push_str(if drained {
            "Metrics drain confirmation: observed (aggregator publication only)\n"
        } else {
            "Metrics drain confirmation: unconfirmed\n"
        });
    }
    text.push_str(
        &ConsoleProjection::new()
            .render_until(view, Some(deadline.into_std()))
            .map_err(|e| io::Error::other(e.to_string()))?,
    );
    Ok(text)
}

// Read existing coordination evidence once, after cleanup. This creates no
// acknowledgement or new durable event and shares the closing deadline.
async fn drain_observed(
    journal: Option<&Arc<dyn Journal<SystemEvent>>>,
    deadline: Instant,
) -> bool {
    let Some(journal) = journal else {
        return false;
    };
    let scan = async {
        let mut reader = journal.reader_from(0).await.ok()?;
        while Instant::now() < deadline {
            let envelope = reader.next().await.ok()??;
            if matches!(
                envelope.event.event,
                SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
            ) {
                return Some(true);
            }
        }
        None
    };
    timeout_at(deadline, scan)
        .await
        .ok()
        .flatten()
        .unwrap_or(false)
}

async fn run(
    model: Arc<MetricsReadModel>,
    flow: Option<Weak<FlowHandle>>,
    journal: Option<Arc<dyn Journal<SystemEvent>>>,
    collector: Option<AbortHandle>,
    mut control: watch::Receiver<Control>,
) {
    let mut writer = match ConsoleTransport::start() {
        Ok(writer) => writer,
        Err(error) => {
            tracing::warn!(%error, "Console writer startup failed");
            return;
        }
    };
    let mut updates = model.subscribe();
    let mut latest = model.snapshot();
    let mut pending = String::new();
    let mut offset = 0;
    let mut closing: Option<MetricsReadView> = None;
    let mut deadline: Option<Instant> = None;
    let mut final_started = false;
    let mut drained = false;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let command = *control.borrow_and_update();
        if let (Some(current), Some(limit)) = (deadline, command.deadline) {
            deadline = Some(current.min(limit));
        }
        let completed = command.cleanup_complete
            || flow
                .as_ref()
                .is_some_and(|flow| flow.upgrade().is_none_or(|flow| !flow.is_running()));
        if deadline.is_none() && (completed || command.finish) {
            let end = Instant::now() + CLOSE_ALLOWANCE;
            deadline = Some(command.deadline.map_or(end, |limit| limit.min(end)));
            if completed {
                if let Some(collector) = &collector {
                    collector.abort();
                }
                closing = Some(model.snapshot());
                // Evidence is optional; reserve the rest of the same allowance
                // for rendering, delivery and reclaiming the writer.
                let evidence_end =
                    (Instant::now() + Duration::from_millis(50)).min(deadline.unwrap());
                drained = drain_observed(journal.as_ref(), evidence_end).await;
                let view = closing.as_ref().unwrap();
                tracing::debug!(app_observed = ?view.app.as_ref().map(|s| s.timestamp),
                    infra_observed = ?view.infra.as_ref().map(|s| s.timestamp),
                    drain_observed = drained, "Console closing view captured after Runtime cleanup");
            } else {
                let _ = writer.terminate(deadline.unwrap()).await;
                return;
            }
        }
        if let Some(end) = deadline {
            let write_end = end.checked_sub(REAP_ALLOWANCE).unwrap_or(end);
            if Instant::now() >= write_end {
                let result = writer.terminate(end).await;
                tracing::warn!(?result, "Console finalisation deadline expired");
                return;
            }
            if pending.is_empty() {
                if let Some(view) = closing.take() {
                    match render(&view, true, drained, write_end) {
                        Ok(report) => {
                            pending = report;
                            offset = 0;
                            final_started = true;
                        }
                        Err(error) => {
                            let _ = writer.terminate(end).await;
                            tracing::warn!(%error, "Console projection failed");
                            return;
                        }
                    }
                } else if final_started {
                    match writer.finish(end).await {
                        Ok(true) => {
                            tracing::debug!("Console closing summary delivered and writer reaped")
                        }
                        Ok(false) => tracing::warn!("Console finalisation deadline expired"),
                        Err(error) => {
                            tracing::warn!(%error, "Console writer failed during finalisation")
                        }
                    }
                    return;
                }
            }
        }
        tokio::select! {
            // Poll supervisor completion while a pipe write is stalled. An early
            // terminal state alone does not enter finalisation.
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            changed = control.changed() => {
                if changed.is_err() {
                    let _ = writer.terminate(Instant::now() + REAP_ALLOWANCE).await;
                    return;
                }
            },
            changed = updates.changed(), if deadline.is_none() => {
                if let Ok(view) = changed { latest = view; }
            },
            _ = tick.tick(), if deadline.is_none() && pending.is_empty() => {
                match render(&latest, false, false, Instant::now() + Duration::from_millis(10)) {
                    Ok(report) => { pending = report; offset = 0; }
                    Err(error) => tracing::warn!(%error, "Console projection failed"),
                }
            },
            result = async { writer.input.as_mut().unwrap().write(&pending.as_bytes()[offset..]).await }, if !pending.is_empty() => {
                match result {
                    Ok(0) => {
                        let _ = writer.terminate(deadline.unwrap_or_else(|| Instant::now() + REAP_ALLOWANCE)).await;
                        tracing::warn!("Console writer returned zero bytes");
                        return;
                    },
                    Ok(written) => {
                        offset += written;
                        if offset == pending.len() { pending.clear(); offset = 0; }
                    },
                    Err(error) => {
                        let _ = writer.terminate(deadline.unwrap_or_else(|| Instant::now() + REAP_ALLOWANCE)).await;
                        tracing::warn!(%error, "Console write failed; partial report will not be retried");
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use obzenflow_core::{metrics::MetricsSnapshotSink, TypedPayload};
    use obzenflow_dsl::{flow, sink, source};
    use obzenflow_runtime::run_context::FlowBuildContext;
    use obzenflow_runtime::stages::sink::SinkTyped;
    use serde::{Deserialize, Serialize};
    use std::process::Command as ProcessCommand;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Item(u64);
    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "console.shutdown.proof";
    }

    async fn run_while_host_remains_alive() {
        let model = Arc::new(MetricsReadModel::default());
        let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = delivered.clone();
        let definition = obzenflow_dsl::FlowDefinition::materialize(move |_| {
            let input = obzenflow_adapters::sources::finite(vec![Item(1), Item(2), Item(3)]);
            let output = SinkTyped::new(move |item: Item| {
                observed.lock().unwrap().push(item.0);
                async {}
            })
            .idempotent();
            Ok(flow! {
                name: "console_shutdown_proof",
                journals: crate::journal::memory_journals(),
                stages: {
                    input = source!(Item => input);
                    output = sink!(Item => output);
                },
                topology: { input |> output; }
            })
        });
        let handle = Arc::new(
            definition
                .build(FlowBuildContext::for_tests().with_metrics_sink(model.clone()))
                .await
                .unwrap(),
        );
        let owner = ConsoleOwner::start(
            model.clone(),
            Some(Arc::downgrade(&handle)),
            handle.system_journal(),
            None,
        );
        handle.start().await.unwrap();
        while handle.is_running() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(*delivered.lock().unwrap(), vec![1, 2, 3]);
        assert!(handle.current_state().is_terminal());
        // Keep the host's strong handle alive. Console must finish at actual
        // cleanup, without waiting for host shutdown or another finish call.
        tokio::time::timeout(CLOSE_ALLOWANCE + Duration::from_millis(100), async {
            while !owner.task.as_ref().unwrap().is_finished() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("parked host must reclaim console writer after cleanup");
        let mut late = obzenflow_core::metrics::AppMetricsSnapshot::default();
        late.pipeline_state = "late publication must not restart console".into();
        model.publish_app_snapshot(late);
        owner.finish(Instant::now() + CLOSE_ALLOWANCE, true).await;
    }

    // Executed only in an owned subprocess. Explicit runtime destruction before
    // exit proves that cancellation leaves no blocked Tokio worker behind.
    #[test]
    fn output_process_child() {
        let Ok(mode) = std::env::var("OBZENFLOW_CONSOLE_PROOF_CHILD") else {
            return;
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            if mode == "blocked" {
                let mut transport = ConsoleTransport::start().unwrap();
                let report = vec![b'x'; 16 * 1024 * 1024];
                assert!(tokio::time::timeout(
                    Duration::from_millis(50),
                    transport.input.as_mut().unwrap().write_all(&report)
                )
                .await
                .is_err());
                let end = Instant::now() + CLOSE_ALLOWANCE;
                assert!(!transport.finish(end).await.unwrap());
                assert!(
                    transport.child.id().is_none(),
                    "child must be reaped before completion"
                );
                assert!(Instant::now() <= end);
                // Stdout remains full and unread after the filler writer is
                // reaped. Exercise the real owner alongside a running pipeline.
                run_while_host_remains_alive().await;
            } else if mode == "parked" {
                run_while_host_remains_alive().await;
            } else {
                let model = Arc::new(MetricsReadModel::default());
                let owner = ConsoleOwner::start(model, None, None, None);
                owner.finish(Instant::now() + CLOSE_ALLOWANCE, true).await;
            }
        });
        drop(runtime);
        // The test harness must not print into the intentionally full stdout pipe.
        std::process::exit(0);
    }

    fn child(mode: &str) -> std::process::Child {
        ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "monitoring_backend::console::tests::output_process_child",
                "--nocapture",
            ])
            .env("OBZENFLOW_CONSOLE_PROOF_CHILD", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    #[test]
    fn unread_output_pipe_reclaims_writer_and_exits_process() {
        let mut process = child("blocked");
        let end = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = process.try_wait().unwrap() {
                assert!(status.success(), "blocked-pipe child failed: {status}");
                return;
            }
            if std::time::Instant::now() >= end {
                process.kill().unwrap();
                process.wait().unwrap();
                panic!("console writer prevented process shutdown");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn healthy_writer_delivers_one_closing_summary_with_missing_values() {
        let output = child("healthy").wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert_eq!(text.matches("ObzenFlow closing summary").count(), 1);
        assert!(text.contains("Application observed: unavailable"));
        assert!(text.contains("Infrastructure observed: unavailable"));
        assert!(text.contains("Metrics drain confirmation: unconfirmed"));
    }

    #[test]
    fn parked_host_closes_once_after_cleanup_and_ignores_late_publication() {
        let output = child("parked").wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert_eq!(text.matches("ObzenFlow closing summary").count(), 1);
        assert!(text.contains("Metrics drain confirmation: observed"));
        assert!(!text.contains("late publication"));
    }
}
