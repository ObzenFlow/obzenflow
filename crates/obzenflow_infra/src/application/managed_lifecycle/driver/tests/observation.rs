// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::application::lifecycle_observation::{Progress, Reader};
use obzenflow_core::event::{
    PipelineCancellationCause, PipelineLifecycleEvent as Lifecycle, PipelineStopAdmission,
    SystemEvent, SystemPayload,
};
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::{JournalRecord, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_runtime::commands::perform;
use obzenflow_runtime::effects::EffectError;
use obzenflow_runtime::queries::FiniteRead;
use obzenflow_runtime::stages::common::handlers::source::SourceRead;
use obzenflow_runtime::stages::common::handlers::TypedAsyncFiniteSourceHandler;
use obzenflow_runtime::systems::Port;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Payload;
impl TypedPayload for Payload {
    const EVENT_TYPE: &'static str = "observation.regression";
}

#[derive(Clone, Debug)]
struct PendingSource {
    started: Arc<Notify>,
    released: Arc<Notify>,
    cancelled: Arc<Notify>,
}

struct PollLifetime(Arc<Notify>);
impl Drop for PollLifetime {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl PendingSource {
    async fn poll(&self) {
        let _lifetime = PollLifetime(self.cancelled.clone());
        self.started.notify_one();
        self.released.notified().await;
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PendingRead {
    emitted: bool,
}

#[obzenflow_runtime::effects::port]
#[async_trait::async_trait]
trait PendingInput {
    #[query(type = "test.observation_pending.read")]
    async fn read(
        &self,
        from: &PendingRead,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Payload, PendingRead>, obzenflow_runtime::queries::QueryError>;
}

#[async_trait::async_trait]
impl PendingInput for PendingSource {
    async fn read(
        &self,
        from: &PendingRead,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Payload, PendingRead>, obzenflow_runtime::queries::QueryError> {
        if from.emitted {
            return Ok(FiniteRead::EndOfInput);
        }
        self.poll().await;
        Ok(FiniteRead::Batch {
            records: vec![Payload],
            next: PendingRead { emitted: true },
        })
    }
}

struct PendingFinite(Port<dyn PendingInput>);

#[async_trait::async_trait]
impl TypedAsyncFiniteSourceHandler for PendingFinite {
    type Output = Payload;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Payload> {
        SourceRead::finite(self.0.clone(), PendingRead { emitted: false }, Ok)
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct Hold;

#[obzenflow_runtime::effects::port]
#[async_trait::async_trait]
trait HoldPort {
    #[effect(type = "test.observation_hold", at_least_once, records = reply)]
    async fn hold(&self, request: &Hold) -> Result<(), EffectError>;
}

#[derive(Clone, Debug)]
struct CountingSink {
    writes: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    released: Arc<Notify>,
}

#[async_trait::async_trait]
impl HoldPort for CountingSink {
    async fn hold(&self, _: &Hold) -> Result<(), EffectError> {
        self.entered.notify_one();
        self.released.notified().await;
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn pending_flow() -> (Arc<FlowHandle>, PendingSource, CountingSink) {
    let source = PendingSource {
        started: Arc::new(Notify::new()),
        released: Arc::new(Notify::new()),
        cancelled: Arc::new(Notify::new()),
    };
    let source_handler = source.clone();
    let sink = CountingSink {
        writes: Arc::new(AtomicUsize::new(0)),
        entered: Arc::new(Notify::new()),
        released: Arc::new(Notify::new()),
    };
    let sink_handler = perform(|_: &Payload| Hold).through(Port::to(sink.clone()));
    let flow = FlowDefinition::materialize(move |_| {
        let source_handler = PendingFinite(Port::to(source_handler));
        Ok(flow! {
            name: "observation_finite", journals: crate::journal::memory_journals(),
            stages: { src = source!(Payload => source_handler); snk = sink!(Payload => sink_handler); },
            topology: { src |> snk; }
        })
    }).build(obzenflow_runtime::run_context::FlowBuildContext::for_tests()).await.unwrap();
    (Arc::new(flow), source, sink)
}

/// Only the application's physical reader is held; Runtime and the oracle keep reading.
struct GatedReader {
    inner: Box<dyn JournalReader<SystemEvent>>,
    before_running: bool,
    gate: Option<oneshot::Receiver<Result<(), JournalError>>>,
    entered: Option<oneshot::Sender<()>>,
    held: Option<JournalRecord<SystemPayload>>,
    saw_ready: bool,
}
#[async_trait::async_trait]
impl obzenflow_core::journal::JournalStorageReader<SystemEvent> for GatedReader {
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<SystemPayload>>, JournalError> {
        let event = match self.held.take() {
            Some(event) => Some(event),
            None => {
                let event = self.inner.next().await?;
                if event.as_ref().is_some_and(|event| {
                    matches!(
                        event.payload,
                        SystemPayload::PipelineLifecycle(Lifecycle::ReadyForRun { .. })
                    )
                }) {
                    self.saw_ready = true;
                }
                if self.before_running
                    && self.gate.is_some()
                    && self.saw_ready
                    && event.as_ref().is_some_and(|event| {
                        matches!(
                            event.payload,
                            SystemPayload::PipelineLifecycle(
                                Lifecycle::Starting | Lifecycle::Running { .. }
                            )
                        )
                    })
                {
                    // Publish the caught-up ReadyForRun projection before blocking the next batch.
                    self.held = event;
                    return Ok(None);
                }
                event
            }
        };
        let hold = !self.before_running
            || self.saw_ready
                && event.as_ref().is_some_and(|event| {
                    matches!(
                        event.payload,
                        SystemPayload::PipelineLifecycle(
                            Lifecycle::Starting | Lifecycle::Running { .. }
                        )
                    )
                });
        if hold {
            if let Some(gate) = self.gate.take() {
                self.entered.take().unwrap().send(()).unwrap();
                gate.await.expect("reader gate retained")?;
            }
        }
        Ok(event)
    }
    fn storage_position(&self) -> u64 {
        self.inner.position()
    }
    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Ok(self.held.is_none() && self.inner.initial_prefix_complete()?)
    }
}

async fn gated_observation(
    driver: &mut ApplicationLifecycle,
    flow: &Arc<FlowHandle>,
    before_running: bool,
) -> (
    oneshot::Sender<Result<(), JournalError>>,
    oneshot::Receiver<()>,
) {
    let (release, gate) = oneshot::channel();
    let (entered, waiting) = oneshot::channel();
    let reader = GatedReader {
        inner: flow.system_journal().unwrap().reader().await.unwrap(),
        before_running,
        gate: Some(gate),
        entered: Some(entered),
        held: None,
        saw_ready: false,
    };
    let (feed, task) = Feed::spawn_reader(Reader::from_reader(
        Box::new(reader),
        flow.pipeline_writer_id(),
    ));
    driver.stop = Some(feed);
    driver.tasks.push(ApplicationTask(task));
    driver.attach_flow(flow.clone());
    (release, waiting)
}

async fn observe_until(
    reader: &mut Reader,
    reached: impl Fn(&crate::application::lifecycle_observation::Projection) -> bool,
) {
    loop {
        reader.catch_up().await;
        if reached(&reader.projection) {
            break;
        }
        // Observe a committed fact, without advancing a paused deadline while
        // merely giving its producer an opportunity to run.
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "warp-server")]
#[tokio::test(start_paused = true)]
async fn lagging_reader_sigterm_preserves_graceful_admission_and_original_deadline() {
    use crate::application::flow_application::ShutdownSignal;
    use obzenflow_core::event::types::DurationMs;

    for before_running in [false, true] {
        for already_admitted in [false, true] {
            println!("lagging reader: before_running={before_running}, already_admitted={already_admitted}");
            let (flow, source, sink) = pending_flow().await;
            flow.start().await.unwrap();
            source.started.notified().await;
            source.released.notify_one();
            sink.entered.notified().await;
            let mut oracle = Reader::new(flow.system_journal().unwrap(), flow.pipeline_writer_id());
            observe_until(&mut oracle, |p| {
                matches!(p.progress, Progress::Running | Progress::Draining)
            })
            .await;
            let grace = Duration::from_secs(1);
            let requested_grace = if already_admitted {
                Duration::from_secs(30)
            } else {
                grace
            };
            let mut driver = ApplicationLifecycle::new(requested_grace, OnTerminalArg::Park);
            let (release, waiting) = gated_observation(&mut driver, &flow, before_running).await;
            waiting.await.unwrap();
            assert_eq!(
                driver.stop.as_ref().unwrap().snapshot().progress,
                if before_running {
                    Progress::ReadyForRun
                } else {
                    Progress::Unknown
                }
            );
            // The observer is now held. Witness admission before advancing the
            // real Runtime deadline; elapsed wall time is not the oracle.
            let original_start = TokioInstant::now();
            if already_admitted {
                flow.stop_graceful(grace).await.unwrap();
                observe_until(&mut oracle, |p| p.admission.is_some()).await;
                tokio::time::advance(Duration::from_millis(300)).await;
            }
            let (signal, receiver) = oneshot::channel();
            signal.send(ShutdownSignal::Sigterm).unwrap();
            let signals = Some(signals::Signals::new(Some(receiver)).unwrap());
            let mut running =
                Box::pin(driver.drive(Event::HostBound(StartupMode::Manual), signals));
            assert!(futures::poll!(&mut running).is_pending());
            observe_until(&mut oracle, |p| p.admission.is_some()).await;
            let graceful = PipelineStopAdmission::Graceful {
                timeout_ms: DurationMs(1_000),
            };
            assert_eq!(oracle.projection.admission, Some(graceful.clone()));
            let before_deadline = original_start + grace - Duration::from_millis(100);
            tokio::time::advance(before_deadline - TokioInstant::now()).await;
            assert!(futures::poll!(&mut running).is_pending());
            oracle.catch_up().await;
            assert_eq!(oracle.projection.admission, Some(graceful.clone()));
            // Cross the original deadline, allowing one timer-resolution tick.
            // A wrongly reset 30-second deadline cannot satisfy this assertion.
            tokio::time::advance(Duration::from_millis(101)).await;
            observe_until(&mut oracle, |p| {
                matches!(p.admission, Some(PipelineStopAdmission::Cancel { .. }))
            })
            .await;
            assert!(
                TokioInstant::now() == original_start + grace + Duration::from_millis(1),
                "the later application request must preserve Runtime's original deadline"
            );
            sink.released.notify_one();
            observe_until(&mut oracle, |p| p.outcome.is_some()).await;
            assert_eq!(oracle.projection.outcome, Some(ObservedOutcome::Cancelled));
            let facts = flow
                .system_journal()
                .unwrap()
                .read_all_unordered()
                .await
                .unwrap();
            let admissions: Vec<_> = facts
                .into_iter()
                .filter_map(|fact| match fact.into_parts().1 {
                    SystemPayload::PipelineLifecycle(Lifecycle::StopAdmitted { admission }) => {
                        Some(admission)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(
                admissions,
                vec![
                    graceful,
                    PipelineStopAdmission::Cancel {
                        cause: PipelineCancellationCause::GracefulTimeout,
                    }
                ]
            );
            source.cancelled.notified().await;
            release.send(Ok(())).unwrap();
            tokio::time::timeout(Duration::from_secs(2), running)
                .await
                .unwrap();
            driver.take_result().unwrap();
        }
    }
}

#[cfg(feature = "warp-server")]
#[tokio::test]
async fn lagging_reader_sigterm_drains_admitted_work() {
    use crate::application::flow_application::ShutdownSignal;
    let (flow, source, sink) = pending_flow().await;
    flow.start().await.unwrap();
    source.started.notified().await;
    source.released.notify_one();
    sink.entered.notified().await;
    let mut driver = ApplicationLifecycle::new(Duration::from_secs(10), OnTerminalArg::Exit);
    let (release, waiting) = gated_observation(&mut driver, &flow, false).await;
    waiting.await.unwrap();
    let (signal, receiver) = oneshot::channel();
    signal.send(ShutdownSignal::Sigterm).unwrap();
    let mut running = Box::pin(driver.drive(
        Event::HostBound(StartupMode::Manual),
        Some(signals::Signals::new(Some(receiver)).unwrap()),
    ));
    assert!(futures::poll!(&mut running).is_pending());
    let mut oracle = Reader::new(flow.system_journal().unwrap(), flow.pipeline_writer_id());
    observe_until(&mut oracle, |p| p.admission.is_some()).await;
    assert!(matches!(
        oracle.projection.admission,
        Some(PipelineStopAdmission::Graceful { .. })
    ));
    sink.released.notify_one();
    release.send(Ok(())).unwrap();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap();
    driver.take_result().unwrap();
    oracle.catch_up().await;
    assert_eq!(oracle.projection.outcome, Some(ObservedOutcome::Completed));
    assert_eq!(sink.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn observation_failure_cancels_pending_runtime_and_retains_source_and_joins() {
    use std::error::Error;
    // Manual terminal parking, automatic startup, standalone, and an admitted graceful stop.
    for phase in [0, 1, 2, 3] {
        let (flow, source, _) = pending_flow().await;
        let mut driver = ApplicationLifecycle::new(Duration::from_secs(10), OnTerminalArg::Park);
        let (release, waiting) = gated_observation(&mut driver, &flow, false).await;
        let (task, mut task_cancelled) = pending_task().await;
        driver.tasks.push(task);
        let (metrics, mut metrics_cancelled) = pending_task().await;
        driver.metrics_collector = Some(metrics);
        let feed = driver.stop.clone().unwrap();
        let initial = match phase {
            0 => {
                flow.start().await.unwrap();
                Event::HostBound(crate::application::config::StartupMode::Manual)
            }
            1 => Event::HostBound(crate::application::config::StartupMode::Auto),
            2 => Event::Standalone,
            _ => {
                flow.start().await.unwrap();
                source.started.notified().await;
                let mut oracle =
                    Reader::new(flow.system_journal().unwrap(), flow.pipeline_writer_id());
                observe_until(&mut oracle, |p| p.progress == Progress::Running).await;
                Event::Stop(StopReason::Graceful, driver.stop_input())
            }
        };
        let mut running = Box::pin(driver.drive(initial, None));
        assert!(futures::poll!(&mut running).is_pending());
        if phase != 3 {
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = &mut running => panic!("application returned before the observation failure"),
                    _ = source.started.notified() => {},
                }
            }).await.expect("Runtime must start while the driver is polled");
        }
        waiting.await.unwrap();
        assert!(futures::poll!(Box::pin(lifecycle::wait(&flow))).is_pending());
        release
            .send(Err(JournalError::Implementation {
                message: "reader failure witness".into(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "original read failure",
                )),
            }))
            .unwrap();
        let original = feed.failed().await;
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .expect("observation failure must wake policy before Runtime completion");
        let error = driver.take_result().unwrap_err();
        let mut source_error: &(dyn Error + 'static) = &error;
        while source_error.downcast_ref::<std::io::Error>().is_none() {
            source_error = source_error
                .source()
                .expect("original source chain retained");
        }
        let ObservationError::Journal(journal_error) = original else {
            panic!("reader error")
        };
        let JournalError::Implementation {
            source: original, ..
        } = journal_error.as_ref()
        else {
            panic!("original journal error")
        };
        assert!(std::ptr::eq(
            source_error.downcast_ref::<std::io::Error>().unwrap(),
            original.downcast_ref::<std::io::Error>().unwrap()
        ));
        assert_eq!(task_cancelled.try_recv(), Ok(()));
        assert_eq!(metrics_cancelled.try_recv(), Ok(()));
        assert!(driver.tasks.is_empty());
        source.cancelled.notified().await;
        lifecycle::wait(&flow).await.unwrap();
        let mut oracle = Reader::new(flow.system_journal().unwrap(), flow.pipeline_writer_id());
        oracle.catch_up().await;
        assert_eq!(
            oracle.projection.admission,
            Some(PipelineStopAdmission::Cancel {
                cause: PipelineCancellationCause::Requested,
            })
        );
        assert_eq!(oracle.projection.outcome, Some(ObservedOutcome::Cancelled));
    }
}
