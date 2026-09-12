// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_runtime::__private::lifecycle;

async fn abort_execution_for_test(flow: &FlowHandle) {
    drop(lifecycle::guard_execution(flow));
    let error = lifecycle::wait(flow)
        .await
        .expect_err("emergency cancellation retains the aborted result");
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains("aborted"));
}

#[derive(Clone, Copy, Debug)]
enum FaultPhase {
    BeforeRun,
    Manual,
    Running,
    Draining,
    Cancelling,
    Terminal,
}

#[derive(Clone, Copy, Debug)]
enum HostFault {
    Complete,
    Error,
    Panic,
}

#[derive(Clone, Debug)]
struct PendingLifetimeSource {
    started: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    cancelled: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

struct PendingPollDrop(Option<oneshot::Sender<()>>);

impl Drop for PendingPollDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[async_trait::async_trait]
impl obzenflow_runtime::stages::common::handlers::TypedAsyncInfiniteSourceHandler
    for PendingLifetimeSource
{
    type Output = IdlePayload;

    async fn next(&mut self) -> Result<Vec<IdlePayload>, SourceError> {
        let _drop = PendingPollDrop(self.cancelled.lock().unwrap().take());
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        std::future::pending().await
    }
}

fn pending_lifetime_source() -> (
    PendingLifetimeSource,
    oneshot::Receiver<()>,
    oneshot::Receiver<()>,
) {
    let (started_tx, started_rx) = oneshot::channel();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    (
        PendingLifetimeSource {
            started: Arc::new(Mutex::new(Some(started_tx))),
            cancelled: Arc::new(Mutex::new(Some(cancelled_tx))),
        },
        started_rx,
        cancelled_rx,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_application_cancels_runtime_and_pending_stage_work() {
    use obzenflow_core::event::JournalEvent;
    use obzenflow_dsl::async_infinite_source;

    for hosted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(&config, format!(
            "[server]\nenabled = {hosted}\nhost = \"127.0.0.1\"\nport = {port}\nstartup_mode = \"auto\"\n[metrics]\nenabled = false\n"
        )).unwrap();
        let (left, left_started, left_cancelled) = pending_lifetime_source();
        let (right, right_started, right_cancelled) = pending_lifetime_source();
        let (flow_tx, flow_rx) = oneshot::channel();
        let flow_tx = Mutex::new(Some(flow_tx));
        let application = tokio::spawn(FlowApplication::launch(
            FlowDefinition::materialize(move |_| {
                let sink = NoopSink;
                Ok(flow! {
                    name: "application_drop", journals: crate::journal::memory_journals(),
                    stages: {
                        left = async_infinite_source!(IdlePayload => left);
                        right = async_infinite_source!(IdlePayload => right);
                        sink = sink!(IdlePayload => sink);
                    },
                    topology: { left |> sink; right |> sink; }
                })
            }),
            LaunchParams {
                cli_args: Some(vec![
                    "regression".into(),
                    "--config".into(),
                    config.into_os_string(),
                ]),
                flow_handle_hooks: vec![Box::new(move |flow| {
                    // Standalone execution must retain unique ownership of its handle.
                    let observation = (
                        hosted.then(|| flow.clone()),
                        flow.state_receiver(),
                        flow.system_journal().unwrap(),
                    );
                    assert!(flow_tx
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(observation)
                        .is_ok());
                    Ok(tokio::spawn(async {}))
                })],
                ..LaunchParams::default()
            },
        ));
        let (retained_flow, mut states, journal) = flow_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            left_started.await.unwrap();
            right_started.await.unwrap();
        })
        .await
        .expect("both stage operations must be pending");
        application.abort();
        assert!(application.await.unwrap_err().is_cancelled());
        let cancelled = tokio::time::timeout(Duration::from_secs(2), async {
            if let Some(flow) = &retained_flow {
                let error = lifecycle::wait(flow).await.unwrap_err();
                assert!(std::error::Error::source(&error)
                    .unwrap()
                    .to_string()
                    .contains("aborted"));
            } else {
                // No FlowHandle survives standalone execution, so closure also witnesses
                // release of the pipeline task's state publisher on the live caller runtime.
                while states.changed().await.is_ok() {}
            }
            left_cancelled.await.unwrap();
            right_cancelled.await.unwrap();
        })
        .await;
        if let Some(flow) = &retained_flow {
            // Clean up even if the cancellation assertion regresses.
            abort_execution_for_test(flow).await;
        }
        cancelled.expect("application drop must cancel the supervisor and both stage operations");
        let facts = journal.read_all_unordered().await.unwrap();
        assert!(
            !facts.iter().any(|fact| matches!(
                fact.event.event_type_name(),
                "system.pipeline.completed" | "system.pipeline.cancelled"
            )),
            "emergency cancellation must not invent a published outcome"
        );
        if hosted {
            let listener = TcpListener::bind(("127.0.0.1", port)).expect("host listener released");
            drop(listener);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_application_during_host_preparation_cancels_the_built_flow() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("obzenflow.toml");
    let port = available_local_port();
    std::fs::write(&config, format!(
        "[server]\nenabled = true\nhost = \"127.0.0.1\"\nport = {port}\nstartup_mode = \"auto\"\n[metrics]\nenabled = false\n"
    )).unwrap();
    let (flow_tx, flow_rx) = oneshot::channel();
    let flow_tx = Mutex::new(Some(flow_tx));
    let (preparing_tx, preparing_rx) = oneshot::channel();
    let application = tokio::spawn(FlowApplication::launch(
        FlowDefinition::materialize(move |_| {
            let source = IdleInfiniteSource;
            let sink = NoopSink;
            Ok(flow! {
                name: "application_drop_preparation", journals: crate::journal::memory_journals(),
                stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                topology: { src |> sink; }
            })
        }),
        LaunchParams {
            cli_args: Some(vec![
                "regression".into(),
                "--config".into(),
                config.into_os_string(),
            ]),
            flow_handle_hooks: vec![Box::new(move |flow| {
                assert!(flow_tx
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(flow.clone())
                    .is_ok());
                Ok(tokio::spawn(async {}))
            })],
            test_host_task: Some((
                Box::pin(async move {
                    preparing_tx.send(()).unwrap();
                    std::future::pending().await
                }),
                true,
            )),
            ..LaunchParams::default()
        },
    ));
    let flow = flow_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), preparing_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(flow.is_running());
    application.abort();
    assert!(application.await.unwrap_err().is_cancelled());
    let completed = tokio::time::timeout(Duration::from_secs(2), lifecycle::wait(&flow)).await;
    abort_execution_for_test(&flow).await;
    assert!(completed
        .expect("built flow must be cancelled during host preparation")
        .is_err());
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("host listener released");
    drop(listener);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hosted_start_observes_runtime_exit_before_readiness() {
    for terminal_mode in ["exit", "park"] {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(&config, format!(
            "[server]\nenabled = true\nhost = \"127.0.0.1\"\nport = {port}\nstartup_mode = \"auto\"\non_terminal = \"{terminal_mode}\"\n[metrics]\nenabled = false\n"
        )).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), FlowApplication::launch(
            FlowDefinition::new(move |context| async move {
                let source = IdleInfiniteSource;
                let sink = NoopSink;
                let flow = flow! {
                    name: "startup_runtime_exit", journals: crate::journal::memory_journals(),
                    stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                    topology: { src |> sink; }
                }.build(context).await?;
                abort_execution_for_test(&flow).await;
                assert!(!flow.is_running());
                assert!(!flow.current_state().is_terminal());
                Ok(flow)
            }),
            LaunchParams {
                cli_args: Some(vec!["regression".into(), "--config".into(), config.into_os_string()]),
                ..LaunchParams::default()
            },
        )).await.expect("hosted startup must observe Runtime termination before readiness");
        assert!(
            matches!(result, Err(ApplicationError::FlowExecutionFailed(ref message))
            if message.contains("Supervisor task was aborted")),
            "joined Runtime error: {result:?}"
        );
        let listener =
            TcpListener::bind(("127.0.0.1", port)).expect("host must be joined before return");
        drop(listener);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_startup_signals_withhold_automatic_run() {
    use obzenflow_core::event::JournalEvent;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone, Debug)]
    struct CountingSource(Arc<AtomicUsize>);
    impl TypedInfiniteSourceHandler for CountingSource {
        type Output = IdlePayload;
        fn next(&mut self) -> Result<Vec<IdlePayload>, SourceError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }
    for signal in [ShutdownSignal::Sigint, ShutdownSignal::Sigterm] {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("obzenflow.toml");
        std::fs::write(
            &config,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {}
startup_mode = "auto"
on_terminal = "exit"
[metrics]
enabled = false
"#,
                available_local_port()
            ),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let source_calls = calls.clone();
        let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
        let hook_observed = observed.clone();
        let definition = FlowDefinition::materialize(move |_| {
            let source = CountingSource(source_calls);
            let sink = NoopSink;
            Ok(flow! {
                name: "startup_signal", journals: crate::journal::memory_journals(),
                stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                topology: { src |> sink; }
            })
        });
        let (signal_tx, signal_rx) = oneshot::channel();
        signal_tx.send(signal).unwrap();
        FlowApplication::launch(
            definition,
            LaunchParams {
                cli_args: Some(vec![
                    "regression".into(),
                    "--config".into(),
                    config.into_os_string(),
                ]),
                flow_handle_hooks: vec![Box::new(move |flow| {
                    *hook_observed.lock().unwrap() = Some(flow.clone());
                    Ok(tokio::spawn(async {}))
                })],
                test_shutdown_signal: Some(signal_rx),
                ..LaunchParams::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "{signal:?} must withhold automatic Run"
        );
        let flow = observed.lock().unwrap().take().unwrap();
        let facts = flow
            .system_journal()
            .unwrap()
            .read_all_unordered()
            .await
            .unwrap();
        let terminals: Vec<_> = facts
            .iter()
            .map(|event| event.event.event_type_name())
            .filter(|kind| {
                matches!(
                    *kind,
                    "system.pipeline.cancelled"
                        | "system.pipeline.completed"
                        | "system.pipeline.failed"
                        | "system.pipeline.not_started"
                )
            })
            .collect();
        assert_eq!(terminals, ["system.pipeline.not_started"]);
    }
}

#[tokio::test(start_paused = true)]
async fn startup_failure_retains_prior_hook_joins_and_the_not_started_journal_outcome() {
    use obzenflow_core::event::JournalEvent;

    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("obzenflow.toml");
    std::fs::write(
        &config,
        r#"
[server]
enabled = true
startup_mode = "manual"
[runtime]
shutdown_timeout_secs = 1
[metrics]
enabled = false
"#,
    )
    .unwrap();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (started_tx, started_rx) = oneshot::channel();
    let (terminated_tx, mut terminated_rx) = oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        started_tx.send(()).unwrap();
        let _ = release_rx.recv();
        terminated_tx.send(()).unwrap();
    });
    started_rx.await.unwrap();
    let task = Mutex::new(Some(task));
    let (flow_tx, flow_rx) = oneshot::channel();
    let flow_tx = Mutex::new(Some(flow_tx));
    let definition = FlowDefinition::materialize(move |_| {
        let source = IdleInfiniteSource;
        let sink = NoopSink;
        Ok(flow! {
            name: "startup_cleanup_join", journals: crate::journal::memory_journals(),
            stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
            topology: { src |> sink; }
        })
    });
    let application = tokio::spawn(FlowApplication::launch(
        definition,
        LaunchParams {
            cli_args: Some(vec![
                "regression".into(),
                "--config".into(),
                config.into_os_string(),
            ]),
            flow_handle_hooks: vec![
                Box::new(move |flow| {
                    assert!(flow_tx
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(flow.clone())
                        .is_ok());
                    Ok(task.lock().unwrap().take().unwrap())
                }),
                Box::new(|_| {
                    Err(ApplicationError::IoError(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "startup failure witness",
                    )))
                }),
            ],
            ..LaunchParams::default()
        },
    ));
    let flow = flow_rx.await.unwrap();
    // A live spawn_blocking task suppresses Tokio's automatic clock advance.
    // Advance explicitly while Runtime settles its pre-execution cancellation.
    for _ in 0..200 {
        if !flow.is_running() {
            break;
        }
        tokio::time::advance(Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
    }
    assert!(!flow.is_running(), "pre-execution cancellation must settle");
    lifecycle::wait(&flow).await.unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(
        !application.is_finished(),
        "an expired auxiliary budget cannot release the hook"
    );
    let facts = flow
        .system_journal()
        .unwrap()
        .read_all_unordered()
        .await
        .unwrap();
    let terminals: Vec<_> = facts
        .iter()
        .map(|event| event.event.event_type_name())
        .filter(|kind| {
            matches!(
                *kind,
                "system.pipeline.cancelled"
                    | "system.pipeline.completed"
                    | "system.pipeline.failed"
                    | "system.pipeline.not_started"
            )
        })
        .collect();
    assert_eq!(terminals, ["system.pipeline.not_started"]);
    release_tx.send(()).unwrap();
    let result = application.await.unwrap();
    assert!(matches!(result, Err(ApplicationError::IoError(error))
        if error.kind() == std::io::ErrorKind::InvalidInput && error.to_string() == "startup failure witness"));
    assert_eq!(terminated_rx.try_recv(), Ok(()));
    assert!(!flow.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_completion_and_panic_remain_primary_across_application_phases() {
    use crate::application::lifecycle_observation::Reader;
    use crate::web::host_error::ManagedWebHostError;
    use futures::FutureExt;
    use obzenflow_core::event::{PipelineLifecycleEvent, PipelineStopAdmission, SystemEventType};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug)]
    struct TestSource {
        terminal: bool,
        calls: Arc<AtomicUsize>,
    }
    impl TypedFiniteSourceHandler for TestSource {
        type Output = IdlePayload;
        fn next(&mut self) -> Result<Option<Vec<IdlePayload>>, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(if self.terminal {
                None
            } else {
                Some(Vec::new())
            })
        }
    }

    for phase in [
        FaultPhase::BeforeRun,
        FaultPhase::Manual,
        FaultPhase::Running,
        FaultPhase::Draining,
        FaultPhase::Cancelling,
        FaultPhase::Terminal,
    ] {
        for fault in [HostFault::Complete, HostFault::Error, HostFault::Panic] {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("obzenflow.toml");
            let startup = if matches!(phase, FaultPhase::Manual) {
                "manual"
            } else {
                "auto"
            };
            std::fs::write(
                &config,
                format!(
                    r#"
[server]
enabled = true
host = "127.0.0.1"
port = {}
startup_mode = "{startup}"
on_terminal = "park"
[runtime]
shutdown_timeout_secs = 2
[metrics]
enabled = false
"#,
                    available_local_port()
                ),
            )
            .unwrap();
            let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
            let hook_observed = observed.clone();
            let task_observed = observed.clone();
            let observed_admission = Arc::new(Mutex::new(None));
            let task_admission = observed_admission.clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let source_calls = calls.clone();
            let (signal, received_signal) = oneshot::channel();
            let signal = Arc::new(Mutex::new(Some(signal)));
            let task_signal = signal.clone();
            let future = async move {
                let flow = task_observed.lock().unwrap().clone().unwrap();
                let mut states = flow.state_receiver();
                if !matches!(phase, FaultPhase::BeforeRun) {
                    loop {
                        let state = states.borrow_and_update().clone();
                        let ready = match phase {
                            FaultPhase::Manual => matches!(state, PipelineState::ReadyForRun),
                            FaultPhase::Terminal => state.is_terminal(),
                            _ => matches!(state, PipelineState::Running),
                        };
                        if ready {
                            break;
                        }
                        states.changed().await.unwrap();
                    }
                }
                if matches!(phase, FaultPhase::Draining | FaultPhase::Cancelling) {
                    let mut status = Reader::new(
                        flow.system_journal().unwrap().clone(),
                        flow.pipeline_writer_id(),
                    );
                    if matches!(phase, FaultPhase::Draining) {
                        flow.stop_graceful(Duration::from_secs(60)).await.unwrap();
                    } else {
                        flow.stop_cancel().await.unwrap();
                    }
                    loop {
                        status.catch_up().await;
                        let admitted = status.projection.admission.clone();
                        if matches!(admitted, Some(PipelineStopAdmission::Graceful { .. })) {
                            *task_admission.lock().unwrap() = admitted;
                            break;
                        }
                        if matches!(admitted, Some(PipelineStopAdmission::Cancel { .. })) {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }
                // A terminal-success observation and shutdown signal cannot hide
                // an independently failed host task.
                if matches!(phase, FaultPhase::Terminal) {
                    let mut reader =
                        Reader::new(flow.system_journal().unwrap(), flow.pipeline_writer_id());
                    loop {
                        reader.catch_up().await;
                        if reader.projection.outcome.is_some() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    let _ = task_signal
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(ShutdownSignal::Sigterm);
                }
                match fault {
                    HostFault::Complete => Ok(()),
                    HostFault::Error => Err(ManagedWebHostError::Accept(std::io::Error::other(
                        "injected listener failure",
                    ))),
                    HostFault::Panic => panic!("injected managed host panic"),
                }
            }
            .boxed();
            let definition = FlowDefinition::materialize(move |_| {
                let source = TestSource {
                    terminal: matches!(phase, FaultPhase::Terminal),
                    calls: source_calls,
                };
                let sink = NoopSink;
                Ok(flow! {
                    name: "managed_host_fault",
                    journals: crate::journal::memory_journals(),
                    stages: { src = source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                    topology: { src |> sink; }
                })
            });
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                FlowApplication::launch(
                    definition,
                    LaunchParams {
                        cli_args: Some(vec![
                            "obzenflow".into(),
                            "--config".into(),
                            config.into_os_string(),
                        ]),
                        flow_handle_hooks: vec![Box::new(move |flow| {
                            *hook_observed.lock().unwrap() = Some(flow.clone());
                            Ok(tokio::spawn(async {}))
                        })],
                        test_host_task: Some((future, matches!(phase, FaultPhase::BeforeRun))),
                        test_shutdown_signal: Some(received_signal),
                        ..LaunchParams::default()
                    },
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("host failure cleanup timed out in {phase:?}/{fault:?}"));
            let Err(ApplicationError::Other(error)) = result else {
                panic!("expected primary host error in {phase:?}/{fault:?}: {result:?}")
            };
            let error = error
                .downcast_ref::<ManagedWebHostError>()
                .expect("host failure retains its type");
            assert!(
                matches!(
                    (fault, error),
                    (
                        HostFault::Complete,
                        ManagedWebHostError::PrematureCompletion
                    ) | (HostFault::Error, ManagedWebHostError::Accept(_))
                        | (HostFault::Panic, ManagedWebHostError::Task(_))
                ),
                "wrong host failure: {phase:?}/{fault:?}: {error:?}"
            );
            let flow = observed.lock().unwrap().take().unwrap();
            assert!(!flow.is_running());
            if matches!(phase, FaultPhase::BeforeRun | FaultPhase::Manual) {
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    0,
                    "sources must not run after pre-run host failure"
                );
            }
            let events = flow
                .system_journal()
                .unwrap()
                .read_all_unordered()
                .await
                .unwrap();
            if let Some(admission) = observed_admission.lock().unwrap().as_ref() {
                let graceful: Vec<_> = events
                    .iter()
                    .filter_map(|envelope| match &envelope.event.event {
                        SystemEventType::PipelineLifecycle(
                            PipelineLifecycleEvent::StopAdmitted {
                                admission: value @ PipelineStopAdmission::Graceful { .. },
                            },
                        ) => Some(value),
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    graceful,
                    [admission],
                    "host cleanup must not replace an admitted graceful request"
                );
            }
            if matches!(phase, FaultPhase::Terminal) {
                assert!(
                    events.iter().any(|event| matches!(
                        event.event.event,
                        SystemEventType::PipelineLifecycle(
                            PipelineLifecycleEvent::Completed { .. }
                        )
                    )),
                    "host cleanup must join terminal publication"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_async_closes_pending_responses_before_returning_to_a_live_runtime() {
    use obzenflow_core::web::{
        EndpointError, HttpMethod, ManagedResponse, Request, Response, SseBody, SseFrame,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::Notify;

    struct Witness(Arc<AtomicUsize>);
    impl Drop for Witness {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct PendingResponse {
        sse: bool,
        entered: Arc<Notify>,
        release: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl HttpEndpoint for PendingResponse {
        fn path(&self) -> &str {
            if self.sse {
                "/pending-sse"
            } else {
                "/pending-unary"
            }
        }
        fn methods(&self) -> &[HttpMethod] {
            &[HttpMethod::Get]
        }
        async fn handle(&self, _: Request) -> Result<ManagedResponse, EndpointError> {
            let witness = Witness(self.dropped.clone());
            if self.sse {
                let entered = self.entered.clone();
                let release = self.release.clone();
                Ok(ManagedResponse::Sse(SseBody::new(futures::stream::once(
                    async move {
                        let _witness = witness;
                        entered.notify_one();
                        release.notified().await;
                        SseFrame::data("finished")
                    },
                ))))
            } else {
                let _witness = witness;
                self.entered.notify_one();
                self.release.notified().await;
                Ok(Response::ok().with_text("finished").into())
            }
        }
    }

    for force_close in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(
            &config,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {port}
startup_mode = "manual"
on_terminal = "exit"
[runtime]
shutdown_timeout_secs = 2
[metrics]
enabled = false
"#
            ),
        )
        .unwrap();
        let unary_entered = Arc::new(Notify::new());
        let sse_entered = Arc::new(Notify::new());
        let unary_release = Arc::new(Notify::new());
        let sse_release = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let (handle_tx, handle_rx) = oneshot::channel();
        let handle_tx = Arc::new(Mutex::new(Some(handle_tx)));
        let application = FlowApplication::builder()
            .with_config_file(config)
            .with_cli_args(["embedded-host-test"])
            .with_log_level(LogLevel::Error)
            .with_web_endpoints(vec![
                Box::new(PendingResponse {
                    sse: false,
                    entered: unary_entered.clone(),
                    release: unary_release.clone(),
                    dropped: dropped.clone(),
                }),
                Box::new(PendingResponse {
                    sse: true,
                    entered: sse_entered.clone(),
                    release: sse_release.clone(),
                    dropped: dropped.clone(),
                }),
            ])
            .with_flow_handle_hook(move |flow| {
                let _ = handle_tx.lock().unwrap().take().unwrap().send(flow.clone());
                tokio::spawn(async {})
            });
        let definition = FlowDefinition::materialize(move |_| {
            let source = IdleInfiniteSource;
            let sink = NoopSink;
            Ok(flow! {
                name: "embedded_host_scope", journals: crate::journal::memory_journals(),
                stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                topology: { src |> sink; }
            })
        });
        let application = tokio::spawn(application.run_async(definition));
        let flow = handle_rx.await.unwrap();
        let address = ("127.0.0.1", port);
        let mut ready = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(socket) = TcpStream::connect(address).await {
                    break socket;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real listener must bind while manual Run is withheld");
        ready
            .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        ready.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 503"));
        let mut health = TcpStream::connect(address).await.unwrap();
        health
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        response.clear();
        health.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(!matches!(flow.current_state(), PipelineState::Running));
        flow.start().await.unwrap();
        let mut unary = TcpStream::connect(address).await.unwrap();
        let mut sse = TcpStream::connect(address).await.unwrap();
        unary
            .write_all(b"GET /pending-unary HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        sse.write_all(b"GET /pending-sse HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        unary_entered.notified().await;
        sse_entered.notified().await;
        flow.stop_graceful(Duration::from_secs(2)).await.unwrap();
        if !force_close {
            tokio::time::timeout(Duration::from_secs(3), async {
                while flow.is_running() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Runtime must finish terminal publication before host close");
            unary_release.notify_one();
            sse_release.notify_one();
        }
        let result = tokio::time::timeout(Duration::from_secs(10), application)
            .await
            .unwrap()
            .unwrap();
        if force_close {
            assert!(matches!(result, Err(ApplicationError::Other(error))
                if matches!(error.downcast_ref::<crate::web::host_error::ManagedWebHostError>(), Some(crate::web::host_error::ManagedWebHostError::CloseTimeout))));
        } else {
            result.unwrap();
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        let _ = tokio::time::timeout(Duration::from_secs(1), unary.read_to_end(&mut Vec::new()))
            .await
            .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), sse.read_to_end(&mut Vec::new()))
            .await
            .unwrap();
        let _rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signals_preserve_published_failures_and_repeatable_observation() {
    use obzenflow_core::event::JournalEvent;
    for started in [false, true] {
        for on_terminal in ["park", "exit"] {
            for signal in [ShutdownSignal::Sigint, ShutdownSignal::Sigterm] {
                let dir = tempfile::tempdir().unwrap();
                let config = dir.path().join("obzenflow.toml");
                let port = available_local_port();
                std::fs::write(
                    &config,
                    format!(
                        r#"
[server]
enabled = true
host = "127.0.0.1"
port = {port}
startup_mode = "manual"
on_terminal = "{on_terminal}"
[metrics]
enabled = false
"#
                    ),
                )
                .unwrap();
                let (signal_tx, signal_rx) = oneshot::channel();
                let signal_tx = Arc::new(Mutex::new(Some(signal_tx)));
                let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
                let hook_observed = observed.clone();
                let (admitted_tx, admitted_rx) = oneshot::channel();
                let admitted_tx = Mutex::new(Some(admitted_tx));
                let application = FlowApplication::launch(
                    FlowDefinition::materialize(move |_| {
                        let source = IdleInfiniteSource;
                        let sink = NoopSink;
                        Ok(flow! {
                            name: "failed_exit_signal", journals: crate::journal::memory_journals(),
                            stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                            topology: { src |> sink; }
                        })
                    }),
                    LaunchParams {
                        cli_args: Some(vec![
                            "regression".into(),
                            "--config".into(),
                            config.into_os_string(),
                        ]),
                        test_shutdown_signal: Some(signal_rx),
                        flow_handle_hooks: vec![Box::new(move |flow| {
                            *hook_observed.lock().unwrap() = Some(flow.clone());
                            admitted_tx
                                .lock()
                                .unwrap()
                                .take()
                                .unwrap()
                                .send(())
                                .unwrap();
                            let flow = flow.clone();
                            let signal_tx = signal_tx.lock().unwrap().take().unwrap();
                            Ok(tokio::spawn(async move {
                                flow.wait_for_ready().await.unwrap();
                                if started {
                                    flow.start().await.unwrap();
                                }
                                flow.abort("actual pipeline failure").await.unwrap();
                                // In park mode, the signal is strictly later than publication
                                // and this observer cannot consume the application's join.
                                assert!(flow.wait_for_completion().await.is_err());
                                let _ = signal_tx.send(signal);
                            }))
                        })],
                        ..LaunchParams::default()
                    },
                );
                tokio::pin!(application);
                // Bootstrap installs are serialised within a test process.
                // Bound this application's work after it acquires that slot.
                let result = tokio::select! {
                    result = &mut application => result,
                    admitted = admitted_rx => {
                        admitted.unwrap();
                        tokio::time::timeout(Duration::from_secs(5), application).await
                            .expect("failed application must settle after admission")
                    }
                };
                let flow = observed.lock().unwrap().take().unwrap();
                assert!(
                    matches!(result, Err(ApplicationError::FlowExecutionFailed(ref reason))
                    if reason.contains("actual pipeline failure")),
                    "{result:?}"
                );
                assert!(!flow.is_running());
                assert!(flow.wait_for_completion().await.is_err());
                let events = flow
                    .system_journal()
                    .unwrap()
                    .read_all_unordered()
                    .await
                    .unwrap();
                assert!(!events
                    .iter()
                    .any(|envelope| envelope.event.event_type_name()
                        == "system.pipeline.stop_admitted"));
                let terminal: Vec<_> = events
                    .iter()
                    .map(|event| event.event.event_type_name())
                    .filter(|kind| {
                        matches!(
                            *kind,
                            "system.pipeline.failed"
                                | "system.pipeline.cancelled"
                                | "system.pipeline.completed"
                        )
                    })
                    .collect();
                assert_eq!(terminal, ["system.pipeline.failed"]);
                let _rebound = TcpListener::bind(("127.0.0.1", port)).unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_finite_completion_and_infinite_cancellation_match_application_results() {
    use obzenflow_core::event::JournalEvent;
    #[derive(Clone, Debug)]
    struct WaitingFiniteSource;
    impl TypedFiniteSourceHandler for WaitingFiniteSource {
        type Output = IdlePayload;
        fn next(&mut self) -> Result<Option<Vec<IdlePayload>>, SourceError> {
            Ok(Some(Vec::new()))
        }
    }
    for finite in [false, true] {
        for cancel in [false, true] {
            let definition = if finite {
                FlowDefinition::materialize(move |_| {
                    let source = WaitingFiniteSource;
                    let sink = NoopSink;
                    Ok(flow! {
                        name: "finite_stop_outcome", journals: crate::journal::memory_journals(),
                        stages: { src = source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                        topology: { src |> sink; }
                    })
                })
            } else {
                FlowDefinition::materialize(move |_| {
                    let source = IdleInfiniteSource;
                    let sink = NoopSink;
                    Ok(flow! {
                        name: "infinite_stop_outcome", journals: crate::journal::memory_journals(),
                        stages: { src = infinite_source!(IdlePayload => source); sink = sink!(IdlePayload => sink); },
                        topology: { src |> sink; }
                    })
                })
            };
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("obzenflow.toml");
            std::fs::write(
                &config,
                format!(
                    r#"
[server]
enabled = true
host = "127.0.0.1"
port = {}
startup_mode = "manual"
on_terminal = "exit"
[metrics]
enabled = false
"#,
                    available_local_port()
                ),
            )
            .unwrap();
            let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
            let hook_observed = observed.clone();
            let (admitted_tx, admitted_rx) = oneshot::channel();
            let admitted_tx = Mutex::new(Some(admitted_tx));
            let application = FlowApplication::builder()
                .with_cli_args(["regression"])
                .with_config_file(config)
                .with_flow_handle_hook(move |flow| {
                    *hook_observed.lock().unwrap() = Some(flow.clone());
                    admitted_tx
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(())
                        .unwrap();
                    let flow = flow.clone();
                    tokio::spawn(async move {
                        flow.start().await.unwrap();
                        // start() admits Run; it does not wait for execution.
                        // This case exercises stopping an acknowledged running
                        // flow, rather than cancellation overtaking startup.
                        let mut reader = crate::application::lifecycle_observation::Reader::new(
                            flow.system_journal().unwrap(),
                            flow.pipeline_writer_id(),
                        );
                        loop {
                            reader.catch_up().await;
                            if matches!(
                                reader.projection.progress,
                                crate::application::lifecycle_observation::Progress::Running
                            ) {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                        if cancel {
                            flow.stop_cancel().await.unwrap();
                        } else {
                            flow.stop_graceful(Duration::from_secs(2)).await.unwrap();
                        }
                        lifecycle::wait(&flow).await.unwrap();
                    })
                })
                .run_async(definition);
            tokio::pin!(application);
            let result = tokio::select! {
                result = &mut application => result,
                admitted = admitted_rx => {
                    admitted.unwrap();
                    tokio::time::timeout(Duration::from_secs(5), application).await
                        .expect("stopped application must settle after admission")
                }
            };
            assert!(
                result.is_ok(),
                "finite={finite} cancel={cancel}: {result:?}"
            );
            let flow = observed.lock().unwrap().take().unwrap();
            lifecycle::wait(&flow).await.unwrap();
            let facts = flow
                .system_journal()
                .unwrap()
                .read_all_unordered()
                .await
                .unwrap();
            let terminals: Vec<_> = facts
                .iter()
                .map(|event| event.event.event_type_name())
                .filter(|kind| {
                    matches!(
                        *kind,
                        "system.pipeline.failed"
                            | "system.pipeline.cancelled"
                            | "system.pipeline.completed"
                    )
                })
                .collect();
            assert_eq!(
                terminals,
                [if finite && !cancel {
                    "system.pipeline.completed"
                } else {
                    "system.pipeline.cancelled"
                }]
            );
        }
    }
}
