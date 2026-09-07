// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_completion_and_panic_remain_primary_across_application_phases() {
    use crate::web::host_error::ManagedWebHostError;
    use futures::FutureExt;
    use obzenflow_core::event::{PipelineLifecycleEvent, SystemEventType};
    use obzenflow_runtime::pipeline::FlowStopStatus;
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
            let admitted_deadline = Arc::new(Mutex::new(None));
            let task_deadline = admitted_deadline.clone();
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
                    let mut status = flow.stop_status_receiver();
                    if matches!(phase, FaultPhase::Draining) {
                        flow.stop_graceful(Duration::from_secs(60)).await.unwrap();
                    } else {
                        flow.stop_cancel().await.unwrap();
                    }
                    loop {
                        let admitted = status.borrow_and_update().clone();
                        if let FlowStopStatus::Graceful { deadline } = admitted {
                            *task_deadline.lock().unwrap() = Some(deadline);
                            break;
                        }
                        if matches!(admitted, FlowStopStatus::Cancelling { .. }) {
                            break;
                        }
                        status.changed().await.unwrap();
                    }
                }
                // A terminal-success observation and shutdown signal cannot hide
                // an independently failed host task.
                if matches!(phase, FaultPhase::Terminal) {
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
            if let Some(deadline) = *admitted_deadline.lock().unwrap() {
                assert!(
                    matches!(*flow.stop_status_receiver().borrow(), FlowStopStatus::Graceful { deadline: final_deadline } if deadline == final_deadline)
                );
            }
            let events = flow
                .system_journal()
                .unwrap()
                .read_all_unordered()
                .await
                .unwrap();
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
    use obzenflow_runtime::pipeline::{FlowStopStatus, PipelineEvent};
    use obzenflow_runtime::supervised_base::SupervisorHandle;
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
                                flow.send_event(PipelineEvent::Error {
                                    message: "actual pipeline failure".into(),
                                })
                                .await
                                .unwrap();
                                // In park mode, the signal is strictly later than publication
                                // and this observer cannot consume the application's join.
                                assert!(flow.wait_for_termination().await.is_err());
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
                assert!(matches!(
                    *flow.stop_status_receiver().borrow(),
                    FlowStopStatus::NotRequested
                ));
                assert!(flow.wait_for_termination().await.is_err());
                let events = flow
                    .system_journal()
                    .unwrap()
                    .read_all_unordered()
                    .await
                    .unwrap();
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
                        if cancel {
                            flow.stop_cancel().await.unwrap();
                        } else {
                            flow.stop_graceful(Duration::from_secs(2)).await.unwrap();
                        }
                        flow.wait_for_termination().await.unwrap();
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
            flow.wait_for_termination().await.unwrap();
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
