// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Spring Boot-style application framework for ObzenFlow
//!
//! Provides automatic lifecycle management for flows including:
//! - CLI argument parsing
//! - Runtime creation and configuration
//! - Observability setup (tracing, console-subscriber)
//! - HTTP server management
//! - Graceful shutdown handling

use super::web_surface::label_endpoint;
use super::{
    ApplicationError, FlowConfig, Presentation, RunPresentationOutcome, WebSurfaceAttachment,
    WebSurfaceWiringContext,
};
#[cfg(feature = "warp-server")]
use crate::application::config::CorsModeArg;
use crate::application::config::ResolvedStartupConfig;
use crate::web::endpoints::event_ingestion::{HttpIngress, IngressDecoder, IngressHandle};
#[cfg(feature = "warp-server")]
use crate::web::host_config::{HostConfig, HostCorsConfig, HostCorsMode};
#[cfg(feature = "warp-server")]
use crate::web::surface_metrics::{HttpSurfaceMetricsCollector, HttpSurfaceMetricsEmitter};
#[cfg(feature = "warp-server")]
use crate::web::RuntimeInstanceId;
use obzenflow_adapters::monitoring::MetricsReadModel;
use obzenflow_core::metrics::{InfraMetricsSnapshot, MetricsSnapshotExporter};
use obzenflow_core::web::HttpEndpoint;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::FlowDefinition;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, try_install_bootstrap_config};
use obzenflow_runtime::journal::CurrentRunLocator;
use obzenflow_runtime::prelude::FlowHandle;
use obzenflow_runtime::stages::LivenessSnapshots;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

type FlowHandleHook =
    Box<dyn Fn(&Arc<FlowHandle>) -> Result<JoinHandle<()>, ApplicationError> + Send + Sync>;

/// Cancels an application-owned task even if launch or shutdown is dropped.
/// This guard has no reporting or provider responsibilities.
struct ApplicationTask(JoinHandle<()>);

impl ApplicationTask {
    async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for ApplicationTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod application_task_tests {
    use super::ApplicationTask;
    use tokio::sync::oneshot;

    struct Cancelled(Option<oneshot::Sender<()>>);

    impl Drop for Cancelled {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }

    async fn pending_task() -> (ApplicationTask, oneshot::Receiver<()>) {
        let (started_tx, started_rx) = oneshot::channel();
        let (cancelled_tx, cancelled_rx) = oneshot::channel();
        let task = ApplicationTask(tokio::spawn(async move {
            let _cancelled = Cancelled(Some(cancelled_tx));
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        started_rx.await.unwrap();
        (task, cancelled_rx)
    }

    #[tokio::test]
    async fn stopping_application_task_joins_its_cleanup() {
        let (task, mut cancelled) = pending_task().await;
        task.stop().await;
        assert_eq!(cancelled.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn dropping_application_task_cancels_pending_work() {
        let (task, cancelled) = pending_task().await;
        drop(task);
        tokio::time::timeout(std::time::Duration::from_secs(1), cancelled)
            .await
            .expect("application drop must not leave a detached task")
            .unwrap();
    }
}

#[derive(Default)]
struct LaunchParams {
    builder_config_file: Option<PathBuf>,
    enable_autodiscovery: bool,
    web_surfaces: Vec<WebSurfaceAttachment>,
    extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    flow_handle_hooks: Vec<FlowHandleHook>,
    presentation: Option<Presentation>,
    cli_args: Option<Vec<OsString>>,
    #[cfg(all(test, feature = "warp-server"))]
    test_shutdown_signal: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
    #[cfg(all(test, feature = "warp-server"))]
    test_host_task: Option<(
        futures::future::BoxFuture<
            'static,
            Result<(), crate::web::host_error::ManagedWebHostError>,
        >,
        bool,
    )>,
}

impl LaunchParams {
    fn autodiscovery_enabled() -> Self {
        Self {
            enable_autodiscovery: true,
            ..Self::default()
        }
    }
}

/// Application identity and shutdown ownership passed together to the host.
#[cfg(feature = "warp-server")]
struct HostLifecycle {
    runtime_config: Arc<obzenflow_runtime::runtime_config::ResolvedRuntimeConfig>,
    instance_id: RuntimeInstanceId,
    shutdown: tokio::sync::watch::Sender<bool>,
}

#[cfg(all(test, feature = "warp-server"))]
mod tests {
    use super::*;
    use crate::journal::disk_journals;
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;

    use obzenflow_dsl::{flow, infinite_source, sink, source};
    use obzenflow_runtime::pipeline::PipelineState;
    use obzenflow_runtime::stages::common::handlers::{
        InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
        TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
    };
    use obzenflow_runtime::stages::SourceError;
    use std::net::TcpListener;
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    include!("managed_lifecycle_tests.rs");

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct IdlePayload;

    impl TypedPayload for IdlePayload {
        const EVENT_TYPE: &'static str = "flow_application.idle";
    }

    #[derive(Clone, Debug)]
    struct IdleInfiniteSource;

    impl TypedInfiniteSourceHandler for IdleInfiniteSource {
        type Output = IdlePayload;

        fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
            Ok(Vec::new())
        }
    }

    #[derive(Clone, Debug)]
    struct OneShotSource {
        emitted: bool,
    }

    impl OneShotSource {
        fn new() -> Self {
            Self { emitted: false }
        }
    }

    impl TypedFiniteSourceHandler for OneShotSource {
        type Output = IdlePayload;

        fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            if self.emitted {
                Ok(None)
            } else {
                self.emitted = true;
                Ok(Some(vec![IdlePayload]))
            }
        }
    }

    #[derive(Clone, Debug)]
    struct NoopSink;

    #[async_trait]
    impl InlineSink for NoopSink {
        type Input = IdlePayload;

        fn describe(&self) -> SinkDescription {
            SinkDescription::unspecified()
        }

        async fn write(
            &mut self,
            _input: IdlePayload,
            _context: SinkWriteContext,
        ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
            Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
                DeliveryMethod::Custom("test".to_string()),
                None,
            )))
        }
    }

    fn available_local_port() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
        listener.local_addr().expect("local addr").port()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn authentication_admission_failure_prevents_automatic_run() {
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

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("obzenflow.toml");
        let missing = format!("OBZENFLOW_AUTH_MISSING_{}", uuid::Uuid::new_v4().simple());
        std::fs::write(
            &config_path,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {}
startup_mode = "auto"
[server.control_plane_auth]
mode = "api_key"
value_env = "{missing}"
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
        let definition = FlowDefinition::new(move |context| async move {
            let source = CountingSource(source_calls);
            let sink = NoopSink;
            let handle = flow! {
                name: "auth_admission_no_automatic_run",
                journals: crate::journal::memory_journals(),
                stages: {
                    src = infinite_source!(IdlePayload => source);
                    sink = sink!(IdlePayload => sink);
                },
                topology: { src |> sink; }
            }
            .build(context)
            .await?;
            handle
                .wait_for_ready()
                .await
                .expect("pipeline reaches readiness");
            assert!(matches!(handle.current_state(), PipelineState::ReadyForRun));
            Ok(handle)
        });
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            FlowApplication::launch(
                definition,
                LaunchParams {
                    enable_autodiscovery: false,
                    cli_args: Some(vec![
                        "obzenflow".into(),
                        "--config".into(),
                        config_path.into_os_string(),
                    ]),
                    flow_handle_hooks: vec![Box::new(move |handle| {
                        *hook_observed.lock().unwrap() = Some(handle.clone());
                        Ok(tokio::spawn(async {}))
                    })],
                    ..LaunchParams::default()
                },
            ),
        )
        .await
        .expect("auth admission must return promptly");
        assert!(matches!(
            result,
            Err(ApplicationError::ServerStartFailed(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let handle = observed.lock().unwrap().take().unwrap();
        assert!(
            !handle.is_running(),
            "failed admission must stop the waiting supervisor"
        );
        assert!(handle.current_state().is_terminal());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn occupied_port_prevents_automatic_run() {
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

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("obzenflow.toml");
        let journal_dir = dir.path().join("journals");
        let flow_journal_dir = journal_dir.clone();
        let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        std::fs::write(
            &config_path,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {}
startup_mode = "auto"
[metrics]
enabled = false
"#,
                occupied.local_addr().unwrap().port()
            ),
        )
        .unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let source_calls = calls.clone();
        let observed = Arc::new(Mutex::new(None::<Arc<FlowHandle>>));
        let hook_observed = observed.clone();
        let definition = FlowDefinition::new(move |context| async move {
            let source = CountingSource(source_calls);
            let sink = NoopSink;
            let handle = flow! {
                name: "bind_admission_no_automatic_run",
                journals: disk_journals(flow_journal_dir),
                stages: {
                    src = infinite_source!(IdlePayload => source);
                    sink = sink!(IdlePayload => sink);
                },
                topology: { src |> sink; }
            }
            .build(context)
            .await?;
            handle
                .wait_for_ready()
                .await
                .expect("pipeline reaches readiness");
            assert!(matches!(handle.current_state(), PipelineState::ReadyForRun));
            Ok(handle)
        });
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            FlowApplication::launch(
                definition,
                LaunchParams {
                    enable_autodiscovery: false,
                    cli_args: Some(vec![
                        "obzenflow".into(),
                        "--config".into(),
                        config_path.into_os_string(),
                    ]),
                    flow_handle_hooks: vec![Box::new(move |handle| {
                        *hook_observed.lock().unwrap() = Some(handle.clone());
                        Ok(tokio::spawn(async {}))
                    })],
                    ..LaunchParams::default()
                },
            ),
        )
        .await
        .expect("bind admission must return promptly");
        assert!(matches!(
            result,
            Err(ApplicationError::ServerStartFailed(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let handle = observed.lock().unwrap().take().unwrap();
        assert!(
            !handle.is_running(),
            "failed admission must stop the waiting supervisor"
        );
        assert!(handle.current_state().is_terminal());
        let run_dir = std::fs::read_dir(journal_dir.join("flows"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap();
        let export = dir.path().join("occupied-port.jsonl");
        crate::journal::disk::inspect::export_jsonl(&run_dir, Some(&export)).unwrap();
        let records: Vec<serde_json::Value> = std::fs::read_to_string(export)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            !records.is_empty(),
            "the negative witness must inspect actual committed records"
        );
        for record in records {
            let event = &record["event"];
            assert_ne!(
                event["pipeline_event"], "running",
                "failed bind cannot publish Running"
            );
            assert_ne!(
                event["content"]["content_type"], "data",
                "failed bind cannot commit source data or effect records"
            );
            assert_ne!(
                event["content"]["content_type"], "delivery",
                "failed bind cannot commit sink receipts"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_auto_mode_starts_after_host_admission() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let journal_dir = tempdir.path().join("journals");
        std::fs::create_dir_all(&journal_dir).expect("create journal root");
        let config_path = tempdir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(
            &config_path,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {port}
startup_mode = "auto"

[runtime]
shutdown_timeout_secs = 2

[metrics]
enabled = false
"#
            ),
        )
        .expect("write test config");

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
        let (running_tx, running_rx) = oneshot::channel();
        let running_tx = Arc::new(Mutex::new(Some(running_tx)));

        let hook_shutdown = Arc::clone(&shutdown_tx);
        let hook_running = Arc::clone(&running_tx);
        let hook = move |flow_handle: &Arc<FlowHandle>| {
            let flow_handle = Arc::clone(flow_handle);
            let hook_shutdown = Arc::clone(&hook_shutdown);
            let hook_running = Arc::clone(&hook_running);
            tokio::spawn(async move {
                let mut states = flow_handle.state_receiver();
                loop {
                    if matches!(*states.borrow(), PipelineState::Running) {
                        break;
                    }
                    if states.changed().await.is_err() {
                        return;
                    }
                }

                if let Some(tx) = hook_running.lock().expect("running lock poisoned").take() {
                    let _ = tx.send(());
                }
                if let Some(tx) = hook_shutdown.lock().expect("shutdown lock poisoned").take() {
                    let _ = tx.send(ShutdownSignal::Sigint);
                }
            })
        };

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            FlowApplication::launch(
                FlowDefinition::materialize(move |_runtime_config| {
                    let source = IdleInfiniteSource;
                    let sink = NoopSink;

                    Ok(flow! {
                        name: "server_auto_double_run_regression",
                        journals: disk_journals(journal_dir),

                        stages: {
                            src = infinite_source!(IdlePayload => source);
                            sink = sink!(IdlePayload => sink);
                        },

                        topology: {
                            src |> sink;
                        }
                    })
                }),
                LaunchParams {
                    enable_autodiscovery: false,
                    flow_handle_hooks: vec![Box::new(move |flow_handle| Ok(hook(flow_handle)))],
                    cli_args: Some(vec![
                        OsString::from("obzenflow"),
                        OsString::from("--config"),
                        config_path.into_os_string(),
                    ]),
                    test_shutdown_signal: Some(shutdown_rx),
                    ..LaunchParams::default()
                },
            ),
        )
        .await
        .expect("FlowApplication should not hang in server auto mode");

        result.expect("server auto mode should shut down cleanly");
        running_rx.await.expect("flow should reach Running");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_on_terminal_exit_waits_for_terminal_journal_fact() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let journal_dir = tempdir.path().join("journals");
        let flow_journal_dir = journal_dir.clone();
        std::fs::create_dir_all(&journal_dir).expect("create journal root");
        let config_path = tempdir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(
            &config_path,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {port}
startup_mode = "auto"
on_terminal = "exit"

[runtime]
shutdown_timeout_secs = 2

[metrics]
enabled = false
"#
            ),
        )
        .expect("write test config");

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            FlowApplication::launch(
                FlowDefinition::materialize(move |_runtime_config| {
                    let source = OneShotSource::new();
                    let sink = NoopSink;

                    Ok(flow! {
                        name: "server_terminal_journal_regression",
                        journals: disk_journals(flow_journal_dir),

                        stages: {
                            src = source!(IdlePayload => source);
                            sink = sink!(IdlePayload => sink);
                        },

                        topology: {
                            src |> sink;
                        }
                    })
                }),
                LaunchParams {
                    enable_autodiscovery: false,
                    cli_args: Some(vec![
                        OsString::from("obzenflow"),
                        OsString::from("--config"),
                        config_path.into_os_string(),
                    ]),
                    ..LaunchParams::default()
                },
            ),
        )
        .await
        .expect("FlowApplication should not hang in on_terminal=exit mode");

        result.expect("finite server flow should exit cleanly");

        let flows_dir = journal_dir.join("flows");
        let run_dir = std::fs::read_dir(&flows_dir)
            .expect("flows directory should exist")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.is_dir())
            .expect("one run directory should exist");
        let system_log =
            std::fs::read_to_string(run_dir.join("system.log")).expect("system.log readable");

        assert!(
            system_log.contains(r#""pipeline_event":"completed""#),
            "on_terminal=exit must not close the runtime before the final pipeline_completed fact is committed; system.log:\n{system_log}"
        );
    }

    // FLOWIP-114d gap 24 regression: on graceful server-mode shutdown the
    // heartbeat's fenced DELETE must reach the phonebook rather than being
    // cancelled mid-flight by the generic managed-task abort (which would
    // leave the entry to linger until lease expiry). Drives a real
    // FlowApplication in server mode against a stub phonebook, waits until a
    // registration has landed, then triggers shutdown and asserts the fenced
    // deregistration arrives.
    #[cfg(feature = "studio-registration")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn server_mode_deregisters_from_phonebook_on_graceful_shutdown() {
        use warp::Filter;

        #[derive(Default)]
        struct Stub {
            registrations: Mutex<Vec<serde_json::Value>>,
            deletes: Mutex<Vec<String>>,
        }
        let stub = Arc::new(Stub::default());

        let register = {
            let stub = stub.clone();
            warp::path!("register")
                .and(warp::post())
                .and(warp::body::json())
                .map(move |body: serde_json::Value| {
                    stub.registrations
                        .lock()
                        .expect("registrations lock")
                        .push(body);
                    warp::reply::with_status(warp::reply(), warp::http::StatusCode::NO_CONTENT)
                })
        };
        let deregister = {
            let stub = stub.clone();
            warp::path!("register" / String)
                .and(warp::delete())
                .and(warp::query::raw())
                .map(move |job_id: String, query: String| {
                    stub.deletes
                        .lock()
                        .expect("deletes lock")
                        .push(format!("{job_id}?{query}"));
                    warp::reply::with_status(warp::reply(), warp::http::StatusCode::NO_CONTENT)
                })
        };
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let phonebook_addr = listener.local_addr().unwrap();
        let phonebook_server = warp::serve(register.or(deregister)).incoming(listener);
        let phonebook_task = tokio::spawn(phonebook_server.run());
        let phonebook_url = format!("http://{phonebook_addr}");

        let tempdir = tempfile::tempdir().expect("tempdir");
        let journal_dir = tempdir.path().join("journals");
        std::fs::create_dir_all(&journal_dir).expect("create journal root");
        let config_path = tempdir.path().join("obzenflow.toml");
        let port = available_local_port();
        std::fs::write(
            &config_path,
            format!(
                r#"
[server]
enabled = true
host = "127.0.0.1"
port = {port}
startup_mode = "auto"

[server.cors]
mode = "allow-list"
allow_origins = ["{phonebook_url}"]

[studio]
enabled = true
phonebook_url = "{phonebook_url}"
job_id = "gap24_demo"
advertise_url = "http://127.0.0.1:{port}"
lease_ttl_secs = 30
renew_interval_secs = 1

[runtime]
shutdown_timeout_secs = 2
"#
            ),
        )
        .expect("write test config");

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let shutdown_tx = Arc::new(Mutex::new(Some(shutdown_tx)));
        let (running_tx, running_rx) = oneshot::channel();
        let running_tx = Arc::new(Mutex::new(Some(running_tx)));

        // The hook waits for Running, then for the first registration to land
        // at the stub, and only then triggers shutdown, so registration always
        // precedes deregistration deterministically.
        let hook_shutdown = Arc::clone(&shutdown_tx);
        let hook_running = Arc::clone(&running_tx);
        let hook_stub = Arc::clone(&stub);
        let hook = move |flow_handle: &Arc<FlowHandle>| {
            let flow_handle = Arc::clone(flow_handle);
            let hook_shutdown = Arc::clone(&hook_shutdown);
            let hook_running = Arc::clone(&hook_running);
            let hook_stub = Arc::clone(&hook_stub);
            tokio::spawn(async move {
                let mut states = flow_handle.state_receiver();
                loop {
                    if matches!(*states.borrow(), PipelineState::Running) {
                        break;
                    }
                    if states.changed().await.is_err() {
                        return;
                    }
                }
                if let Some(tx) = hook_running.lock().expect("running lock poisoned").take() {
                    let _ = tx.send(());
                }
                // Bounded wait for the heartbeat's first registration.
                for _ in 0..100 {
                    if !hook_stub
                        .registrations
                        .lock()
                        .expect("registrations lock")
                        .is_empty()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                if let Some(tx) = hook_shutdown.lock().expect("shutdown lock poisoned").take() {
                    let _ = tx.send(ShutdownSignal::Sigint);
                }
            })
        };

        let result = tokio::time::timeout(
            Duration::from_secs(15),
            FlowApplication::launch(
                FlowDefinition::materialize(move |_runtime_config| {
                    let source = IdleInfiniteSource;
                    let sink = NoopSink;

                    Ok(flow! {
                        name: "gap24_deregister_regression",
                        journals: disk_journals(journal_dir),

                        stages: {
                            src = infinite_source!(IdlePayload => source);
                            sink = sink!(IdlePayload => sink);
                        },

                        topology: {
                            src |> sink;
                        }
                    })
                }),
                LaunchParams {
                    enable_autodiscovery: false,
                    flow_handle_hooks: vec![Box::new(move |flow_handle| Ok(hook(flow_handle)))],
                    cli_args: Some(vec![
                        OsString::from("obzenflow"),
                        OsString::from("--config"),
                        config_path.into_os_string(),
                    ]),
                    test_shutdown_signal: Some(shutdown_rx),
                    ..LaunchParams::default()
                },
            ),
        )
        .await
        .expect("FlowApplication should not hang in server studio mode");

        result.expect("server studio mode should shut down cleanly");
        running_rx.await.expect("flow should reach Running");

        assert!(
            !stub
                .registrations
                .lock()
                .expect("registrations lock")
                .is_empty(),
            "runtime should have registered with the phonebook"
        );
        let deletes = stub.deletes.lock().expect("deletes lock");
        let prefix = "gap24_demo?runtime_instance_id=";
        assert!(
            deletes
                .iter()
                .any(|d| d.starts_with(prefix) && d.len() > prefix.len()),
            "graceful shutdown must send a fenced deregistration rather than \
             relying on lease expiry; got {deletes:?}"
        );

        phonebook_task.abort();
    }
}

#[cfg(feature = "warp-server")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ShutdownSignal {
    Sigint,
    Sigterm,
}

fn resolve_startup_config(
    builder_config_file: Option<PathBuf>,
    enable_autodiscovery: bool,
    cli_args: Option<Vec<OsString>>,
) -> Result<ResolvedStartupConfig, ApplicationError> {
    let result = if let Some(cli_args) = cli_args {
        FlowConfig::parse_and_resolve_from(cli_args, builder_config_file, enable_autodiscovery)
    } else {
        FlowConfig::parse_and_resolve(builder_config_file, enable_autodiscovery)
    };

    result.map_err(|err| ApplicationError::InvalidConfiguration(err.to_string()))
}

/// Configuration for log level filtering
#[derive(Debug, Clone)]
pub enum LogLevel {
    /// Show all logs (trace, debug, info, warn, error)
    Trace,
    /// Show debug and above (debug, info, warn, error)
    Debug,
    /// Show info and above (info, warn, error) - default
    Info,
    /// Show warnings and errors only
    Warn,
    /// Show errors only
    Error,
    /// Custom filter string (e.g., "info,obzenflow=debug")
    Custom(String),
}

impl LogLevel {
    fn as_filter_string(&self) -> String {
        match self {
            LogLevel::Trace => "trace".to_string(),
            LogLevel::Debug => "debug".to_string(),
            LogLevel::Info => "info".to_string(),
            LogLevel::Warn => "warn".to_string(),
            LogLevel::Error => "error".to_string(),
            LogLevel::Custom(s) => s.clone(),
        }
    }
}

/// Builder for advanced FlowApplication configuration.
///
/// **Prefer `FlowApplication::run()` with `#[tokio::main]` for most use cases.** The builder
/// is only needed when you require web endpoints, flow handle hooks, or console-subscriber
/// integration. If you only need a log level, set the `RUST_LOG` environment variable instead.
///
/// # Example
/// ```ignore
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let flow = build_flow(); // returns a deferred FlowDefinition
///     FlowApplication::builder()
///         .with_web_endpoint(my_endpoint)
///         .run_blocking(flow)?;
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct FlowApplicationBuilder {
    console_subscriber: bool,
    console_bind: Option<String>,
    log_level: Option<LogLevel>,
    config_file: Option<PathBuf>,
    web_surfaces: Vec<WebSurfaceAttachment>,
    web_endpoints: Vec<Box<dyn HttpEndpoint>>,
    flow_handle_hooks: Vec<FlowHandleHook>,
    presentation: Option<Presentation>,
    cli_args: Option<Vec<OsString>>,
}

impl FlowApplicationBuilder {
    /// Enable tokio-console-subscriber for runtime introspection
    ///
    /// This allows you to connect with `tokio-console` CLI tool to inspect
    /// tasks, async operations, and resource usage in real-time.
    ///
    /// This method is always available, but only takes effect when the `tokio-console`
    /// feature is enabled at compile time. This allows user code to be written
    /// once without #[cfg] attributes.
    ///
    /// # Example
    /// ```ignore
    /// // This works whether or not 'tokio-console' feature is enabled!
    /// FlowApplication::builder()
    ///     .with_console_subscriber()  // No-op if feature disabled
    ///     .run_blocking(build_flow())
    /// ```
    pub fn with_console_subscriber(mut self) -> Self {
        self.console_subscriber = true;
        self
    }

    /// Set the bind address for console-subscriber
    ///
    /// Default is "127.0.0.1:6669"
    pub fn with_console_bind(mut self, bind: impl Into<String>) -> Self {
        self.console_bind = Some(bind.into());
        self
    }

    /// Set the log level for tracing output
    ///
    /// This filters which log levels are displayed. If not set, defaults to Info.
    /// Can be overridden by the `RUST_LOG` environment variable.
    pub fn with_log_level(mut self, level: LogLevel) -> Self {
        self.log_level = Some(level);
        self
    }

    /// Use an explicit startup config file for builder-driven runs.
    ///
    /// When not set, builder-driven runs also participate in `obzenflow.toml`
    /// autodiscovery from the current working directory. CLI `--config <path>`
    /// still wins when both are present.
    pub fn with_config_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_file = Some(path.into());
        self
    }

    /// Use explicit application argv for config parsing instead of process argv.
    ///
    /// This is useful for embedded callers and tests where process argv belongs
    /// to a host/test harness. The first item should be the binary name, matching
    /// normal CLI parsing conventions.
    pub fn with_cli_args<I, T>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString>,
    {
        self.cli_args = Some(args.into_iter().map(Into::into).collect());
        self
    }

    /// Add multiple HTTP endpoints to be hosted by FlowApplication when running with `--server`.
    ///
    /// This appends to any endpoints already registered via `with_web_endpoint(...)`.
    pub fn with_web_endpoints(mut self, mut endpoints: Vec<Box<dyn HttpEndpoint>>) -> Self {
        self.web_endpoints.append(&mut endpoints);
        self
    }

    /// Register a managed web surface to be hosted by FlowApplication when running with `--server`.
    ///
    /// This is the preferred extension point for HTTP-facing capabilities that should share
    /// `FlowApplication` lifecycle, readiness wiring, and shutdown behaviour by default.
    pub fn with_web_surface(mut self, surface: WebSurfaceAttachment) -> Self {
        self.web_surfaces.push(surface);
        self
    }

    /// Register the optional framework-owned HTTP ingress adaptor.
    pub fn with_http_ingress<D>(mut self, ingress: HttpIngress<D>) -> Self
    where
        D: IngressDecoder,
    {
        let (surface, handle) = ingress.into_surface_and_handle();
        self.web_surfaces.push(surface);
        self = self.with_ingress_handle(handle);
        self
    }

    /// Wire a developer-owned ingress handle to the built flow.
    pub fn with_ingress_handle<T>(mut self, handle: IngressHandle<T>) -> Self
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        self.flow_handle_hooks.push(Box::new(move |flow_handle| {
            handle.bind_flow_handle(flow_handle)
        }));
        self
    }

    /// Add a single HTTP endpoint to be hosted by FlowApplication when running with `--server`.
    pub fn with_web_endpoint(mut self, endpoint: Box<dyn HttpEndpoint>) -> Self {
        self.web_endpoints.push(endpoint);
        self
    }

    /// Register a hook that runs after the flow is built (but before the server starts).
    ///
    /// This is useful for wiring FlowHandle state into other subsystems (e.g. ingestion readiness).
    pub fn with_flow_handle_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&Arc<FlowHandle>) -> JoinHandle<()> + Send + Sync + 'static,
    {
        self.flow_handle_hooks
            .push(Box::new(move |flow_handle| Ok(hook(flow_handle))));
        self
    }

    pub fn with_presentation(mut self, presentation: Presentation) -> Self {
        self.presentation = Some(presentation);
        self
    }

    /// Run the flow in a blocking context (without #[tokio::main])
    ///
    /// This builds the tokio runtime, initializes observability, and runs the flow.
    /// Use this when you have a plain `fn main()` and want FlowApplication to
    /// manage the entire runtime lifecycle.
    pub fn run_blocking(self, flow: FlowDefinition) -> Result<(), ApplicationError> {
        // Build tokio runtime so we have a handle for console-subscriber
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| ApplicationError::RuntimeCreationFailed(e.to_string()))?;

        // Initialize tracing/console-subscriber using the runtime handle
        self.init_observability(Some(runtime.handle()));

        let FlowApplicationBuilder {
            config_file,
            web_surfaces,
            web_endpoints,
            flow_handle_hooks,
            presentation,
            cli_args,
            ..
        } = self;

        // Run the flow in the runtime
        runtime.block_on(FlowApplication::launch(
            flow,
            LaunchParams {
                builder_config_file: config_file,
                enable_autodiscovery: true,
                web_surfaces,
                extra_endpoints: web_endpoints,
                flow_handle_hooks,
                presentation,
                cli_args,
                #[cfg(all(test, feature = "warp-server"))]
                test_shutdown_signal: None,
                #[cfg(all(test, feature = "warp-server"))]
                test_host_task: None,
            },
        ))
    }

    /// Run the flow in an existing async context (with #[tokio::main])
    ///
    /// Use this when you already have a tokio runtime (e.g., from #[tokio::main])
    /// and just want FlowApplication to handle observability setup.
    pub async fn run_async(self, flow: FlowDefinition) -> Result<(), ApplicationError> {
        // Initialize tracing/console-subscriber with the current runtime handle
        self.init_observability(Some(&tokio::runtime::Handle::current()));

        let FlowApplicationBuilder {
            config_file,
            web_surfaces,
            web_endpoints,
            flow_handle_hooks,
            presentation,
            cli_args,
            ..
        } = self;

        // Run the flow
        FlowApplication::launch(
            flow,
            LaunchParams {
                builder_config_file: config_file,
                enable_autodiscovery: true,
                web_surfaces,
                extra_endpoints: web_endpoints,
                flow_handle_hooks,
                presentation,
                cli_args,
                #[cfg(all(test, feature = "warp-server"))]
                test_shutdown_signal: None,
                #[cfg(all(test, feature = "warp-server"))]
                test_host_task: None,
            },
        )
        .await
    }

    /// Initialize observability (tracing + console-subscriber)
    fn init_observability(&self, _runtime_handle: Option<&tokio::runtime::Handle>) {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::EnvFilter;

        // Determine log level (RUST_LOG env var takes precedence)
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            let level = self
                .log_level
                .as_ref()
                .unwrap_or(&LogLevel::Info)
                .as_filter_string();
            EnvFilter::new(level)
        });

        #[cfg(feature = "tokio-console")]
        if self.console_subscriber {
            // Set bind address for console-subscriber (honor existing env override)
            let bind = std::env::var("TOKIO_CONSOLE_BIND")
                .ok()
                .or_else(|| self.console_bind.clone())
                .unwrap_or_else(|| "127.0.0.1:6669".to_string());
            let addr: std::net::SocketAddr = bind.parse().unwrap_or_else(|err| {
                let fallback = "127.0.0.1:6669";
                eprintln!(
                    "❌ Invalid TOKIO_CONSOLE_BIND '{bind}': {err}. Falling back to {fallback}",
                );
                fallback.parse().expect("fallback address should parse")
            });
            // Ensure downstream tooling that relies on the env var still sees the effective address
            std::env::set_var("TOKIO_CONSOLE_BIND", &bind);
            eprintln!("ℹ️  tokio-console attempting to bind to {addr}");

            let builder = console_subscriber::ConsoleLayer::builder()
                .with_default_env()
                .server_addr(addr);
            let (console_layer, server) = builder.build();

            // Spawn console server with error logging so bind failures are visible instead of silent
            let bind_for_log = bind.clone();
            let spawn_server = async move {
                if let Err(err) = server.serve().await {
                    eprintln!("❌ tokio-console failed to bind on {bind_for_log}: {err}");
                }
            };
            // Small self-connect probe to surface connectivity issues early
            let addr_for_probe = addr;
            let spawn_probe = async move {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                match tokio::net::TcpStream::connect(addr_for_probe).await {
                    Ok(_) => {
                        eprintln!("✅ tokio-console TCP probe successful on {addr_for_probe}")
                    }
                    Err(err) => {
                        eprintln!("❌ tokio-console TCP probe failed on {addr_for_probe}: {err}")
                    }
                }
            };

            if let Some(handle) = _runtime_handle {
                handle.spawn(spawn_server);
                handle.spawn(spawn_probe);
            } else {
                // Fallback: attempt to spawn on whatever runtime is available
                tokio::spawn(spawn_server);
                tokio::spawn(spawn_probe);
            }

            tracing_subscriber::registry()
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer())
                .with(filter)
                .try_init()
                .ok();

            eprintln!("🚦 tokio-console enabled on {bind}");
            eprintln!("   Connect with: tokio-console http://{bind}");
            if !cfg!(tokio_unstable) {
                eprintln!("⚠️  Built without `--cfg tokio_unstable`; console may show limited data. Run with RUSTFLAGS=\"--cfg tokio_unstable\" for full instrumentation.");
            }
            return;
        }

        #[cfg(not(feature = "tokio-console"))]
        if self.console_subscriber {
            eprintln!("⚠️  Console subscriber requested but 'tokio-console' feature not enabled");
            eprintln!("   Recompile with --features obzenflow_infra/tokio-console");
        }

        // Standard tracing setup (no console-subscriber)
        let _ = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter)
            .try_init();
    }
}

/// The main application framework for running ObzenFlow flows
///
/// This provides a Spring Boot-style experience where users just call
/// `FlowApplication::run()` with their flow and the framework handles everything:
/// - CLI parsing (--server, --server-port)
/// - Server startup if requested
/// - Flow execution
/// - Graceful shutdown
///
/// # Example with #[tokio::main]
/// ```ignore
/// use obzenflow_infra::application::FlowApplication;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::run(build_flow()).await?;
///     Ok(())
/// }
/// ```
///
/// # Example with builder (console-subscriber, no #[tokio::main])
/// ```ignore
/// use obzenflow_infra::application::{FlowApplication, LogLevel};
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::builder()
///         .with_console_subscriber()
///         .with_log_level(LogLevel::Info)
///         .run_blocking(build_flow())?;
///     Ok(())
/// }
/// ```
pub struct FlowApplication;

impl FlowApplication {
    /// Create a builder for advanced configuration
    ///
    /// Use this when you need:
    /// - Console-subscriber integration
    /// - Custom log levels
    /// - Runtime creation without #[tokio::main]
    ///
    /// # Example
    /// ```ignore
    /// FlowApplication::builder()
    ///     .with_console_subscriber()
    ///     .with_log_level(LogLevel::Info)
    ///     .run_blocking(build_flow())
    /// ```
    pub fn builder() -> FlowApplicationBuilder {
        FlowApplicationBuilder::default()
    }

    /// Run a flow with automatic lifecycle management
    ///
    /// This is the only public method users need to call. It:
    /// 1. Parses CLI arguments automatically
    /// 2. Builds the flow from the provided future
    /// 3. Starts HTTP server if --server flag is present
    /// 4. Runs the flow to completion
    /// 5. Manages server lifecycle after flow completes
    ///
    /// # Arguments
    /// * `flow` - A flow definition produced by `flow!`
    ///
    /// # Returns
    /// * `Ok(())` if flow completes successfully
    /// * `Err(ApplicationError)` if flow fails or cannot start
    pub async fn run(flow: FlowDefinition) -> Result<(), ApplicationError> {
        Self::run_with_web_endpoints(flow, Vec::new()).await
    }

    pub async fn run_with_presentation(
        flow: FlowDefinition,
        presentation: Presentation,
    ) -> Result<(), ApplicationError> {
        Self::launch(
            flow,
            LaunchParams {
                presentation: Some(presentation),
                ..LaunchParams::autodiscovery_enabled()
            },
        )
        .await
    }

    /// Run a flow and host additional web endpoints when `--server` is enabled.
    pub async fn run_with_web_endpoints(
        flow: FlowDefinition,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    ) -> Result<(), ApplicationError> {
        Self::run_with_web_endpoints_and_hooks(flow, extra_endpoints, Vec::new()).await
    }

    pub async fn run_with_web_endpoints_and_hooks(
        flow: FlowDefinition,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        flow_handle_hooks: Vec<FlowHandleHook>,
    ) -> Result<(), ApplicationError> {
        Self::launch(
            flow,
            LaunchParams {
                extra_endpoints,
                flow_handle_hooks,
                ..LaunchParams::autodiscovery_enabled()
            },
        )
        .await
    }

    async fn launch(flow: FlowDefinition, params: LaunchParams) -> Result<(), ApplicationError> {
        let LaunchParams {
            builder_config_file,
            enable_autodiscovery,
            web_surfaces,
            extra_endpoints,
            flow_handle_hooks,
            presentation,
            cli_args,
            #[cfg(all(test, feature = "warp-server"))]
            test_shutdown_signal,
            #[cfg(all(test, feature = "warp-server"))]
            test_host_task,
        } = params;

        // Best-effort tracing initialization when the builder isn't used.
        // This ensures examples like char_transform still emit logs without
        // requiring callers to wire tracing explicitly.
        {
            use tracing_subscriber::prelude::*;
            // Try env filter first; fall back to info if unset.
            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
            let _ = tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .with(filter)
                .try_init();
        }

        let config = resolve_startup_config(builder_config_file, enable_autodiscovery, cli_args)?;

        // FLOWIP-120i: resolve the run mode once, for banner and footer copy.
        let run_mode = match &config.replay {
            Some(replay) => match replay.verb {
                obzenflow_runtime::bootstrap::ReplayVerb::Replay => {
                    super::run_mode::RunMode::replay_from_archive(replay.from.clone())
                }
                obzenflow_runtime::bootstrap::ReplayVerb::Resume => {
                    super::run_mode::RunMode::resume_from_archive(replay.from.clone())
                }
            },
            None => super::run_mode::RunMode::Live,
        };

        // FLOWIP-095j pre-flight: verification re-reads the source archive
        // after completion, so it must remain present through the run.
        if config.replay.as_ref().is_some_and(|replay| replay.verify) {
            if let Some(replay) = &config.replay {
                tracing::info!(
                    archive = %replay.from.display(),
                    "--verify: the source archive must remain present through completion"
                );
            }
        }

        let presentation_enabled = presentation.is_some();

        let grace_timeout = config.runtime.shutdown_timeout;
        #[cfg(feature = "warp-server")]
        let surface_metrics_interval = config.runtime.surface_metrics_interval;

        // Background tasks spawned by FlowHandle hooks and/or web surface wiring closures.
        // These must not be allowed to outlive FlowApplication, even on early-return paths.
        let mut managed_tasks: Vec<ApplicationTask> = Vec::new();
        let metrics_model = (cfg!(feature = "prometheus") && config.metrics.enabled)
            .then(|| Arc::new(MetricsReadModel::default()));
        let metrics_exporter = metrics_model
            .as_ref()
            .map(|model| model.clone() as Arc<dyn MetricsSnapshotExporter>);
        let mut metrics_collector: Option<ApplicationTask> = None;
        // FLOWIP-114d gap 24: the Studio heartbeat is tracked here rather than in
        // `managed_tasks` so the shutdown sequence can join its fenced deregistration
        // before the generic managed-task abort would cancel the in-flight DELETE.
        #[cfg(feature = "studio-registration")]
        let mut heartbeat_task: Option<ApplicationTask> = None;
        #[cfg(feature = "warp-server")]
        let mut surface_metrics_emitter: Option<HttpSurfaceMetricsEmitter> = None;

        if let Some(presentation) = &presentation {
            let rendered = presentation.render_banner(&run_mode);
            for warning in rendered.warnings {
                tracing::warn!("{warning}");
            }
            print!("{}", rendered.text);
        }

        let (result, flow_name, run_state, stopped) = 'run: {
            if (!extra_endpoints.is_empty() || !web_surfaces.is_empty()) && !config.server.enabled {
                break 'run (
                    Err(ApplicationError::InvalidConfiguration(
                        "Web endpoints or surfaces were configured, but FlowApplication is not running with --server"
                            .to_string(),
                    )),
                    None,
                    None,
                    false,
                );
            }

            // FLOWIP-120u: the host opens the replay/resume input archive and the
            // bootstrap snapshot carries it; the build consumes it without the
            // factory opening anything. A bad archive fails here, before any
            // journal is created.
            let bootstrap_config = {
                let mut bootstrap_config = config.bootstrap_config();
                if config.server.enabled {
                    // The host releases automatic Run only after policy admission
                    // and a real socket bind. The supervisor must not race ahead.
                    bootstrap_config.startup_mode =
                        obzenflow_runtime::bootstrap::StartupMode::Manual;
                }
                if let Some(replay) = bootstrap_config.replay.clone() {
                    let archive = crate::journal::disk::replay_archive::DiskReplayArchive::open(
                        replay.archive_path,
                        replay.allow_incomplete_archive,
                    )
                    .await
                    .map_err(|e| {
                        ApplicationError::InvalidConfiguration(format!(
                            "Failed to open replay archive: {e}"
                        ))
                    })?;
                    bootstrap_config.replay_archive = Some(Arc::new(archive));
                }
                bootstrap_config
            };

            let _bootstrap_guard = if cfg!(debug_assertions) {
                // In debug/test builds, allow Rust's parallel test runner to serialize installs
                // rather than failing unrelated tests with an overlapping-run error.
                install_bootstrap_config(bootstrap_config)
            } else {
                try_install_bootstrap_config(bootstrap_config).map_err(|_| {
                    ApplicationError::InvalidConfiguration(
                        "Overlapping FlowApplication runs in the same process are not supported"
                            .to_string(),
                    )
                })?
            };

            // Note: Logging/console-subscriber should be initialized in main() before
            // the tokio runtime is created for console_subscriber to work properly

            tracing::info!("🚀 Starting FlowApplication");

            #[cfg(not(feature = "warp-server"))]
            if config.server.enabled {
                break 'run (
                    Err(ApplicationError::FeatureNotEnabled("web-host".into())),
                    None,
                    None,
                    false,
                );
            }

            // Build the flow (this executes the flow! macro) against the
            // explicit per-run context (FLOWIP-010 §7): the host owns the
            // resolved snapshot; the build consumes it as an input.
            let build_context = obzenflow_runtime::run_context::FlowBuildContext::new(
                config.runtime_config.clone(),
            );
            let build_context = match metrics_exporter.clone() {
                Some(exporter) => build_context.with_metrics_exporter(exporter),
                None => build_context,
            };
            let flow_handle = match flow.build(build_context).await {
                Ok(handle) => handle,
                Err(failure) => {
                    // FLOWIP-120u F2: the failure carrier holds substrate state; None
                    // means the build failed before substrate selection, so no run
                    // directory exists to point at.
                    break 'run (
                        Err(ApplicationError::FlowBuildFailed(failure.error.to_string())),
                        None,
                        failure.run,
                        false,
                    );
                }
            };

            // The selected run substrate (FLOWIP-120u): durable with its locator,
            // or ephemeral with none. An ephemeral resume never reaches here;
            // the build refuses it (F13).
            let run_state = Some(flow_handle.run_substrate().clone());

            let print_replay_hint = |locator: &CurrentRunLocator| {
                println!("FlowApplication complete!");
                println!("To replay, add: --replay-from {locator}");
                println!("(Source config env vars are ignored during replay)");
            };

            let flow_handle = Arc::new(flow_handle);
            let flow_name = flow_handle.flow_name().to_string();

            for hook in &flow_handle_hooks {
                match hook(&flow_handle) {
                    Ok(task) => managed_tasks.push(ApplicationTask(task)),
                    Err(err) => {
                        Self::stop_before_run(&flow_handle, grace_timeout).await;
                        break 'run (Err(err), Some(flow_name.clone()), run_state, false);
                    }
                }
            }

            if let Some(exporter) = metrics_exporter.clone() {
                let liveness = flow_handle.liveness_snapshots();
                Self::publish_infra_snapshot(&exporter, liveness.as_ref());
                let collector = Self::spawn_infra_metrics_collector(
                    exporter,
                    liveness,
                    config.runtime.surface_metrics_interval,
                );
                metrics_collector = Some(ApplicationTask(collector));
            }

            #[cfg(feature = "warp-server")]
            let surface_metrics_collector = {
                let system_journal = flow_handle.system_journal();
                let surface_metrics_collector = if config.server.enabled
                    && !web_surfaces.is_empty()
                    && system_journal.is_some()
                {
                    Some(Arc::new(HttpSurfaceMetricsCollector::new()))
                } else {
                    None
                };

                if let (Some(collector), Some(system_journal)) =
                    (surface_metrics_collector.clone(), system_journal)
                {
                    let emitter = HttpSurfaceMetricsEmitter::new(collector, system_journal);
                    managed_tasks.push(ApplicationTask(
                        emitter.spawn_periodic(surface_metrics_interval),
                    ));
                    surface_metrics_emitter = Some(emitter);
                }

                surface_metrics_collector
            };

            let mut all_extra_endpoints = extra_endpoints;
            for surface in web_surfaces {
                let (surface_name, endpoints, wiring, ingress_slot) = surface.into_parts();
                // FLOWIP-115d (AC41): a registered hosted ingress surface whose
                // source half was never placed in flow topology has an unfilled
                // binding slot. Fail startup before serving endpoints rather than
                // silently running without its configured ingress identity.
                if let Some(slot) = ingress_slot {
                    if !slot.is_filled() {
                        Self::stop_before_run(&flow_handle, grace_timeout).await;
                        break 'run (
                            Err(ApplicationError::FlowBuildFailed(format!(
                                "hosted ingress surface '{surface_name}' (ingress key '{}') was \
                                 registered but its source half was not placed in the flow \
                                 topology; place the http_ingress source in flow!",
                                slot.ingress_key()
                            ))),
                            Some(flow_name.clone()),
                            run_state,
                            false,
                        );
                    }
                }
                for endpoint in endpoints {
                    all_extra_endpoints.push(label_endpoint(&surface_name, endpoint));
                }
                if let Some(wiring) = wiring {
                    match wiring(WebSurfaceWiringContext {
                        pipeline_state: flow_handle.state_receiver(),
                        // FLOWIP-115d: hand hosted ingress surfaces the host system
                        // journal so they can append refusal facts. A surface with
                        // refusal recording enabled fails startup here if it is None.
                        system_journal: flow_handle.system_journal(),
                    }) {
                        Ok(wired) => {
                            managed_tasks.extend(wired.tasks.into_iter().map(ApplicationTask))
                        }
                        Err(err) => {
                            Self::stop_before_run(&flow_handle, grace_timeout).await;
                            break 'run (Err(err), Some(flow_name.clone()), run_state, false);
                        }
                    }
                }
                tracing::debug!(surface = %surface_name, "Web surface attached");
            }

            #[cfg(feature = "warp-server")]
            if config.server.enabled {
                let runtime_instance_id = RuntimeInstanceId::new();
                let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::watch::channel(false);
                let cors_mode = match config.server.cors_mode {
                    CorsModeArg::AllowAnyOrigin => HostCorsMode::AllowAnyOrigin,
                    CorsModeArg::AllowList => {
                        HostCorsMode::AllowList(config.server.cors_allow_origin.clone())
                    }
                    CorsModeArg::SameOrigin => HostCorsMode::SameOrigin,
                };
                let mut server_config =
                    HostConfig::new(config.server.host.clone(), config.server.port);
                server_config.cors = Some(HostCorsConfig { mode: cors_mode });
                server_config.max_body_size = Some(config.server.max_body_size_bytes);
                server_config.request_timeout_secs = Some(config.server.request_timeout_secs);
                server_config.control_plane_auth = config.server.control_plane_auth.clone();
                let mut host = match Self::start_server(
                    &flow_handle,
                    server_config,
                    all_extra_endpoints,
                    surface_metrics_collector,
                    #[cfg(feature = "prometheus")]
                    metrics_model.as_ref().map(|model| {
                        crate::web::endpoints::PrometheusMetricsEndpoint::new(model.clone())
                    }),
                    HostLifecycle {
                        runtime_config: config.runtime_config.clone(),
                        instance_id: runtime_instance_id.clone(),
                        shutdown: server_shutdown_tx,
                    },
                )
                .await
                {
                    Ok(host) => host,
                    Err(error) => {
                        Self::stop_before_run(&flow_handle, grace_timeout).await;
                        break 'run (Err(error), Some(flow_name), run_state, false);
                    }
                };

                #[cfg(test)]
                if let Some((future, complete_before_run)) = test_host_task {
                    host.replace_serving_for_test(future, complete_before_run)
                        .await;
                }

                #[cfg(not(feature = "studio-registration"))]
                let initial_failure = None;
                #[cfg(not(feature = "studio-registration"))]
                let _ = server_shutdown_rx;
                #[cfg(feature = "studio-registration")]
                let mut initial_failure = None;
                #[cfg(feature = "studio-registration")]
                if let Some(studio) = config.studio.clone() {
                    if let Some(system_journal) = flow_handle.system_journal() {
                        let presence = tokio::select! {
                            biased;
                            error = host.failure() => { initial_failure = Some(error); None },
                            presence = crate::web::studio_presence::RuntimePresenceProjection::open(
                                system_journal, flow_handle.pipeline_writer_id(),
                            ) => Some(presence),
                        };
                        if let Some(presence) = presence {
                            heartbeat_task = Some(ApplicationTask(
                                crate::web::studio_registration::spawn_heartbeat(
                                    crate::web::studio_registration::HeartbeatContext {
                                        studio,
                                        runtime_instance_id,
                                        flow_name: flow_name.clone(),
                                        startup_mode: config.server.startup_mode,
                                        probe_base_url: format!("http://{}", host.address()),
                                    },
                                    presence,
                                    server_shutdown_rx,
                                ),
                            ));
                        }
                    }
                }

                let mut result = super::managed_lifecycle::supervise(
                    &mut host,
                    &flow_handle,
                    config.server.startup_mode,
                    config.server.on_terminal,
                    grace_timeout,
                    initial_failure,
                    #[cfg(test)]
                    test_shutdown_signal,
                )
                .await;
                if let Some(task) = metrics_collector.take() {
                    task.stop().await;
                }
                if let Err(error) = host.close().await {
                    let has_host_failure = matches!(&result, Err(ApplicationError::Other(primary))
                        if primary.is::<crate::web::host_error::ManagedWebHostError>());
                    if has_host_failure {
                        tracing::warn!(%error, "Managed host close also failed");
                    } else {
                        if let Err(secondary) = result {
                            tracing::warn!(%secondary, "Flow cleanup also failed before managed host close");
                        }
                        result = Err(ApplicationError::Other(Box::new(error)));
                    }
                }
                #[cfg(feature = "studio-registration")]
                if let Some(mut heartbeat) = heartbeat_task.take() {
                    const DEREGISTER_GRACE: Duration = Duration::from_secs(5);
                    if tokio::time::timeout(DEREGISTER_GRACE, &mut heartbeat.0)
                        .await
                        .is_err()
                    {
                        tracing::warn!("Studio deregistration deadline expired; lease expiry will remove the registration");
                        heartbeat.0.abort();
                        let _ = (&mut heartbeat.0).await;
                    }
                }
                break 'run (result, Some(flow_name), run_state, true);
            }

            // Non-server mode: preserve existing behaviour (run to completion, no HTTP server)
            tracing::info!("▶️  Starting flow execution (no server)");
            let handle = match Arc::try_unwrap(flow_handle) {
                Ok(handle) => handle,
                Err(_) => {
                    break 'run (
                        Err(ApplicationError::FlowExecutionFailed(
                            "Failed to unwrap FlowHandle for non-server execution".to_string(),
                        )),
                        Some(flow_name),
                        run_state,
                        false,
                    );
                }
            };
            let result = handle
                .run()
                .await
                .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()));
            if let Some(task) = metrics_collector.take() {
                task.stop().await;
            }
            if result.is_ok() && !presentation_enabled {
                if let Some(locator) = run_state.as_ref().and_then(|s| s.locator()) {
                    print_replay_hint(locator);
                }
            }
            break 'run (result, Some(flow_name), run_state, false);
        };

        if let Some(task) = metrics_collector.take() {
            task.stop().await;
        }

        #[cfg(feature = "warp-server")]
        if let Some(emitter) = &surface_metrics_emitter {
            let _ = tokio::time::timeout(grace_timeout, emitter.flush()).await;
        }

        // FLOWIP-114d gap 24: a non-graceful exit (an early break during server
        // setup) can leave the heartbeat unjoined; the shared close path above
        // already took it on every normal exit. Abort any leftover so it cannot
        // outlive FlowApplication; lease expiry covers deregistration here.
        #[cfg(feature = "studio-registration")]
        if let Some(heartbeat) = heartbeat_task.take() {
            let _ = tokio::time::timeout(grace_timeout, heartbeat.stop()).await;
        }

        // Best-effort: ensure any hook/surface background tasks cannot escape `FlowApplication`
        // lifetime, even if we exited early due to a startup failure or "no server" fallback.
        Self::cancel_and_join_tasks(managed_tasks, grace_timeout).await;

        match (result, flow_name, run_state, stopped) {
            (Ok(()), flow_name, run_state, stopped) => {
                let location = run_state.as_ref().and_then(|s| s.locator()).cloned();
                // FLOWIP-095j: keep the candidate run directory before the
                // outcome takes ownership of the location below.
                let verify_candidate_dir = location
                    .as_ref()
                    .map(|locator| locator.path().to_path_buf());
                if let Some(presentation) = &presentation {
                    let flow_name = flow_name.unwrap_or_else(|| "Flow".to_string());
                    let outcome = if stopped {
                        RunPresentationOutcome::Stopped {
                            flow_name,
                            location,
                            run_mode: run_mode.clone(),
                        }
                    } else {
                        RunPresentationOutcome::Completed {
                            flow_name,
                            location,
                            run_mode: run_mode.clone(),
                        }
                    };
                    let rendered_footer_banner = presentation.render_footer_banner();
                    let footer = presentation.render_footer(outcome);
                    if rendered_footer_banner.is_some() || !footer.trim().is_empty() {
                        println!();
                        if let Some(rendered_banner) = rendered_footer_banner {
                            for warning in rendered_banner.warnings {
                                tracing::warn!("{warning}");
                            }
                            print!("{}", rendered_banner.text);
                        }
                        if !footer.trim().is_empty() {
                            println!("{footer}");
                        }
                    }
                }

                // FLOWIP-095j: verify the replay output against the source
                // archive after the run reaches its terminal outcome.
                if config.replay.as_ref().is_some_and(|replay| replay.verify) {
                    if let super::run_mode::RunMode::Replay(ctx) = &run_mode {
                        return Self::run_post_replay_verification(
                            ctx.archive_path.clone(),
                            verify_candidate_dir,
                        )
                        .await;
                    }
                }
                Ok(())
            }
            (Err(err), flow_name, run_state, _) => {
                if let Some(presentation) = &presentation {
                    let rendered_footer_banner = presentation.render_footer_banner();
                    let footer = presentation.render_footer(RunPresentationOutcome::Failed {
                        flow_name,
                        error: err.to_string(),
                        location: run_state.as_ref().and_then(|s| s.locator()).cloned(),
                        run_mode: run_mode.clone(),
                    });
                    if rendered_footer_banner.is_some() || !footer.trim().is_empty() {
                        println!();
                        if let Some(rendered_banner) = rendered_footer_banner {
                            for warning in rendered_banner.warnings {
                                tracing::warn!("{warning}");
                            }
                            print!("{}", rendered_banner.text);
                        }
                        if !footer.trim().is_empty() {
                            println!("{footer}");
                        }
                    }
                }
                Err(err)
            }
        }
    }

    /// FLOWIP-095j: the `--verify` convenience form. Compares the just-written
    /// replay run against the source archive it replayed and maps the verdict
    /// onto the exit-code contract (`Ok(())` is exit 0, the fully certified
    /// match; codes 1/2/3 travel as `ApplicationError::Verification`).
    async fn run_post_replay_verification(
        baseline: PathBuf,
        candidate: Option<PathBuf>,
    ) -> Result<(), ApplicationError> {
        let skip = |summary: String| {
            println!("\n{summary}");
            Err(ApplicationError::Verification {
                exit_code: 3,
                summary,
            })
        };

        let Some(candidate) = candidate else {
            return skip(
                "verification skipped: this run wrote no durable run directory (verification needs disk journals)"
                    .to_string(),
            );
        };
        if std::fs::metadata(&baseline).is_err() {
            return skip(format!(
                "verification skipped: source archive unavailable ({})\nrestore the source archive, then compare baseline {} and candidate {} with obzenflow::application::verify_run_dirs",
                baseline.display(),
                baseline.display(),
                candidate.display()
            ));
        }

        let options = crate::verify::VerifyOptions::default();
        let outcome = tokio::task::spawn_blocking(move || {
            crate::verify::verify_run_dirs(&baseline, &candidate, &options)
        })
        .await
        .map_err(|err| ApplicationError::Other(Box::new(err)))?
        .map_err(|err| ApplicationError::Other(Box::new(err)))?;

        let rendered = crate::verify::render_verdict(&outcome);
        println!("\n{rendered}");

        match outcome.exit_code() {
            0 => Ok(()),
            exit_code => Err(ApplicationError::Verification {
                exit_code,
                summary: rendered.lines().next().unwrap_or_default().to_string(),
            }),
        }
    }

    async fn cancel_and_join_tasks(tasks: Vec<ApplicationTask>, timeout: Duration) {
        if tasks.is_empty() {
            return;
        }

        for task in &tasks {
            task.0.abort();
        }

        let _ = tokio::time::timeout(timeout, async move {
            for task in tasks {
                task.stop().await;
            }
        })
        .await;
    }

    fn build_infra_snapshot(
        liveness_snapshots: Option<&LivenessSnapshots>,
    ) -> InfraMetricsSnapshot {
        let mut snapshot = InfraMetricsSnapshot::default();
        if let Some(liveness_snapshots) = liveness_snapshots {
            liveness_snapshots.with_read(|guard| {
                for (stage_id, stage) in guard.iter() {
                    snapshot
                        .liveness_metrics
                        .stage_handler_blocked_seconds
                        .insert(
                            *stage_id,
                            stage
                                .handler_blocked_ms
                                .map(|ms| ms.0 as f64 / 1000.0)
                                .unwrap_or(0.0),
                        );

                    snapshot
                        .liveness_metrics
                        .stage_activity
                        .insert(*stage_id, stage.activity);

                    for edge in &stage.edges {
                        snapshot
                            .liveness_metrics
                            .edge_idle_seconds
                            .insert((edge.upstream, edge.reader), edge.idle_ms.0 as f64 / 1000.0);
                    }
                }
            });
        }

        snapshot
    }

    fn publish_infra_snapshot(
        exporter: &Arc<dyn MetricsSnapshotExporter>,
        liveness_snapshots: Option<&LivenessSnapshots>,
    ) {
        let snapshot = Self::build_infra_snapshot(liveness_snapshots);
        exporter.publish_infra_snapshot(snapshot);
    }

    fn spawn_infra_metrics_collector(
        exporter: Arc<dyn MetricsSnapshotExporter>,
        liveness_snapshots: Option<LivenessSnapshots>,
        interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snapshot = Self::build_infra_snapshot(liveness_snapshots.as_ref());
                exporter.publish_infra_snapshot(snapshot);
            }
        })
    }

    /// Internal: Start the web server with all endpoints
    #[cfg(feature = "warp-server")]
    async fn start_server(
        _flow_handle: &Arc<FlowHandle>,
        _server_config: HostConfig,
        extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
        surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
        #[cfg(feature = "prometheus")] metrics_endpoint: Option<
            crate::web::endpoints::PrometheusMetricsEndpoint,
        >,
        lifecycle: HostLifecycle,
    ) -> Result<crate::web::managed_host::ManagedWebHost, ApplicationError> {
        use crate::web::web_server::{bind_managed_host, ManagedHostInput};

        // Every flow has a topology - it's required to run
        let topology = _flow_handle.topology().ok_or_else(|| {
            ApplicationError::ServerStartFailed(
                "Flow missing topology - this should never happen".to_string(),
            )
        })?;
        #[cfg(feature = "prometheus")]
        let has_metrics = metrics_endpoint.is_some();

        // FLOWIP-114b: stage typing, join metadata, middleware, and
        // subgraph annotations live on the canonical `Topology` directly,
        // so only the contract side map needs to be threaded through.
        let contract_attachments = _flow_handle.contract_attachments();

        let handle = bind_managed_host(
            ManagedHostInput {
                topology,
                contract_attachments,
                #[cfg(feature = "prometheus")]
                metrics_endpoint,
                flow_handle: _flow_handle.clone(),
                extra_endpoints,
                surface_metrics,
                runtime_config: lifecycle.runtime_config,
                runtime_instance_id: lifecycle.instance_id,
                shutdown: lifecycle.shutdown,
            },
            _server_config,
        )
        .await
        .map_err(|e| ApplicationError::ServerStartFailed(e.to_string()))?;

        tracing::info!("📊 Web server started on http://{}", handle.address());
        tracing::info!("   /api/topology  - Flow structure");
        tracing::info!("   /api/config    - Resolved configuration (read-only)");
        #[cfg(feature = "prometheus")]
        if has_metrics {
            tracing::info!("   /metrics       - Prometheus metrics");
        }
        tracing::info!("   /health        - Health status");
        tracing::info!("   /ready         - Readiness status");

        Ok(handle)
    }

    /// Stop a materialised pipeline before the bootstrap guard can restore auto-run.
    async fn stop_before_run(flow: &FlowHandle, grace: Duration) {
        use obzenflow_runtime::supervised_base::SupervisorHandle;
        let cleanup = async {
            if flow.is_running() && !flow.current_state().is_terminal() {
                flow.stop_cancel().await?;
            }
            flow.wait_for_termination().await
        };
        match tokio::time::timeout(grace + grace, cleanup).await {
            Ok(Ok(())) => {}
            result => {
                tracing::warn!(
                    ?result,
                    "Pre-run pipeline cleanup did not complete normally"
                );
                let _ = flow.abort_and_wait().await;
            }
        }
    }
}
