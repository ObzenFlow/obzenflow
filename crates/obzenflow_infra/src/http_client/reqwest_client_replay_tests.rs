// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133c composed proofs for strict replay and resume-to-live HTTP sources.

use super::*;
use async_trait::async_trait;
use obzenflow_adapters::sources::http_pull::HttpRetryConfig;
use obzenflow_adapters::sources::{
    simple_poll, HttpPollConfig, HttpPollSource, HttpPullConfig, HttpPullSource,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEvent, ChainEventContent};
use obzenflow_core::http_client::Url;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_dsl::{async_infinite_source, async_source, flow, sink, FlowDefinition};
use obzenflow_runtime::bootstrap::{
    install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{SinkHandler, TypedAsyncFiniteSourceHandler};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HttpFixtureEvent {
    sequence: usize,
}

impl TypedPayload for HttpFixtureEvent {
    const EVENT_TYPE: &'static str = "flowip_133c.http_fixture";
}

const ISOLATED_TEST_ENV: &str = "OBZENFLOW_FLOWIP_133C_ISOLATED_TEST";

fn enter_isolated_test(test_name: &str) -> bool {
    if std::env::var_os("NEXTEST").is_some()
        || std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test")
    {
        return true;
    }

    if std::env::var(ISOLATED_TEST_ENV).as_deref() == Ok(test_name) {
        return true;
    }

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", test_name, "--nocapture"])
        .env(ISOLATED_TEST_ENV, test_name)
        .status()
        .expect("start isolated replay test process");
    assert!(status.success(), "isolated replay test failed: {test_name}");
    false
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicUsize>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if HttpFixtureEvent::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

async fn spawn_http_fixture(
    expected_requests: usize,
) -> (Url, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind composed HTTP fixture");
    let address = listener.local_addr().expect("fixture address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let observed = request_count.clone();
    let task = tokio::spawn(async move {
        for _ in 0..expected_requests {
            let (mut stream, _) = listener.accept().await.expect("accept HTTP request");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            loop {
                let read = stream.read(&mut chunk).await.expect("read HTTP request");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            observed.fetch_add(1, Ordering::SeqCst);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                )
                .await
                .expect("write HTTP response");
        }
    });
    (
        Url::parse(&format!("http://{address}/events")).expect("fixture URL"),
        request_count,
        task,
    )
}

async fn spawn_http_observer() -> (
    Url,
    Arc<AtomicUsize>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind composed HTTP observer");
    let address = listener.local_addr().expect("observer address");
    let request_count = Arc::new(AtomicUsize::new(0));
    let observed = request_count.clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.expect("accept observed HTTP request");
                    observed.fetch_add(1, Ordering::SeqCst);
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                        )
                        .await
                        .expect("write observed HTTP response");
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });
    (
        Url::parse(&format!("http://{address}/events")).expect("observer URL"),
        request_count,
        shutdown_tx,
        task,
    )
}

#[derive(Clone, Default)]
struct BlockingSetupGate(Arc<(Mutex<bool>, Condvar)>);

impl BlockingSetupGate {
    fn wait(&self) {
        let (lock, wake) = &*self.0;
        let released = lock.lock().expect("lock setup gate");
        drop(
            wake.wait_while(released, |released| !*released)
                .expect("wait for setup release"),
        );
    }

    fn release(&self) {
        let (lock, wake) = &*self.0;
        *lock.lock().expect("lock setup gate") = true;
        wake.notify_all();
    }

    fn release_after(&self, timeout: Duration) -> std::thread::JoinHandle<bool> {
        let gate = self.clone();
        std::thread::spawn(move || {
            let (lock, wake) = &*gate.0;
            let released = lock.lock().expect("lock setup watchdog gate");
            let (mut released, _) = wake
                .wait_timeout_while(released, timeout, |released| !*released)
                .expect("wait on setup watchdog gate");
            let forced_release = !*released;
            if forced_release {
                *released = true;
                wake.notify_all();
            }
            forced_release
        })
    }
}

fn counted_client(initializations: Arc<AtomicUsize>) -> ReqwestHttpClient {
    ReqwestHttpClient::with_initializer(move || {
        initializations.fetch_add(1, Ordering::SeqCst);
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|error| HttpClientError::Transport(error.to_string()))
    })
}

fn counted_client_after_resume_marker(
    initializations: Arc<AtomicUsize>,
    journal_base: PathBuf,
) -> ReqwestHttpClient {
    ReqwestHttpClient::with_initializer(move || {
        let run_dir = latest_run_dir(&journal_base);
        let manifest: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(run_dir.join("run_manifest.json"))
                .expect("read resumed run manifest before client initialization"),
        )
        .expect("parse resumed run manifest before client initialization");
        let system_journal = manifest["system_journal_file"]
            .as_str()
            .expect("resumed run system journal path");
        let system_bytes = std::fs::read(run_dir.join(system_journal))
            .expect("read resumed system journal before client initialization");
        const RESUMED_LIVE_FRAME: &[u8] = b"\"replay_event\":\"resumed_live\"";
        assert!(
            system_bytes
                .windows(RESUMED_LIVE_FRAME.len())
                .any(|window| window == RESUMED_LIVE_FRAME),
            "the resume boundary must be durably recorded before HTTP client initialization"
        );

        initializations.fetch_add(1, Ordering::SeqCst);
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|error| HttpClientError::Transport(error.to_string()))
    })
}

fn decoder(url: Url) -> impl obzenflow_adapters::sources::PullDecoder<Item = HttpFixtureEvent> {
    let sequence = Arc::new(AtomicUsize::new(0));
    simple_poll(HttpFixtureEvent::EVENT_TYPE, url, move |_response| {
        Ok(vec![HttpFixtureEvent {
            sequence: sequence.fetch_add(1, Ordering::SeqCst) + 1,
        }])
    })
}

fn pull_flow(
    journal_base: PathBuf,
    url: Url,
    client: ReqwestHttpClient,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let config = HttpPullConfig::builder()
            .client(Arc::new(client))
            .poll_timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| {
                obzenflow_dsl::dsl::FlowBuildError::StageResourcesFailed(error.to_string())
            })?;
        let source = HttpPullSource::new(decoder(url), config);
        let collected = CountingSink { delivered };

        Ok(flow! {
            name: "flowip_133c_http_pull",
            journals: crate::journal::disk_journals(journal_base),

            stages: {
                src = async_source!(HttpFixtureEvent => source);
                collected = sink!(HttpFixtureEvent => collected);
            },

            topology: {
                src |> collected;
            }
        })
    })
}

fn poll_flow(
    journal_base: PathBuf,
    url: Url,
    client: ReqwestHttpClient,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let config = HttpPollConfig::builder()
            .client(Arc::new(client))
            .poll_interval(Duration::from_millis(250))
            .poll_timeout(Duration::from_secs(5))
            .retry(HttpRetryConfig {
                transient_max_retries: 0,
                ..HttpRetryConfig::default()
            })
            .build()
            .map_err(|error| {
                obzenflow_dsl::dsl::FlowBuildError::StageResourcesFailed(error.to_string())
            })?;
        let source = HttpPollSource::new(decoder(url), config);
        let collected = CountingSink { delivered };

        Ok(flow! {
            name: "flowip_133c_http_poll",
            journals: crate::journal::disk_journals(journal_base),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(1)
                .stall_timeout_ms(10_000),

            stages: {
                src = async_infinite_source!(HttpFixtureEvent => source);
                collected = sink!(HttpFixtureEvent => collected);
            },

            topology: {
                src |> collected;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut entries = std::fs::read_dir(base.join("flows"))
        .expect("flow archive directory")
        .map(|entry| entry.expect("flow archive entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    entries.sort();
    entries.pop().expect("flow archive")
}

async fn bootstrap_from(
    archive_path: PathBuf,
    verb: ReplayVerb,
    allow_incomplete_archive: bool,
) -> BootstrapConfig {
    let archive = crate::journal::disk::replay_archive::DiskReplayArchive::open(
        archive_path.clone(),
        allow_incomplete_archive,
    )
    .await
    .expect("open replay archive");
    BootstrapConfig {
        replay: Some(ReplayBootstrap {
            archive_path,
            allow_incomplete_archive,
            allow_duplicate_sink_delivery: false,
            verb,
        }),
        replay_archive: Some(Arc::new(archive)),
        ..BootstrapConfig::default()
    }
}

async fn build_flow(definition: FlowDefinition) -> FlowHandle {
    definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .unwrap_or_else(|failure| panic!("flow failed to build: {failure:?}"))
}

async fn wait_for_running(handle: &FlowHandle) {
    let mut state = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(*state.borrow(), PipelineState::Running) {
                break;
            }
            state.changed().await.expect("pipeline state channel");
        }
    })
    .await
    .expect("pipeline reaches Running");
}

async fn wait_for_count(counter: &AtomicUsize, target: usize, label: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timeout waiting for {label}={target}; observed {}",
            counter.load(Ordering::SeqCst)
        )
    });
}

async fn stop_flow(handle: FlowHandle) {
    handle.stop().await.expect("stop flow");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("flow stops before timeout")
        .expect("flow stops cleanly");
}

async fn wait_for_completion(handle: FlowHandle) {
    tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .expect("flow completes before timeout")
        .expect("flow completes cleanly");
}

async fn source_events(run_dir: &Path) -> Vec<ChainEvent> {
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("read run manifest"),
    )
    .expect("parse run manifest");
    let relative = manifest["stages"]["src"]["data_journal_file"]
        .as_str()
        .expect("source journal path");
    let journal = crate::journal::DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(relative),
        JournalOwner::stage(StageId::new()),
    )
    .expect("open source journal");
    journal
        .read_causally_ordered()
        .await
        .expect("read source journal")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

#[tokio::test]
async fn source_retry_observes_one_cached_initialization_failure() {
    let initializations = Arc::new(AtomicUsize::new(0));
    let init_count = initializations.clone();
    let client = ReqwestHttpClient::with_initializer(move || {
        init_count.fetch_add(1, Ordering::SeqCst);
        Err(HttpClientError::Transport(
            "synthetic source initialization failure".to_string(),
        ))
    });
    let config = HttpPullConfig::builder()
        .client(Arc::new(client))
        .retry(HttpRetryConfig {
            transient_max_retries: 2,
            transient_backoff: vec![Duration::ZERO, Duration::ZERO],
            ..HttpRetryConfig::default()
        })
        .build()
        .expect("HTTP pull config");
    let url = Url::parse("http://127.0.0.1:9/never-sent").expect("test URL");
    let mut source = HttpPullSource::new(decoder(url), config);

    let mut terminal_error = None;
    for _ in 0..12 {
        match source.next().await {
            Ok(_) => {}
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        }
    }

    let error = terminal_error.expect("source retry eventually exhausts");
    assert!(error
        .to_string()
        .contains("synthetic source initialization failure"));
    assert_eq!(
        initializations.load(Ordering::SeqCst),
        1,
        "logical source retry must not repeat physical client initialization"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_during_initialization_cancels_the_waiter_before_request_send() {
    const TEST_NAME: &str = concat!(
        "http_client::reqwest_client::replay_tests::",
        "abort_during_initialization_cancels_the_waiter_before_request_send"
    );
    if !enter_isolated_test(TEST_NAME) {
        return;
    }

    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let initializations = Arc::new(AtomicUsize::new(0));
    let completed_initializations = Arc::new(AtomicUsize::new(0));
    let init_count = initializations.clone();
    let completion_count = completed_initializations.clone();
    let gate = BlockingSetupGate::default();
    let setup_watchdog = gate.release_after(Duration::from_secs(10));
    let initializer_gate = gate.clone();
    let client = ReqwestHttpClient::with_initializer(move || {
        init_count.fetch_add(1, Ordering::SeqCst);
        initializer_gate.wait();
        let result = reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|error| HttpClientError::Transport(error.to_string()));
        completion_count.fetch_add(1, Ordering::SeqCst);
        result
    });
    let delivered = Arc::new(AtomicUsize::new(0));
    let (url, requests, observer_shutdown, observer) = spawn_http_observer().await;
    let running = build_flow(poll_flow(journal_base, url, client, delivered.clone())).await;
    wait_for_running(&running).await;
    wait_for_count(initializations.as_ref(), 1, "client initialization starts").await;

    let mut stopping = tokio::spawn(stop_flow(running));
    let stopped_while_setup_was_blocked =
        match tokio::time::timeout(Duration::from_secs(2), &mut stopping).await {
            Ok(result) => {
                result.expect("stop task completes cleanly");
                true
            }
            Err(_) => false,
        };

    if !stopped_while_setup_was_blocked {
        gate.release();
        stopping.await.expect("stop task after emergency release");
        setup_watchdog.join().expect("setup watchdog");
        let _ = observer_shutdown.send(());
        observer.await.expect("HTTP observer task");
        panic!("Abort must cancel the setup waiter without waiting for physical setup");
    }

    let completed_while_blocked = completed_initializations.load(Ordering::SeqCst);
    let requests_while_blocked = requests.load(Ordering::SeqCst);
    let delivered_while_blocked = delivered.load(Ordering::SeqCst);

    gate.release();
    assert!(
        !setup_watchdog.join().expect("setup watchdog"),
        "the test watchdog must not be what released physical setup"
    );
    wait_for_count(
        completed_initializations.as_ref(),
        1,
        "physical client initialization completes",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(completed_while_blocked, 0);
    assert_eq!(requests_while_blocked, 0);
    assert_eq!(delivered_while_blocked, 0);
    assert_eq!(initializations.load(Ordering::SeqCst), 1);
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    assert_eq!(delivered.load(Ordering::SeqCst), 0);

    observer_shutdown.send(()).expect("stop HTTP observer");
    observer.await.expect("HTTP observer task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_replay_keeps_the_default_source_client_cold() {
    const TEST_NAME: &str = concat!(
        "http_client::reqwest_client::replay_tests::",
        "strict_replay_keeps_the_default_source_client_cold"
    );
    if !enter_isolated_test(TEST_NAME) {
        return;
    }

    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");

    let live_initializations = Arc::new(AtomicUsize::new(0));
    let live_delivered = Arc::new(AtomicUsize::new(0));
    let (live_url, live_requests, live_fixture) = spawn_http_fixture(1).await;
    let live = build_flow(pull_flow(
        journal_base.clone(),
        live_url,
        counted_client(live_initializations.clone()),
        live_delivered.clone(),
    ))
    .await;
    wait_for_completion(live).await;
    live_fixture.await.expect("live HTTP fixture");
    assert_eq!(live_initializations.load(Ordering::SeqCst), 1);
    assert_eq!(live_requests.load(Ordering::SeqCst), 1);
    assert_eq!(live_delivered.load(Ordering::SeqCst), 1);
    let live_archive = latest_run_dir(&journal_base);

    let replay_initializations = Arc::new(AtomicUsize::new(0));
    let replay_delivered = Arc::new(AtomicUsize::new(0));
    let (replay_url, replay_requests, replay_fixture) = spawn_http_fixture(0).await;
    {
        let _bootstrap = install_bootstrap_config(
            bootstrap_from(live_archive.clone(), ReplayVerb::Replay, false).await,
        );
        let replay = build_flow(pull_flow(
            journal_base.clone(),
            replay_url,
            counted_client(replay_initializations.clone()),
            replay_delivered.clone(),
        ))
        .await;
        wait_for_completion(replay).await;
    }
    replay_fixture.await.expect("replay HTTP fixture");

    assert_eq!(replay_initializations.load(Ordering::SeqCst), 0);
    assert_eq!(replay_requests.load(Ordering::SeqCst), 0);
    assert_eq!(replay_delivered.load(Ordering::SeqCst), 1);
    let replay_archive = latest_run_dir(&journal_base);
    assert_ne!(live_archive, replay_archive);

    let verification = crate::verify::verify_run_dirs(
        &live_archive,
        &replay_archive,
        &crate::verify::VerifyOptions {
            write_report: false,
            ..crate::verify::VerifyOptions::default()
        },
    )
    .expect("verify live and replay archives");
    assert_eq!(
        verification.exit_code(),
        0,
        "{}",
        crate::verify::render_verdict(&verification)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_initializes_only_after_the_recorded_prefix_crosses_continue_live() {
    const TEST_NAME: &str = concat!(
        "http_client::reqwest_client::replay_tests::",
        "resume_initializes_only_after_the_recorded_prefix_crosses_continue_live"
    );
    if !enter_isolated_test(TEST_NAME) {
        return;
    }

    const RECORDED: usize = 2;

    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live_initializations = Arc::new(AtomicUsize::new(0));
    let live_delivered = Arc::new(AtomicUsize::new(0));
    let (live_url, live_requests, live_fixture) = spawn_http_fixture(RECORDED).await;
    let live = build_flow(poll_flow(
        journal_base.clone(),
        live_url,
        counted_client(live_initializations.clone()),
        live_delivered.clone(),
    ))
    .await;
    wait_for_running(&live).await;
    wait_for_count(live_delivered.as_ref(), RECORDED, "live deliveries").await;
    stop_flow(live).await;
    live_fixture.await.expect("live poll fixture");
    assert_eq!(live_initializations.load(Ordering::SeqCst), 1);
    assert_eq!(live_requests.load(Ordering::SeqCst), RECORDED);
    let live_archive = latest_run_dir(&journal_base);

    let resume_initializations = Arc::new(AtomicUsize::new(0));
    let resume_delivered = Arc::new(AtomicUsize::new(0));
    let (resume_url, resume_requests, resume_fixture) = spawn_http_fixture(1).await;
    {
        let _bootstrap = install_bootstrap_config(
            bootstrap_from(live_archive.clone(), ReplayVerb::Resume, true).await,
        );
        let resume_client = counted_client_after_resume_marker(
            resume_initializations.clone(),
            journal_base.clone(),
        );
        assert!(!resume_client.is_initialized());
        let resumed = build_flow(poll_flow(
            journal_base.clone(),
            resume_url,
            resume_client,
            resume_delivered.clone(),
        ))
        .await;
        wait_for_running(&resumed).await;
        wait_for_count(
            resume_delivered.as_ref(),
            RECORDED + 1,
            "resume prefix plus live delivery",
        )
        .await;
        wait_for_count(
            resume_initializations.as_ref(),
            1,
            "resume client initializations",
        )
        .await;
        wait_for_count(resume_requests.as_ref(), 1, "resume HTTP requests").await;
        stop_flow(resumed).await;
    }
    resume_fixture.await.expect("resume poll fixture");

    assert_eq!(resume_initializations.load(Ordering::SeqCst), 1);
    assert_eq!(resume_requests.load(Ordering::SeqCst), 1);
    let resumed_archive = latest_run_dir(&journal_base);
    let events = source_events(&resumed_archive).await;
    assert_eq!(
        events.iter().filter(|event| event.is_data()).count(),
        RECORDED + 1
    );
    assert!(events.iter().any(|event| {
        matches!(
            &event.content,
            ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete { .. })
        )
    }));
}
