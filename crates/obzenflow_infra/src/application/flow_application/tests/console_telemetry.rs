// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exercise the Console protocol against the application's real supervisors.

use super::*;
use console_api::instrument::{instrument_client::InstrumentClient, InstrumentRequest, Update};
use console_api::tasks::{Stats, Task};
use console_api::{field, Metadata};
use obzenflow_dsl::{async_source, stateful, transform};
use obzenflow_runtime::stages::common::handlers::TypedAsyncFiniteSourceHandler;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

const SOURCE_TASK: &str = "async_finite_source_console_source";
const TRANSFORM_TASK: &str = "transform_console_transform";
const COUNTER_TASK: &str = "stateful_console_counter";
const SUPERVISORS: [&str; 6] = [
    "pipeline_supervisor",
    SOURCE_TASK,
    TRANSFORM_TASK,
    COUNTER_TASK,
    "sink_console_sink",
    "metrics_aggregator",
];

// libtest can run many tests in one process. Keep the global subscriber and its
// ordinary-info filter isolated using the existing native subprocess pattern.
fn isolated(test: &str) -> bool {
    const CHILD: &str = "OBZENFLOW_CONSOLE_TELEMETRY_TEST";
    if std::env::var(CHILD).as_deref() == Ok(test) {
        return true;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(CHILD, test)
        .env("RUST_LOG", "info")
        .env("TOKIO_CONSOLE_PUBLISH_INTERVAL", "20ms")
        .env_remove("TOKIO_CONSOLE_BIND")
        .status()
        .expect("start native Console telemetry test process");
    assert!(status.success(), "Console telemetry proof failed: {test}");
    false
}

#[test]
fn console_stream_reports_real_supervisor_activity_and_closes_on_completion() {
    if isolated(concat!(
        "application::flow_application::tests::console_telemetry::",
        "console_stream_reports_real_supervisor_activity_and_closes_on_completion"
    )) {
        telemetry_proof(false);
    }
}

#[test]
fn console_stream_reports_real_supervisor_activity_and_closes_on_cancellation() {
    if isolated(concat!(
        "application::flow_application::tests::console_telemetry::",
        "console_stream_reports_real_supervisor_activity_and_closes_on_cancellation"
    )) {
        telemetry_proof(true);
    }
}

#[test]
fn console_configuration_failure_precedes_subscriber_and_listener() {
    if !isolated(concat!(
        "application::flow_application::tests::console_telemetry::",
        "console_configuration_failure_precedes_subscriber_and_listener"
    )) {
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("invalid-console-application.toml");
            std::fs::write(&config, "[server\n").unwrap();
            let (bound_tx, bound_rx) = oneshot::channel();
            let mut app = FlowApplication::builder()
                .with_config_file(config)
                .with_cli_args([
                    "console-config-failure-proof",
                    "--tokio-console",
                    "--tokio-console-bind",
                    "127.0.0.1:0",
                ])
                .with_log_level(LogLevel::Info);
            app.test_console_bound_address = Some(bound_tx);
            let definition = FlowDefinition::materialize(|_| {
                panic!("invalid configuration must be rejected before flow construction")
            });
            let result = tokio::time::timeout(Duration::from_secs(5), app.run_async(definition))
                .await
                .expect("early configuration failure must join diagnostic cleanup");
            assert!(
                matches!(result, Err(ApplicationError::InvalidConfiguration(_))),
                "the original configuration error must remain the result: {result:?}"
            );
            assert!(
                bound_rx.await.is_err(),
                "invalid config must not start Console"
            );
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .try_init()
                .expect("invalid config must not install a subscriber");
            assert_eq!(tokio::spawn(async { 7 }).await.unwrap(), 7);
        });
}

#[test]
fn console_uses_prepared_config_and_closes_after_build_failure() {
    if !isolated(concat!(
        "application::flow_application::tests::console_telemetry::",
        "console_uses_prepared_config_and_closes_after_build_failure"
    )) {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("console.toml");
    std::fs::write(
        &config,
        "[diagnostics.tokio_console]\nenabled = true\nbind = '127.0.0.1:0'\n",
    )
    .unwrap();
    // A shadowed legacy value must not be parsed again by Console's builder.
    std::env::set_var("TOKIO_CONSOLE_BIND", "invalid lower-precedence address");
    let (bound_tx, bound_rx) = oneshot::channel();
    let mut params = LaunchParams {
        builder_config_file: Some(config.clone()),
        cli_args: Some(vec!["console-once-proof".into()]),
        test_console_bound_address: Some(bound_tx),
        ..LaunchParams::default()
    };
    let prepared = params.prepare().unwrap();
    std::fs::write(config, "invalid configuration after preparation").unwrap();
    let definition = FlowDefinition::materialize(|_| {
        Err(obzenflow_dsl::FlowBuildError::BindingConfiguration {
            binding: "controlled-build-failure".into(),
            detail: "after diagnostics started".into(),
        })
    });
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let result = FlowApplication::launch_prepared(definition, params, prepared).await;
            assert!(
                matches!(result, Err(ApplicationError::FlowBuildFailed(message))
                if message.contains("controlled-build-failure"))
            );
            let address = bound_rx
                .await
                .expect("diagnostics acquired before flow build");
            let _rebound = TcpListener::bind(address).expect("failed build releases Console");
        });
}

#[test]
fn blocking_console_disk_flow_preserves_filtering_and_completed_journal() {
    const TEST: &str = concat!(
        "application::flow_application::tests::console_telemetry::",
        "blocking_console_disk_flow_preserves_filtering_and_completed_journal"
    );
    const CHILD: &str = "OBZENFLOW_BLOCKING_CONSOLE_TEST";
    if std::env::var(CHILD).as_deref() != Ok(TEST) {
        let capture = tempfile::tempdir().unwrap();
        let output_path = capture.path().join("console-child.log");
        let output = std::fs::File::create(&output_path).unwrap();
        // Keep the deadline outside the product runtime. A failed hook before
        // manual start must not leave the host waiting indefinitely for Play.
        // File capture also lets a noisy failure exit without filling a pipe.
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env(CHILD, TEST)
                    .env("RUST_LOG", "info")
                    .env("TOKIO_CONSOLE_PUBLISH_INTERVAL", "20ms")
                    .env_remove("TOKIO_CONSOLE_BIND")
                    .stdin(std::process::Stdio::null())
                    .stdout(output.try_clone().unwrap())
                    .stderr(output)
                    .kill_on_drop(true)
                    .spawn()
                    .expect("start native blocking Console regression");
                let result = tokio::time::timeout(Duration::from_secs(20), child.wait()).await;
                if result.is_err() {
                    child
                        .kill()
                        .await
                        .expect("terminate and reap timed-out Console test child");
                }
                result
            });
        let output_text = std::fs::read_to_string(output_path).unwrap();
        let status = result
            .unwrap_or_else(|_| panic!("Console test child exceeded its deadline: {output_text}"))
            .expect("reap native Console test child");
        assert!(status.success(), "{output_text}");
        assert!(output_text.contains("blocking_console_info_visible"));
        assert!(
            !output_text.contains("runtime::resource::poll_op")
                && !output_text.contains("blocking_console_trace_hidden")
                && !output_text.contains("Span not found"),
            "Console must preserve the ordinary-info formatter under real disk work: {output_text}"
        );
        return;
    }

    // Unlike run_async, the product's run_blocking path installs tracing before
    // constructing its runtime and all traced Tokio I/O resources. There must
    // be no caller runtime here to hide that ordering difference.
    const INPUTS: usize = 32;
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("blocking-console.toml");
    std::fs::write(
        &config,
        "[server]\nenabled = true\nhost = '127.0.0.1'\nport = 8080\nstartup_mode = 'manual'\non_terminal = 'exit'\n[metrics]\nenabled = true\n",
    )
    .unwrap();
    let journal_root = dir.path().join("journals");
    let (source_input, input) = mpsc::channel(1);
    let (entered, source_entered) = mpsc::unbounded_channel();
    let source = ControlledSource {
        input: Arc::new(tokio::sync::Mutex::new(input)),
        entered,
    };
    let (console_tx, console_rx) = oneshot::channel();
    let driver = Mutex::new(Some((console_rx, source_input, source_entered)));
    let (proved_tx, proved_rx) = std::sync::mpsc::sync_channel(1);
    let mut app = FlowApplication::builder()
        .with_config_file(config)
        .with_cli_args([
            "blocking-console-proof",
            "--server-port",
            "0",
            "--tokio-console",
            "--tokio-console-bind",
            "127.0.0.1:0",
        ])
        .with_log_level(LogLevel::Info)
        .with_flow_handle_hook(move |flow| {
            let (console_rx, source_input, mut source_entered) =
                driver.lock().unwrap().take().expect("one flow driver");
            let flow = flow.clone();
            let proved_tx = proved_tx.clone();
            tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    let address = console_rx.await.expect("Console listener started");
                    flow.wait_for_ready().await.unwrap();
                    let mut client = InstrumentClient::connect(format!("http://{address}"))
                        .await
                        .unwrap();
                    let mut stream = client
                        .watch_updates(InstrumentRequest {})
                        .await
                        .unwrap()
                        .into_inner();
                    let mut telemetry = Telemetry::default();
                    telemetry
                        .until(
                            &mut stream,
                            "blocking host exposes real stage tasks",
                            |view| {
                                SUPERVISORS[..5]
                                    .iter()
                                    .all(|name| view.polls(name).is_some_and(|polls| polls > 0))
                            },
                        )
                        .await;
                    flow.start().await.unwrap();
                    source_entered.recv().await.expect("source awaits input");
                    telemetry
                        .until(&mut stream, "metrics starts with execution", |view| {
                            view.polls("metrics_aggregator")
                                .is_some_and(|polls| polls > 0)
                        })
                        .await;
                    tracing::info!("blocking_console_info_visible");
                    tracing::trace!("blocking_console_trace_hidden");
                    // Each protocol update supplies the next controlled input,
                    // repeatedly waking Console's receiver around actual disk
                    // publication and resource spans without timing sleeps.
                    for _ in 0..INPUTS {
                        let before = telemetry.polls(SOURCE_TASK).unwrap();
                        source_input.send(IdlePayload).await.unwrap();
                        source_entered
                            .recv()
                            .await
                            .expect("source published and awaits next input");
                        telemetry
                            .until(&mut stream, "disk-backed source polls advance", |view| {
                                view.polls(SOURCE_TASK).is_some_and(|polls| polls > before)
                            })
                            .await;
                    }
                    // Publish the proof inputs before EOF can trigger normal
                    // application cleanup, which owns and aborts hook tasks.
                    let run_path = flow.run_substrate().locator().unwrap().path().to_path_buf();
                    proved_tx.send((address, run_path)).unwrap();
                    drop(source_input);
                })
                .await
                .expect("bounded blocking Console protocol proof");
            })
        });
    app.test_console_bound_address = Some(console_tx);
    let definition = FlowDefinition::materialize(move |_| {
        let mapper = obzenflow_runtime::stages::transform::map(|event: IdlePayload| event);
        let counter = obzenflow_runtime::stages::stateful::reduce(
            IdlePayload,
            |_: &mut IdlePayload, _: &IdlePayload| {},
        )
        .emit_on_eof();
        let sink = NoopSink;
        Ok(flow! {
            name: "blocking_console_disk",
            journals: disk_journals(journal_root),
            stages: {
                console_source = async_source!(IdlePayload => source);
                console_transform = transform!(IdlePayload -> IdlePayload => mapper);
                console_counter = stateful!(IdlePayload -> IdlePayload => counter);
                console_sink = sink!(IdlePayload => sink);
            },
            topology: {
                console_source |> console_transform;
                console_transform |> console_counter;
                console_counter |> console_sink;
            }
        })
    });
    app.run_blocking(definition)
        .expect("the real blocking application completes");
    let (address, run_path) = proved_rx
        .try_recv()
        .expect("Console protocol proof completed before source EOF");
    let _rebound = TcpListener::bind(address).expect("blocking host releases Console listener");
    // The product runtime has now completed and been dropped. A separate
    // reader runtime cannot affect its subscriber/runtime construction order.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(assert_completed_disk_cohort(&run_path, INPUTS));
}

async fn assert_completed_disk_cohort(run_path: &std::path::Path, inputs: usize) {
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::{ChainPayload, PipelineLifecycleEvent, SystemPayload};
    use obzenflow_core::journal::read::{RunJournalKind, RunOutcome, RunRecordData, TailRead};
    use std::collections::BTreeMap;

    let mut snapshot = crate::journal::read::open_disk_run(run_path).await.unwrap();
    let mut facts = BTreeMap::new();
    let mut deliveries = 0;
    let mut completed = 0;
    while let Some(record) = snapshot.next().await.unwrap() {
        match record.record {
            RunRecordData::Chain(row) => match &row.payload {
                ChainPayload::Fact(_) => {
                    assert_eq!(record.journal.kind, RunJournalKind::Data);
                    assert!(row.envelope.provenance.event.processing.status.is_success());
                    *facts.entry(record.journal.stage.unwrap().key).or_insert(0) += 1;
                }
                ChainPayload::Delivery(receipt) => {
                    assert_eq!(record.journal.kind, RunJournalKind::Data);
                    assert_eq!(record.journal.stage.unwrap().key, "console_sink");
                    assert!(row.envelope.provenance.event.processing.status.is_success());
                    assert!(matches!(receipt.result, DeliveryResult::Success { .. }));
                    deliveries += 1;
                }
                _ => {}
            },
            RunRecordData::System(row) => match row.payload {
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed { .. }) => {
                    completed += 1;
                }
                SystemPayload::PipelineLifecycle(
                    PipelineLifecycleEvent::Failed { .. }
                    | PipelineLifecycleEvent::Cancelled { .. }
                    | PipelineLifecycleEvent::NotStarted,
                ) => panic!("blocking application did not commit a successful terminal outcome"),
                _ => {}
            },
        }
    }
    assert_eq!(
        facts,
        BTreeMap::from([
            ("console_source".to_string(), inputs),
            ("console_transform".to_string(), inputs),
            ("console_counter".to_string(), 1),
        ])
    );
    assert_eq!(deliveries, 1);
    assert_eq!(completed, 1);
    let mut tail = snapshot.into_tail();
    assert!(matches!(tail.read_next().await.unwrap(), TailRead::Pending));
    let progress = tail.progress();
    let outcome = progress.outcome.as_ref().expect("recorded run outcome");
    assert_eq!(outcome.outcome, RunOutcome::Completed);
    let settled = progress
        .settled_prefix
        .as_ref()
        .expect("terminal, drain and fresh journal-end observations settle the run");
    assert_eq!(settled.terminal_event_id, outcome.event_id);
    assert_eq!(Some(settled.drained_event_id), progress.drained_event_id);
    assert_eq!(settled.end_positions.len(), tail.journals().len());
    assert!(tail
        .journals()
        .all(|journal| settled.end_positions.contains_key(&journal.id)));
}

#[derive(Clone, Debug)]
struct ControlledSource {
    input: Arc<tokio::sync::Mutex<mpsc::Receiver<IdlePayload>>>,
    entered: mpsc::UnboundedSender<()>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for ControlledSource {
    type Output = IdlePayload;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.entered.send(()).expect("source observation is alive");
        Ok(self
            .input
            .lock()
            .await
            .recv()
            .await
            .map(|event| vec![event]))
    }
}

#[derive(Default)]
struct Telemetry {
    metadata: HashMap<u64, Metadata>,
    tasks: HashMap<u64, Task>,
    stats: HashMap<u64, Stats>,
    updates: usize,
}

impl Telemetry {
    fn apply(&mut self, update: Update) {
        self.updates += 1;
        if let Some(metadata) = update.new_metadata {
            for entry in metadata.metadata {
                self.metadata.insert(
                    entry.id.expect("metadata identity").id,
                    entry.metadata.expect("metadata payload"),
                );
            }
        }
        if let Some(tasks) = update.task_update {
            assert_eq!(tasks.dropped_events, 0, "incomplete Console task evidence");
            for task in tasks.new_tasks {
                self.tasks.insert(task.id.expect("task identity").id, task);
            }
            self.stats.extend(tasks.stats_update);
        }
        if let Some(resources) = update.resource_update {
            assert_eq!(resources.dropped_events, 0, "dropped resource evidence");
        }
        if let Some(operations) = update.async_op_update {
            assert_eq!(operations.dropped_events, 0, "dropped async-op evidence");
        }
    }

    fn task_name<'a>(&'a self, task: &'a Task) -> Option<&'a str> {
        task.fields.iter().find_map(|value| {
            let name = match value.name.as_ref()? {
                field::Name::StrName(name) => name.as_str(),
                field::Name::NameIdx(index) => {
                    let metadata = value.metadata_id.as_ref().or(task.metadata.as_ref())?;
                    self.metadata
                        .get(&metadata.id)?
                        .field_names
                        .get(*index as usize)?
                        .as_str()
                }
            };
            if name != "task.name" {
                return None;
            }
            match value.value.as_ref()? {
                field::Value::StrVal(name) => Some(name.as_str()),
                field::Value::DebugVal(name) => Some(name.trim_matches('"')),
                _ => None,
            }
        })
    }

    fn stats(&self, name: &str) -> Option<&Stats> {
        self.tasks.iter().find_map(|(id, task)| {
            (self.task_name(task) == Some(name))
                .then(|| self.stats.get(id))
                .flatten()
        })
    }

    fn polls(&self, name: &str) -> Option<u64> {
        Some(self.stats(name)?.poll_stats.as_ref()?.polls)
    }

    async fn until(
        &mut self,
        stream: &mut tonic::Streaming<Update>,
        reason: &str,
        predicate: impl Fn(&Self) -> bool,
    ) {
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            while !predicate(self) {
                let update = stream
                    .message()
                    .await
                    .expect("Console WatchUpdates stream")
                    .expect("Console closed before the observation completed");
                self.apply(update);
            }
        })
        .await;
        assert!(
            observed.is_ok(),
            "{reason}; updates={}, metadata={}, tasks={}, named tasks={:?}",
            self.updates,
            self.metadata.len(),
            self.tasks.len(),
            self.tasks
                .values()
                .filter_map(|task| self.task_name(task))
                .collect::<Vec<_>>()
        );
    }
}

fn telemetry_proof(cancel: bool) {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("console.toml");
            std::fs::write(
                &config,
                "[server]\nenabled = true\nhost = '127.0.0.1'\nport = 8080\nstartup_mode = 'manual'\non_terminal = 'exit'\n[metrics]\nenabled = true\n[diagnostics.tokio_console]\nenabled = true\nbind = '127.0.0.1:0'\n",
            )
            .unwrap();
            let (source_input, input) = mpsc::channel(1);
            let (entered, mut source_entered) = mpsc::unbounded_channel();
            let source = ControlledSource {
                input: Arc::new(tokio::sync::Mutex::new(input)),
                entered,
            };
            let (flow_tx, flow_rx) = oneshot::channel();
            let flow_tx = Mutex::new(Some(flow_tx));
            let (console_tx, console_rx) = oneshot::channel();
            let transformed = Arc::new(AtomicUsize::new(0));
            let accumulated = Arc::new(AtomicUsize::new(0));
            let transform_count = transformed.clone();
            let accumulator_count = accumulated.clone();
            let mut app = FlowApplication::builder()
                .with_config_file(config)
                .with_cli_args(["console-telemetry-proof", "--server-port", "0"])
                .with_log_level(LogLevel::Info)
                .with_flow_handle_hook(move |flow| {
                    assert!(flow_tx.lock().unwrap().take().unwrap().send(flow.clone()).is_ok());
                    tokio::spawn(async {})
                });
            app.test_console_bound_address = Some(console_tx);
            let definition = FlowDefinition::materialize(move |_| {
                let mapper = obzenflow_runtime::stages::transform::map(move |event: IdlePayload| {
                    transform_count.fetch_add(1, Ordering::SeqCst);
                    event
                });
                let counter = obzenflow_runtime::stages::stateful::reduce(
                    IdlePayload,
                    move |_: &mut IdlePayload, _: &IdlePayload| {
                        accumulator_count.fetch_add(1, Ordering::SeqCst);
                    },
                )
                .emit_on_eof();
                let sink = NoopSink;
                Ok(flow! {
                    name: "console_telemetry",
                    journals: crate::journal::memory_journals(),
                    stages: {
                        console_source = async_source!(IdlePayload => source);
                        console_transform = transform!(IdlePayload -> IdlePayload => mapper);
                        console_counter = stateful!(IdlePayload -> IdlePayload => counter);
                        console_sink = sink!(IdlePayload => sink);
                    },
                    topology: {
                        console_source |> console_transform;
                        console_transform |> console_counter;
                        console_counter |> console_sink;
                    }
                })
            });
            let application = tokio::spawn(app.run_async(definition));
            let address = console_rx.await.expect("retained Console listener address");
            let flow = flow_rx.await.expect("the real application flow");
            flow.wait_for_ready().await.unwrap();
            assert!(
                !tracing::enabled!(target: "obzenflow::console_filter_proof", tracing::Level::TRACE),
                "Console must not enable unrelated application trace work at RUST_LOG=info"
            );
            let mut client = InstrumentClient::connect(format!("http://{address}"))
                .await
                .expect("Console protocol connection");
            let mut stream = client
                .watch_updates(InstrumentRequest {})
                .await
                .expect("Console WatchUpdates admission")
                .into_inner();
            let mut telemetry = Telemetry::default();
            telemetry
                .until(&mut stream, "real supervisor tasks must be visible at info", |view| {
                    // Metrics is execution-owned and starts when consumers start.
                    SUPERVISORS[..5].iter().all(|name| view.polls(name).is_some_and(|polls| polls > 0))
                })
                .await;
            let pipeline_ready_polls = telemetry.polls("pipeline_supervisor").unwrap();
            flow.start().await.unwrap();
            source_entered.recv().await.expect("source is pending on controlled input");
            telemetry
                .until(&mut stream, "pipeline start must advance real task polls", |view| {
                    view.polls("pipeline_supervisor").is_some_and(|polls| polls > pipeline_ready_polls)
                        && view.polls("metrics_aggregator").is_some_and(|polls| polls > 0)
                        && view.stats(SOURCE_TASK).is_some_and(|stats| stats.dropped_at.is_none())
                })
                .await;
            let before = [SOURCE_TASK, TRANSFORM_TASK, COUNTER_TASK]
                .map(|name| telemetry.polls(name).unwrap());
            let pending_update = telemetry.updates;
            source_input.send(IdlePayload).await.unwrap();
            source_entered.recv().await.expect("source resumed and is pending again");
            telemetry
                .until(&mut stream, "resuming source work must advance its real stage polls", |view| {
                    [SOURCE_TASK, TRANSFORM_TASK, COUNTER_TASK]
                        .iter()
                        .zip(before)
                        .all(|(name, before)| view.polls(name).is_some_and(|polls| polls > before))
                        && transformed.load(Ordering::SeqCst) == 1
                        && accumulated.load(Ordering::SeqCst) == 1
                })
                .await;
            assert!(telemetry.updates > pending_update, "metadata alone cannot pass");
            for name in SUPERVISORS {
                let stats = telemetry.stats(name).unwrap();
                let polls = stats.poll_stats.as_ref().unwrap();
                assert!(stats.dropped_at.is_none(), "{name} must remain live");
                assert!(polls.first_poll.is_some() && polls.polls > 0);
                assert!(polls.busy_time.is_some(), "{name} has actual poll timing");
            }

            if cancel {
                application.abort();
                assert!(application.await.unwrap_err().is_cancelled());
            } else {
                drop(source_input);
                tokio::time::timeout(Duration::from_secs(5), application)
                    .await
                    .expect("application completes")
                    .unwrap()
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                while let Ok(Some(_)) = stream.message().await {}
            })
            .await
            .expect("application shutdown closes the active Console stream");
            let _rebound = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    match TcpListener::bind(address) {
                        Ok(listener) => break listener,
                        Err(error) if cancel && error.kind() == std::io::ErrorKind::AddrInUse => {
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("Console listener survived application shutdown: {error}"),
                    }
                }
            })
            .await
            .expect("owned Console listener is released on the still-live caller runtime");
            assert_eq!(tokio::spawn(async { 7 }).await.unwrap(), 7);
        });
}
