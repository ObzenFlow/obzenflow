// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

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
