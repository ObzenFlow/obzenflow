// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

fn isolated(name: &str) -> bool {
    let name = name.strip_prefix("obzenflow_infra::").unwrap_or(name);
    const MARKER: &str = "OBZENFLOW_CONSOLE_SETUP_TEST";
    if std::env::var(MARKER).as_deref() == Ok(name) {
        return true;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(MARKER, name)
        .env_remove("TOKIO_CONSOLE_BIND")
        .env_remove("TOKIO_CONSOLE_RECORD_PATH")
        .status()
        .expect("start isolated Console setup test");
    assert!(status.success(), "isolated test failed: {name}");
    false
}

#[test]
fn ordinary_tracing_accepts_an_existing_subscriber() {
    let name = concat!(
        module_path!(),
        "::ordinary_tracing_accepts_an_existing_subscriber"
    );
    if !isolated(name) {
        return;
    }
    tracing_subscriber::registry().try_init().unwrap();
    assert!(PreparedObservability::install(
        false,
        Some("invalid and ignored"),
        EnvFilter::new("info")
    )
    .is_ok());
}

#[cfg(not(feature = "tokio-console"))]
#[test]
fn console_request_without_feature_fails() {
    assert!(matches!(
        PreparedObservability::install(true, None, EnvFilter::new("info")),
        Err(ApplicationError::FeatureNotEnabled(feature)) if feature == "tokio-console"
    ));
}

#[cfg(all(feature = "tokio-console", not(tokio_unstable)))]
#[test]
fn console_request_without_unstable_fails() {
    assert!(matches!(
        PreparedObservability::install(true, None, EnvFilter::new("info")),
        Err(ApplicationError::InvalidConfiguration(message)) if message.contains("tokio_unstable")
    ));
}

#[cfg(feature = "tokio-console")]
#[test]
fn console_address_validation_and_precedence() {
    use std::env::VarError;
    assert_eq!(
        console_address(Some("127.0.0.1:1234"), Ok("127.0.0.1:5678".into())).unwrap(),
        "127.0.0.1:5678".parse::<std::net::SocketAddr>().unwrap()
    );
    assert_eq!(
        console_address(Some("127.0.0.1:1234"), Err(VarError::NotPresent)).unwrap(),
        "127.0.0.1:1234".parse::<std::net::SocketAddr>().unwrap()
    );
    assert!(matches!(
        console_address(None, Ok("not an address".into())),
        Err(ApplicationError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        console_address(None, Err(VarError::NotUnicode(std::ffi::OsString::new()))),
        Err(ApplicationError::InvalidConfiguration(_))
    ));
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
#[test]
fn console_setup_reports_bind_and_subscriber_errors() {
    let name = concat!(
        module_path!(),
        "::console_setup_reports_bind_and_subscriber_errors"
    );
    if !isolated(name) {
        return;
    }
    assert!(matches!(
        PreparedObservability::install(true, Some("invalid"), EnvFilter::new("info")),
        Err(ApplicationError::InvalidConfiguration(_))
    ));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    assert!(matches!(
        PreparedObservability::install(true, Some(&address.to_string()), EnvFilter::new("info")),
        Err(ApplicationError::ServerStartFailed(_))
    ));
    // Neither failed setup installed a subscriber or leaked its retained socket.
    tracing_subscriber::registry().try_init().unwrap();
    drop(listener);
    let error =
        PreparedObservability::install(true, Some(&address.to_string()), EnvFilter::new("info"))
            .err()
            .expect("an explicit request must reject a subscriber conflict");
    assert!(error
        .to_string()
        .contains("could not install its tracing subscriber"));
    let _rebound = std::net::TcpListener::bind(address).unwrap();
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
#[test]
fn console_recording_is_rejected_before_binding_or_creating_a_file() {
    let name = concat!(
        module_path!(),
        "::console_recording_is_rejected_before_binding_or_creating_a_file"
    );
    if !isolated(name) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let recording = directory.path().join("console.json");
    std::env::set_var("TOKIO_CONSOLE_RECORD_PATH", &recording);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // An attempted bind would report ServerStartFailed for this occupied port.
    assert!(matches!(
        PreparedObservability::install(true, Some(&address.to_string()), EnvFilter::new("info")),
        Err(ApplicationError::InvalidConfiguration(message))
            if message.contains("Managed Tokio Console recording is unsupported")
    ));
    assert!(
        !recording.exists(),
        "unsupported recording must not create a file"
    );
    // Rejection also precedes global subscriber installation.
    tracing_subscriber::registry().try_init().unwrap();
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
fn start_console() -> (console_subscriber::ConsoleLayer, ApplicationDiagnostics) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let (layer, server) = console_subscriber::ConsoleLayer::builder()
        .server_addr(listener.local_addr().unwrap())
        .build();
    (layer, PreparedConsole { listener, server }.start().unwrap())
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
async fn connect_idle_client(diagnostics: &ApplicationDiagnostics) -> tokio::net::TcpStream {
    let stream = tokio::net::TcpStream::connect(diagnostics.address().unwrap())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while diagnostics.connections.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server accepted idle client");
    stream
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
async fn assert_client_closed(mut client: tokio::net::TcpStream) {
    use tokio::io::AsyncReadExt;
    let mut buffered = Vec::new();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        client.read_to_end(&mut buffered),
    )
    .await
    .expect("server closed idle client without client participation");
    // Tonic may already have buffered HTTP/2 settings. EOF or a reset proves
    // closure after draining those bytes; no client read assisted owner.finish.
    if let Err(error) = result {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::BrokenPipe
            ),
            "unexpected read failure: {error}"
        );
    }
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_owner_joins_both_tasks_and_closes_idle_clients() {
    for early_failure in [false, true] {
        let (_layer, diagnostics) = start_console();
        let address = diagnostics.address().unwrap();
        let aborts: Vec<_> = diagnostics
            .tasks
            .iter()
            .map(|task| task.0.abort_handle())
            .collect();
        assert_eq!(
            aborts.len(),
            2,
            "both aggregator and server must be retained"
        );
        let connections = diagnostics.connections.clone();
        let client = connect_idle_client(&diagnostics).await;
        let outcome = if early_failure {
            Err(ApplicationError::InvalidConfiguration(
                "test setup failure".into(),
            ))
        } else {
            Ok(())
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            diagnostics.finish(outcome),
        )
        .await
        .expect("diagnostics terminated with an idle connected client");
        assert_eq!(result.is_err(), early_failure);
        assert!(aborts.iter().all(tokio::task::AbortHandle::is_finished));
        assert_eq!(connections.len(), 0, "all accepted transport IO released");
        let _rebound = std::net::TcpListener::bind(address).unwrap();
        assert_client_closed(client).await;
    }
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_owner_drop_terminates_tasks_and_idle_clients() {
    let (_layer, diagnostics) = start_console();
    let address = diagnostics.address().unwrap();
    let aborts: Vec<_> = diagnostics
        .tasks
        .iter()
        .map(|task| task.0.abort_handle())
        .collect();
    let connections = diagnostics.connections.clone();
    let client = connect_idle_client(&diagnostics).await;
    drop(diagnostics);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !aborts.iter().all(tokio::task::AbortHandle::is_finished) {
            tokio::task::yield_now().await;
        }
        connections.wait_closed().await;
    })
    .await
    .expect("dropped owner terminated tasks and IO while caller runtime remained alive");
    let _rebound = std::net::TcpListener::bind(address).unwrap();
    assert_client_closed(client).await;
}

#[cfg(all(feature = "tokio-console", tokio_unstable))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_owner_closes_a_watch_stream_with_no_receive_window() {
    use console_api::instrument::{instrument_client::InstrumentClient, InstrumentRequest};

    let (_layer, diagnostics) = start_console();
    let address = diagnostics.address().unwrap();
    let connections = diagnostics.connections.clone();
    let aborts: Vec<_> = diagnostics
        .tasks
        .iter()
        .map(|task| task.0.abort_handle())
        .collect();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .initial_stream_window_size(0u32)
        .connect()
        .await
        .unwrap();
    let mut client = InstrumentClient::new(channel);
    // Response headers do not consume the HTTP/2 DATA window. Keep the stream
    // alive and unread: queued updates cannot drain during graceful shutdown.
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        client.watch_updates(InstrumentRequest {}),
    )
    .await
    .expect("Console accepted the zero-window WatchUpdates request")
    .unwrap()
    .into_inner();
    assert_eq!(connections.len(), 1);
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        diagnostics.finish(Ok(())),
    )
    .await
    .expect("stalled client cannot prolong diagnostics shutdown")
    .unwrap();
    assert!(aborts.iter().all(tokio::task::AbortHandle::is_finished));
    assert_eq!(connections.len(), 0);
    let _rebound = std::net::TcpListener::bind(address).unwrap();
    drop(stream);
    drop(client);
}
