// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::web::host_config::HostConfig;
use crate::web::warp::WarpWebHost;
use obzenflow_core::web::{
    EndpointError, HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, SseBody, SseFrame,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PendingEndpoint {
    sse: bool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl HttpEndpoint for PendingEndpoint {
    fn path(&self) -> &str {
        if self.sse {
            "/sse"
        } else {
            "/pending"
        }
    }
    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }
    async fn handle(&self, _: Request) -> Result<ManagedResponse, EndpointError> {
        let guard = Dropped(self.dropped.clone());
        if self.sse {
            let entered = self.entered.clone();
            let release = self.release.clone();
            Ok(ManagedResponse::Sse(SseBody::new(futures::stream::once(
                async move {
                    let _guard = guard;
                    entered.notify_one();
                    release.notified().await;
                    SseFrame::data("finished")
                },
            ))))
        } else {
            let _guard = guard;
            self.entered.notify_one();
            self.release.notified().await;
            Ok(Response::ok().with_text("finished").into())
        }
    }
}

struct Probe {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
}

fn add_pending(host: &mut WarpWebHost, sse: bool) -> Probe {
    let probe = Probe {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicUsize::new(0)),
    };
    host.register_endpoint(Box::new(PendingEndpoint {
        sse,
        entered: probe.entered.clone(),
        release: probe.release.clone(),
        dropped: probe.dropped.clone(),
    }))
    .unwrap();
    probe
}

async fn connect(host: &ManagedWebHost, path: &str) -> TcpStream {
    let mut socket = TcpStream::connect(host.address()).await.unwrap();
    socket
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .await
        .unwrap();
    socket
}

#[tokio::test]
async fn bind_is_fallible_and_port_zero_reports_the_actual_listener() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    let error = WarpWebHost::new()
        .bind(HostConfig::localhost(port), watch::channel(false).0)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, ManagedWebHostError::BindFailed { .. }));
    let mut routes = WarpWebHost::new();
    let probe = add_pending(&mut routes, false);
    probe.release.notify_one();
    let host = routes
        .bind(HostConfig::localhost(0), watch::channel(false).0)
        .await
        .unwrap();
    assert_ne!(host.address().port(), 0);
    let address = host.address();
    let mut socket = connect(&host, "/pending").await;
    let mut bytes = [0; 256];
    let count = socket.read(&mut bytes).await.unwrap();
    assert!(std::str::from_utf8(&bytes[..count])
        .unwrap()
        .starts_with("HTTP/1.1 200"));
    host.close().await.unwrap();
    let rebound = TcpListener::bind(address).await.unwrap();
    drop(rebound);
}

#[tokio::test(start_paused = true)]
async fn close_finishes_admitted_unary_within_the_grace_period() {
    let mut routes = WarpWebHost::new();
    let probe = add_pending(&mut routes, false);
    let host = routes
        .bind(HostConfig::localhost(0), watch::channel(false).0)
        .await
        .unwrap();
    let mut socket = connect(&host, "/pending").await;
    probe.entered.notified().await;
    let tasks = host.tasks.clone();
    let mut closing = host.shutdown.subscribe();
    let close = tokio::spawn(host.close());
    closing.changed().await.unwrap();
    probe.release.notify_one();
    let mut reply = String::new();
    socket.read_to_string(&mut reply).await.unwrap();
    assert!(reply.starts_with("HTTP/1.1 200"));
    assert!(reply.ends_with("finished"));
    close.await.unwrap().unwrap();
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(tasks.is_empty());
}

#[tokio::test(start_paused = true)]
async fn close_deadline_aborts_and_joins_pending_unary_and_sse_on_a_live_runtime() {
    let mut routes = WarpWebHost::new();
    let unary = add_pending(&mut routes, false);
    let sse = add_pending(&mut routes, true);
    let host = routes
        .bind(HostConfig::localhost(0), watch::channel(false).0)
        .await
        .unwrap();
    let address = host.address();
    let mut unary_socket = connect(&host, "/pending").await;
    let mut sse_socket = connect(&host, "/sse").await;
    unary.entered.notified().await;
    sse.entered.notified().await;
    let tasks = host.tasks.clone();
    let mut closing = host.shutdown.subscribe();
    let close = tokio::spawn(host.close());
    closing.changed().await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(unary.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(sse.dropped.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        close.await.unwrap(),
        Err(ManagedWebHostError::CloseTimeout)
    ));
    assert!(tasks.is_empty());
    assert_eq!(unary.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(sse.dropped.load(Ordering::SeqCst), 1);
    let _ = unary_socket.read_to_end(&mut Vec::new()).await;
    let _ = sse_socket.read_to_end(&mut Vec::new()).await;
    let rebound = TcpListener::bind(address).await.unwrap();
    assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
    drop(rebound);
}

#[tokio::test]
async fn dropping_host_aborts_response_work_and_releases_the_listener() {
    let mut routes = WarpWebHost::new();
    let probe = add_pending(&mut routes, true);
    let host = routes
        .bind(HostConfig::localhost(0), watch::channel(false).0)
        .await
        .unwrap();
    let address = host.address();
    let _socket = connect(&host, "/sse").await;
    probe.entered.notified().await;
    let tasks = host.tasks.clone();
    drop(host);
    tasks.drain().await;
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    let _rebound = TcpListener::bind(address).await.unwrap();
}

#[tokio::test]
async fn client_disconnect_drops_pending_unary_and_sse_work() {
    for sse in [false, true] {
        let mut routes = WarpWebHost::new();
        let probe = add_pending(&mut routes, sse);
        let host = routes
            .bind(HostConfig::localhost(0), watch::channel(false).0)
            .await
            .unwrap();
        let socket = connect(&host, if sse { "/sse" } else { "/pending" }).await;
        probe.entered.notified().await;
        drop(socket);
        tokio::time::timeout(Duration::from_secs(2), async {
            while probe.dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disconnect must drop the pending response work");
        host.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn close_deadline_includes_an_unresponsive_serving_task() {
    let mut host = WarpWebHost::new()
        .bind(HostConfig::localhost(0), watch::channel(false).0)
        .await
        .unwrap();
    host.replace_serving_for_test(Box::pin(std::future::pending()), false)
        .await;
    let serving = host.serving.as_ref().unwrap().abort_handle();
    let mut closing = host.shutdown.subscribe();
    let close = tokio::spawn(host.close());
    closing.changed().await.unwrap();
    tokio::time::advance(CLOSE_GRACE + Duration::from_secs(1)).await;
    assert!(matches!(
        close.await.unwrap(),
        Err(ManagedWebHostError::CloseTimeout)
    ));
    assert!(
        serving.is_finished(),
        "serving task must be aborted and joined before close returns"
    );
}

#[tokio::test(start_paused = true)]
async fn http2_stream_tasks_belong_to_the_host_scope() {
    let mut routes = WarpWebHost::new();
    let probe = add_pending(&mut routes, true);
    let host = routes
        .bind(HostConfig::localhost(0), watch::channel(false).0)
        .await
        .unwrap();
    let socket = TcpStream::connect(host.address()).await.unwrap();
    // This executor belongs only to the test client. The server uses HostTasks.
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(socket),
    )
    .await
    .unwrap();
    let connection = tokio::spawn(connection);
    let request = hyper::Request::builder()
        .uri("http://localhost/sse")
        .body(warp::reply().into_response().into_body())
        .unwrap();
    let response = client.send_request(request).await.unwrap();
    probe.entered.notified().await;
    assert_eq!(response.status(), 200);
    let tasks = host.tasks.clone();
    assert!(
        tasks.0.lock().unwrap().tasks.len() >= 2,
        "HTTP/2 connection and stream tasks must both be owned"
    );
    let mut closing = host.shutdown.subscribe();
    let close = tokio::spawn(host.close());
    closing.changed().await.unwrap();
    tokio::time::advance(CLOSE_GRACE + Duration::from_secs(1)).await;
    assert!(matches!(
        close.await.unwrap(),
        Err(ManagedWebHostError::CloseTimeout)
    ));
    assert_eq!(probe.dropped.load(Ordering::SeqCst), 1);
    assert!(tasks.is_empty());
    drop(response);
    connection.abort();
    let _ = connection.await;
}
