// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt, Shared};
use obzenflow_core::http_client::{HttpClient, HttpClientError, HttpResponse, RequestSpec};
use obzenflow_core::web::HttpMethod;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

#[derive(Debug, Clone)]
struct NativeRootsStatus {
    cert_count: usize,
    error_count: usize,
    first_error: Option<String>,
    ssl_cert_file: Option<String>,
    ssl_cert_dir: Option<String>,
}

impl NativeRootsStatus {
    fn load() -> Self {
        let loaded = rustls_native_certs::load_native_certs();
        Self {
            cert_count: loaded.certs.len(),
            error_count: loaded.errors.len(),
            first_error: loaded.errors.first().map(ToString::to_string),
            ssl_cert_file: std::env::var_os("SSL_CERT_FILE")
                .map(|v| v.to_string_lossy().into_owned()),
            ssl_cert_dir: std::env::var_os("SSL_CERT_DIR")
                .map(|v| v.to_string_lossy().into_owned()),
        }
    }
}

type SharedPlatformFuture<T> = Shared<BoxFuture<'static, Result<T, HttpClientError>>>;

static NATIVE_ROOTS_STATUS: OnceLock<SharedPlatformFuture<NativeRootsStatus>> = OnceLock::new();

async fn ensure_native_roots_for_https(url_scheme: &str) -> Result<(), HttpClientError> {
    if url_scheme != "https" {
        return Ok(());
    }

    let status = NATIVE_ROOTS_STATUS
        .get_or_init(|| {
            shared_platform_step("HTTP native-root discovery", || {
                Ok(NativeRootsStatus::load())
            })
        })
        .clone()
        .await?;
    if status.cert_count > 0 {
        return Ok(());
    }

    let mut message = String::new();
    message.push_str(
        "TLS prerequisites missing: no system CA certificates found for https:// requests.\n",
    );
    message.push_str(
        "Fix: install your OS CA bundle (commonly the `ca-certificates` package), or set SSL_CERT_FILE/SSL_CERT_DIR to a PEM bundle/directory.\n",
    );

    if let Some(path) = &status.ssl_cert_file {
        message.push_str(&format!("SSL_CERT_FILE={path}\n"));
    }
    if let Some(path) = &status.ssl_cert_dir {
        message.push_str(&format!("SSL_CERT_DIR={path}\n"));
    }
    if let Some(err) = &status.first_error {
        message.push_str(&format!("native cert load error: {err}\n"));
    }

    if status.error_count > 1 {
        message.push_str(&format!(
            "native cert load errors: {} total\n",
            status.error_count
        ));
    }

    Err(HttpClientError::Transport(message.trim_end().to_string()))
}

type ClientInitResult = Result<reqwest::Client, HttpClientError>;
type ClientInitFn = dyn Fn() -> ClientInitResult + Send + Sync + 'static;
type ClientInitFuture = SharedPlatformFuture<reqwest::Client>;

#[derive(Clone)]
struct ClientInitializer(Arc<ClientInitFn>);

impl ClientInitializer {
    fn production() -> Self {
        Self(Arc::new(build_default_client))
    }

    #[cfg(test)]
    fn injected(initializer: impl Fn() -> ClientInitResult + Send + Sync + 'static) -> Self {
        Self(Arc::new(initializer))
    }

    fn run(&self) -> ClientInitResult {
        catch_unwind(AssertUnwindSafe(|| (self.0)())).unwrap_or_else(|payload| {
            Err(HttpClientError::Transport(format!(
                "HTTP client initialization failed: initializer panicked: {}",
                panic_payload_message(payload.as_ref())
            )))
        })
    }
}

impl fmt::Debug for ClientInitializer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientInitializer(<private>)")
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
}

fn build_default_client() -> ClientInitResult {
    match catch_unwind(AssertUnwindSafe(reqwest::Client::new)) {
        Ok(client) => Ok(client),
        Err(_) => reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|cause| {
                HttpClientError::Transport(format!("HTTP client initialization failed: {cause}"))
            }),
    }
}

fn shared_platform_step<T, F>(operation: &'static str, step: F) -> SharedPlatformFuture<T>
where
    T: Clone + Send + Sync + 'static,
    F: FnOnce() -> Result<T, HttpClientError> + Send + 'static,
{
    async move {
        let handle = entered_tokio_runtime(operation)?;
        let runtime_flavor = match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => "current_thread",
            tokio::runtime::RuntimeFlavor::MultiThread => "multi_thread",
            _ => "unknown",
        };
        tracing::debug!(
            event = "http_client.platform_setup",
            operation,
            runtime_flavor,
            scheduling = "blocking_pool",
            phase = "scheduled",
            "default HTTP platform setup scheduled"
        );

        match handle
            .spawn_blocking(move || {
                let started_at = Instant::now();
                let result = step();
                let elapsed_ms = started_at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let outcome = if result.is_ok() {
                    "ready"
                } else {
                    "transport_error"
                };
                tracing::debug!(
                    event = "http_client.platform_setup",
                    operation,
                    runtime_flavor,
                    scheduling = "blocking_pool",
                    phase = "completed",
                    outcome,
                    elapsed_ms,
                    "default HTTP platform setup completed"
                );
                result
            })
            .await
        {
            Ok(result) => result,
            Err(cause) => {
                tracing::debug!(
                    event = "http_client.platform_setup",
                    operation,
                    runtime_flavor,
                    scheduling = "blocking_pool",
                    phase = "completed",
                    outcome = "task_failed",
                    error = %cause,
                    "default HTTP platform setup task failed"
                );
                Err(HttpClientError::Transport(format!(
                    "{operation} task failed: {cause}"
                )))
            }
        }
    }
    .boxed()
    .shared()
}

fn entered_tokio_runtime(operation: &str) -> Result<tokio::runtime::Handle, HttpClientError> {
    tokio::runtime::Handle::try_current().map_err(|_| {
        HttpClientError::Transport(format!("{operation} requires an entered Tokio runtime"))
    })
}

fn ready_platform_result<T>(result: Result<T, HttpClientError>) -> SharedPlatformFuture<T>
where
    T: Clone + Send + Sync + 'static,
{
    let ready = futures::future::ready(result).boxed().shared();
    // Populate `Shared`'s cached output so pre-seeded clients report ready without
    // mutating a fresh one-time cell through a fallible `set` path.
    let _ = ready.clone().now_or_never();
    ready
}

#[derive(Clone)]
pub struct ReqwestHttpClient {
    state: Arc<OnceLock<ClientInitFuture>>,
    initializer: ClientInitializer,
}

impl fmt::Debug for ReqwestHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestHttpClient")
            .field("initialized", &self.is_ready())
            .finish_non_exhaustive()
    }
}

impl ReqwestHttpClient {
    pub fn new() -> Self {
        Self {
            state: Arc::new(OnceLock::new()),
            initializer: ClientInitializer::production(),
        }
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self::preseeded(client, ClientInitializer::production())
    }

    fn preseeded(client: reqwest::Client, initializer: ClientInitializer) -> Self {
        Self {
            state: Arc::new(OnceLock::from(ready_platform_result(Ok(client)))),
            initializer,
        }
    }

    #[cfg(test)]
    fn with_initializer(
        initializer: impl Fn() -> ClientInitResult + Send + Sync + 'static,
    ) -> Self {
        Self {
            state: Arc::new(OnceLock::new()),
            initializer: ClientInitializer::injected(initializer),
        }
    }

    #[cfg(test)]
    fn is_initialized(&self) -> bool {
        self.is_ready()
    }

    #[cfg(test)]
    fn with_client_and_initializer(
        client: reqwest::Client,
        initializer: impl Fn() -> ClientInitResult + Send + Sync + 'static,
    ) -> Self {
        Self::preseeded(client, ClientInitializer::injected(initializer))
    }

    fn is_ready(&self) -> bool {
        self.state
            .get()
            .is_some_and(|initialization| initialization.peek().is_some())
    }

    fn initialization(&self) -> ClientInitFuture {
        self.state
            .get_or_init(|| {
                let initializer = self.initializer.clone();
                shared_platform_step("HTTP client initialization", move || initializer.run())
            })
            .clone()
    }

    async fn client(&self) -> ClientInitResult {
        self.initialization().await
    }
}

impl Default for ReqwestHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

fn map_method(method: HttpMethod) -> reqwest::Method {
    match method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Delete => reqwest::Method::DELETE,
        HttpMethod::Patch => reqwest::Method::PATCH,
        HttpMethod::Head => reqwest::Method::HEAD,
        HttpMethod::Options => reqwest::Method::OPTIONS,
    }
}

fn map_reqwest_error(err: reqwest::Error) -> HttpClientError {
    if err.is_timeout() {
        return HttpClientError::Timeout(err.to_string());
    }

    if err.is_connect() {
        return HttpClientError::Connection(err.to_string());
    }

    HttpClientError::Transport(err.to_string())
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn execute(&self, request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
        entered_tokio_runtime("HTTP request execution")?;
        let client = self.client().await?;
        ensure_native_roots_for_https(request.url.scheme()).await?;

        let mut builder = client
            .request(map_method(request.method), request.url)
            .headers(request.headers);

        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        let response = builder.send().await.map_err(map_reqwest_error)?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.map_err(map_reqwest_error)?;

        Ok(HttpResponse::new(status, headers, body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::http_client::RequestSpec;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use url::Url;

    async fn spawn_http_fixture(
        expected_requests: usize,
    ) -> (Url, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind HTTP fixture");
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
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                    )
                    .await
                    .expect("write HTTP response");
            }
        });
        (
            Url::parse(&format!("http://{address}/fixture")).expect("fixture URL"),
            request_count,
            task,
        )
    }

    #[test]
    fn construction_and_cloning_are_cold() {
        let client = ReqwestHttpClient::new();
        let clone = client.clone();

        assert!(!client.is_initialized());
        assert!(!clone.is_initialized());
        assert!(Arc::ptr_eq(&client.state, &clone.state));
    }

    #[test]
    fn execute_without_an_entered_tokio_runtime_returns_a_typed_error() {
        let client = ReqwestHttpClient::new();
        let request = RequestSpec::new(
            HttpMethod::Get,
            Url::parse("http://127.0.0.1:9/never-sent").expect("test URL"),
        );

        let error = futures::executor::block_on(client.execute(request))
            .expect_err("request execution outside Tokio must fail before setup");

        assert_eq!(
            error.to_string(),
            "transport error: HTTP request execution requires an entered Tokio runtime"
        );
        assert!(!client.is_initialized());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_does_not_repeat_setup_or_send_a_request() {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let watchdog_gate = gate.clone();
        let watchdog = std::thread::spawn(move || {
            let (lock, wake) = &*watchdog_gate;
            let released = lock.lock().expect("lock setup gate");
            let (mut released, _) = wake
                .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
                .expect("wait on setup gate");
            if !*released {
                *released = true;
                wake.notify_all();
            }
        });

        let initializations = Arc::new(AtomicUsize::new(0));
        let init_count = initializations.clone();
        let initializer_gate = gate.clone();
        let client = ReqwestHttpClient::with_initializer(move || {
            init_count.fetch_add(1, Ordering::SeqCst);
            let (lock, wake) = &*initializer_gate;
            let released = lock.lock().expect("lock setup gate");
            drop(
                wake.wait_while(released, |released| !*released)
                    .expect("wait for setup release"),
            );
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .map_err(|error| HttpClientError::Transport(error.to_string()))
        });
        let (url, requests, fixture) = spawn_http_fixture(1).await;

        let first_client = client.clone();
        let first_url = url.clone();
        let first = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(50),
                first_client.execute(RequestSpec::new(HttpMethod::Get, first_url)),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while initializations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking-pool initialization starts without blocking current-thread Tokio");

        assert!(
            first.await.expect("first execute task").is_err(),
            "the first waiter must time out while physical setup continues"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 0);

        {
            let (lock, wake) = &*gate;
            *lock.lock().expect("lock setup gate") = true;
            wake.notify_all();
        }
        watchdog.join().expect("setup watchdog");

        let response = client
            .clone()
            .execute(RequestSpec::new(HttpMethod::Get, url))
            .await
            .expect("a later waiter reuses the retained setup verdict");
        assert_eq!(response.status, 200);
        fixture.await.expect("HTTP fixture task");
        assert_eq!(initializations.load(Ordering::SeqCst), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(client.is_initialized());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_execute_initializes_once_across_clones() {
        const CALLERS: usize = 8;
        let initializations = Arc::new(AtomicUsize::new(0));
        let init_count = initializations.clone();
        let client = ReqwestHttpClient::with_initializer(move || {
            init_count.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(25));
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .map_err(|error| HttpClientError::Transport(error.to_string()))
        });
        let (url, requests, fixture) = spawn_http_fixture(CALLERS).await;

        let mut tasks = Vec::new();
        for _ in 0..CALLERS {
            let client = client.clone();
            let url = url.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .execute(RequestSpec::new(HttpMethod::Get, url))
                    .await
                    .expect("fixture request succeeds")
            }));
        }

        for task in tasks {
            assert_eq!(task.await.expect("execute task").status, 200);
        }
        fixture.await.expect("HTTP fixture task");
        assert_eq!(initializations.load(Ordering::SeqCst), 1);
        assert_eq!(requests.load(Ordering::SeqCst), CALLERS);
        assert!(client.is_initialized());
    }

    #[tokio::test]
    async fn initialization_failure_is_cached_across_repeated_execute_calls() {
        let initializations = Arc::new(AtomicUsize::new(0));
        let init_count = initializations.clone();
        let client = ReqwestHttpClient::with_initializer(move || {
            init_count.fetch_add(1, Ordering::SeqCst);
            Err(HttpClientError::Transport(
                "synthetic initialization failure".to_string(),
            ))
        });
        let url = Url::parse("http://127.0.0.1:9/never-sent").expect("test URL");

        for caller in [client.clone(), client.clone(), client.clone()] {
            let error = caller
                .execute(RequestSpec::new(HttpMethod::Get, url.clone()))
                .await
                .expect_err("initialization must fail before request send");
            assert_eq!(
                error.to_string(),
                "transport error: synthetic initialization failure"
            );
        }

        assert_eq!(initializations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn initializer_panic_becomes_one_cached_typed_failure() {
        let initializations = Arc::new(AtomicUsize::new(0));
        let init_count = initializations.clone();
        let client = ReqwestHttpClient::with_initializer(move || {
            init_count.fetch_add(1, Ordering::SeqCst);
            panic!("synthetic initializer panic")
        });
        let url = Url::parse("http://127.0.0.1:9/never-sent").expect("test URL");

        let first = client
            .execute(RequestSpec::new(HttpMethod::Get, url.clone()))
            .await
            .expect_err("panic is converted to a transport error");
        let second = client
            .clone()
            .execute(RequestSpec::new(HttpMethod::Get, url))
            .await
            .expect_err("cached panic result is returned to clones");

        assert_eq!(initializations.load(Ordering::SeqCst), 1);
        assert_eq!(first.to_string(), second.to_string());
        assert!(first
            .to_string()
            .contains("initializer panicked: synthetic initializer panic"));
    }

    #[tokio::test]
    async fn with_client_is_preseeded_and_never_invokes_the_initializer() {
        let initializations = Arc::new(AtomicUsize::new(0));
        let init_count = initializations.clone();
        let reqwest_client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("build preseeded client");
        let client = ReqwestHttpClient::with_client_and_initializer(reqwest_client, move || {
            init_count.fetch_add(1, Ordering::SeqCst);
            Err(HttpClientError::Transport(
                "preseeded initializer must not run".to_string(),
            ))
        });
        let (url, requests, fixture) = spawn_http_fixture(1).await;

        assert!(client.is_initialized());
        let response = client
            .execute(RequestSpec::new(HttpMethod::Get, url))
            .await
            .expect("preseeded client executes request");

        assert_eq!(response.status, 200);
        fixture.await.expect("HTTP fixture task");
        assert_eq!(initializations.load(Ordering::SeqCst), 0);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
#[path = "reqwest_client_replay_tests.rs"]
mod replay_tests;
