use async_trait::async_trait;
use obzenflow_core::event::observability::{HttpPullState, HttpPullTelemetry, WaitReason};
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError};
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_runtime_services::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler,
};
use obzenflow_runtime_services::stages::SourceError;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::Instant;

pub use obzenflow_core::http_client::{HttpResponse, RequestSpec};

pub struct DecodeResult<C, T> {
    pub items: Vec<T>,
    pub next_cursor: Option<C>,
}

#[derive(Debug, Clone)]
pub enum DecodeError {
    /// Unrecoverable error — terminal (e.g., 4xx auth/config errors).
    Fatal(String),

    /// Transient error — may be retried (e.g., 5xx).
    Transient(String),

    /// Rate limited — includes optional retry hint (e.g., 429 + Retry-After).
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
    },

    /// Deserialization failure — page couldn't be parsed.
    Parse(String),
}

/// Decoder trait — owns event typing, request building, and response parsing (FLOWIP-084e OT-6/7).
pub trait PullDecoder: Clone + Debug + Send + Sync + 'static {
    type Cursor: Clone + Debug + Send + Sync;
    type Item: Serialize + Send;

    /// Event type for all decoded items.
    /// MUST be stable for the lifetime of this decoder instance.
    fn event_type(&self) -> String;

    /// Build the next HTTP request.
    ///
    /// INFALLIBLE: decoder must store pre-validated URL/headers.
    fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec;

    /// Decode a successful response (2xx) into items + optional next cursor.
    ///
    /// NOTE: "partial decode" is not supported in v1 (DC-2).
    fn decode_success(
        &self,
        cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError>;

    /// Decode a non-success HTTP response.
    ///
    /// Default mapping (can be overridden):
    /// - `304` => `Ok(empty, next_cursor=None)` (ends current cycle)
    /// - `429` => `DecodeError::RateLimited` (parses `Retry-After` seconds if present)
    /// - `5xx` => `DecodeError::Transient`
    /// - `4xx` => `DecodeError::Fatal` (validation error path)
    fn decode_error(
        &self,
        _cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        match response.status {
            304 => Ok(DecodeResult {
                items: Vec::new(),
                next_cursor: None,
            }),
            429 => {
                let retry_after = response
                    .header("Retry-After")
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs);
                Err(DecodeError::RateLimited {
                    message: "rate_limited".to_string(),
                    retry_after,
                })
            }
            status if status >= 500 => Err(DecodeError::Transient(format!("HTTP {status}"))),
            status if status >= 400 => Err(DecodeError::Fatal(format!("HTTP {status}"))),
            status => Err(DecodeError::Fatal(format!("HTTP {status}"))),
        }
    }

    /// Decode response parts into items + optional next cursor.
    ///
    /// Default behavior calls `decode_success()` for 2xx and `decode_error()` otherwise.
    fn decode(
        &self,
        cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        if response.is_success() {
            self.decode_success(cursor, response)
        } else {
            self.decode_error(cursor, response)
        }
    }
}

/// Cursorless decoder helper for "fetch latest N" polling endpoints.
///
/// This avoids exposing cursor types in user code, since `type Cursor = ()` is extremely common
/// but cannot be expressed as an associated type default on stable Rust.
pub trait CursorlessPullDecoder: Clone + Debug + Send + Sync + 'static {
    type Item: Serialize + Send;

    fn event_type(&self) -> String;
    fn request_spec(&self) -> RequestSpec;

    fn decode_success(&self, response: &HttpResponse) -> Result<Vec<Self::Item>, DecodeError>;

    fn decode_error(&self, response: &HttpResponse) -> Result<Vec<Self::Item>, DecodeError> {
        match response.status {
            304 => Ok(Vec::new()),
            429 => {
                let retry_after = response
                    .header("Retry-After")
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_secs);
                Err(DecodeError::RateLimited {
                    message: "rate_limited".to_string(),
                    retry_after,
                })
            }
            status if status >= 500 => Err(DecodeError::Transient(format!("HTTP {status}"))),
            status if status >= 400 => Err(DecodeError::Fatal(format!("HTTP {status}"))),
            status => Err(DecodeError::Fatal(format!("HTTP {status}"))),
        }
    }

    fn decode(&self, response: &HttpResponse) -> Result<Vec<Self::Item>, DecodeError> {
        if response.is_success() {
            self.decode_success(response)
        } else {
            self.decode_error(response)
        }
    }
}

impl<D> PullDecoder for D
where
    D: CursorlessPullDecoder,
{
    type Cursor = ();
    type Item = D::Item;

    fn event_type(&self) -> String {
        CursorlessPullDecoder::event_type(self)
    }

    fn request_spec(&self, _cursor: Option<&Self::Cursor>) -> RequestSpec {
        CursorlessPullDecoder::request_spec(self)
    }

    fn decode_success(
        &self,
        _cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        let items = CursorlessPullDecoder::decode_success(self, response)?;
        Ok(DecodeResult {
            items,
            next_cursor: None,
        })
    }

    fn decode_error(
        &self,
        _cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        let items = CursorlessPullDecoder::decode_error(self, response)?;
        Ok(DecodeResult {
            items,
            next_cursor: None,
        })
    }
}

/// Closure-based decoder to minimize boilerplate in demos/examples.
pub struct FnPullDecoder<C, T> {
    event_type: String,
    request_spec: Arc<dyn Fn(Option<&C>) -> RequestSpec + Send + Sync>,
    decode_success: Arc<
        dyn Fn(Option<&C>, &HttpResponse) -> Result<DecodeResult<C, T>, DecodeError> + Send + Sync,
    >,
}

impl<C, T> Clone for FnPullDecoder<C, T> {
    fn clone(&self) -> Self {
        Self {
            event_type: self.event_type.clone(),
            request_spec: self.request_spec.clone(),
            decode_success: self.decode_success.clone(),
        }
    }
}

impl<C, T> FnPullDecoder<C, T> {
    pub fn new<E, B, D>(event_type: E, build_request: B, decode_success: D) -> Self
    where
        E: Into<String>,
        B: Fn(Option<&C>) -> RequestSpec + Send + Sync + 'static,
        D: Fn(Option<&C>, &HttpResponse) -> Result<DecodeResult<C, T>, DecodeError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            event_type: event_type.into(),
            request_spec: Arc::new(build_request),
            decode_success: Arc::new(decode_success),
        }
    }
}

impl<C, T> Debug for FnPullDecoder<C, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnPullDecoder")
            .field("event_type", &self.event_type)
            .field("cursor_type", &std::any::type_name::<C>())
            .field("item_type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<C, T> PullDecoder for FnPullDecoder<C, T>
where
    C: Clone + Debug + Send + Sync + 'static,
    T: Serialize + Send + 'static,
{
    type Cursor = C;
    type Item = T;

    fn event_type(&self) -> String {
        self.event_type.clone()
    }

    fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
        (self.request_spec)(cursor)
    }

    fn decode_success(
        &self,
        cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        (self.decode_success)(cursor, response)
    }
}

/// Create a cursorless decoder for "poll latest" endpoints.
///
/// The returned decoder:
/// - issues a `GET` to `url` each poll
/// - decodes only 2xx payloads via `decode_success`
/// - ends each poll cycle immediately (`next_cursor = None`)
pub fn simple_poll<T, D>(
    event_type: impl Into<String>,
    url: obzenflow_core::http_client::Url,
    decode_success: D,
) -> FnPullDecoder<(), T>
where
    T: Serialize + Send + 'static,
    D: Fn(&HttpResponse) -> Result<Vec<T>, DecodeError> + Send + Sync + 'static,
{
    let event_type = event_type.into();
    FnPullDecoder::new(
        event_type,
        move |_cursor: Option<&()>| RequestSpec::get(url.clone()),
        move |_cursor: Option<&()>, response: &HttpResponse| {
            let items = decode_success(response)?;
            Ok(DecodeResult {
                items,
                next_cursor: None,
            })
        },
    )
}

/// Retry configuration for HTTP pull/poll sources (FLOWIP-084e OT-9).
#[derive(Debug, Clone)]
pub struct HttpRetryConfig {
    /// Max retries for transient errors (5xx, connection).
    pub transient_max_retries: usize,

    /// Backoff delays for transient retries.
    pub transient_backoff: Vec<Duration>,

    /// Max wait for rate limit Retry-After.
    ///
    /// If Retry-After exceeds this, treat as transient error instead.
    pub rate_limit_max_wait: Duration,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            transient_max_retries: 2,
            transient_backoff: vec![
                Duration::from_millis(100),
                Duration::from_millis(500),
                Duration::from_secs(2),
            ],
            rate_limit_max_wait: Duration::from_secs(60),
        }
    }
}

/// Configuration for HTTP pull source (finite mode) (FLOWIP-084e OT-1/2/8/9).
#[derive(Clone)]
pub struct HttpPullConfig {
    /// Pre-configured HTTP client (timeouts, TLS, connection pooling).
    pub client: Arc<dyn HttpClient>,

    /// Headers injected into every request.
    pub default_headers: HeaderMap,

    /// Maximum events to emit per next() call.
    pub max_batch_size: usize,

    /// Retry/backoff configuration.
    pub retry: HttpRetryConfig,
}

/// Configuration for HTTP poll source (infinite mode) (FLOWIP-084e OT-1/2/8/9).
#[derive(Clone)]
pub struct HttpPollConfig {
    pub client: Arc<dyn HttpClient>,
    pub default_headers: HeaderMap,
    pub max_batch_size: usize,
    pub retry: HttpRetryConfig,

    /// Interval between polling cycles (enforced by source via tokio::sleep).
    pub poll_interval: Duration,
}

impl std::fmt::Debug for HttpPullConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpPullConfig")
            .field("client", &"<dyn HttpClient>")
            .field("default_headers_len", &self.default_headers.len())
            .field("max_batch_size", &self.max_batch_size)
            .field("retry", &self.retry)
            .finish()
    }
}

impl std::fmt::Debug for HttpPollConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpPollConfig")
            .field("client", &"<dyn HttpClient>")
            .field("default_headers_len", &self.default_headers.len())
            .field("max_batch_size", &self.max_batch_size)
            .field("retry", &self.retry)
            .field("poll_interval", &self.poll_interval)
            .finish()
    }
}

/// Finite (EOF-terminating) HTTP pull source (FLOWIP-084e).
///
/// NOTE: This source may perform in-handler waits (e.g., honoring `Retry-After` and backoff).
/// When mounted via `async_source!`, configure the stage poll timeout accordingly (IC-1):
/// `async_source!("http_pull" => (source, Some(Duration::from_secs(120))))` or disable it with `None`.
#[derive(Debug, Clone)]
pub struct HttpPullSource<D: PullDecoder> {
    inner: Arc<Mutex<HttpPullSourceInner<D>>>,
    decoder: D,
    writer_id: Option<WriterId>,
    config: HttpPullConfig,
}

/// Infinite HTTP poll source (FLOWIP-084e).
///
/// This source self-manages cadence via `poll_interval` and emits low-volume telemetry snapshots
/// (`http_pull.snapshot`) on state changes (e.g., entering/leaving waits) for Prometheus visibility.
#[derive(Debug, Clone)]
pub struct HttpPollSource<D: PullDecoder> {
    inner: Arc<Mutex<HttpPollSourceInner<D>>>,
    decoder: D,
    writer_id: Option<WriterId>,
    config: HttpPollConfig,
}

#[derive(Debug)]
struct HttpPullSourceInner<D: PullDecoder> {
    cursor: Option<D::Cursor>,
    buffer: VecDeque<ChainEvent>,
    exhausted: bool,

    telemetry: HttpPullTelemetry,
    scheduled_wait: Option<ScheduledWait>,
    transient_attempts: usize,

    poisoned: bool,
    pending_error: Option<ChainEvent>,
}

#[derive(Debug)]
struct HttpPollSourceInner<D: PullDecoder> {
    cursor: Option<D::Cursor>,
    buffer: VecDeque<ChainEvent>,
    first_fetch: bool,

    cycle_exhausted: bool,

    telemetry: HttpPullTelemetry,
    scheduled_wait: Option<ScheduledWait>,
    transient_attempts: usize,

    poisoned: bool,
}

#[derive(Debug, Clone)]
struct ScheduledWait {
    reason: WaitReason,
    wake_at: Instant,
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn unix_wake_secs(delay: Duration) -> u64 {
    SystemTime::now()
        .checked_add(delay)
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn observe_http_status(telemetry: &mut HttpPullTelemetry, status: u16) {
    match status / 100 {
        2 => telemetry.responses_2xx = telemetry.responses_2xx.saturating_add(1),
        4 => telemetry.responses_4xx = telemetry.responses_4xx.saturating_add(1),
        5 => telemetry.responses_5xx = telemetry.responses_5xx.saturating_add(1),
        _ => {}
    }
}

fn start_wait(
    telemetry: &mut HttpPullTelemetry,
    reason: WaitReason,
    delay: Duration,
) -> ScheduledWait {
    let wake_unix_secs = unix_wake_secs(delay);
    let wait_seconds = delay.as_secs_f64();
    match reason {
        WaitReason::RateLimit => {
            telemetry.wait_seconds_rate_limit += wait_seconds;
        }
        WaitReason::PollInterval => {
            telemetry.wait_seconds_poll_interval += wait_seconds;
        }
        WaitReason::Backoff => {
            telemetry.wait_seconds_backoff += wait_seconds;
        }
    }

    telemetry.state = HttpPullState::Waiting;
    telemetry.wait_reason = Some(reason.clone());
    telemetry.next_wake_unix_secs = Some(wake_unix_secs);

    ScheduledWait {
        reason,
        wake_at: Instant::now() + delay,
    }
}

fn finish_wait(telemetry: &mut HttpPullTelemetry) {
    telemetry.state = HttpPullState::Fetching;
    telemetry.wait_reason = None;
    telemetry.next_wake_unix_secs = None;
}

fn set_terminal(telemetry: &mut HttpPullTelemetry) {
    telemetry.state = HttpPullState::Terminal;
    telemetry.wait_reason = None;
    telemetry.next_wake_unix_secs = None;
}

fn telemetry_to_observability_event(
    writer_id: WriterId,
    telemetry: &HttpPullTelemetry,
) -> Result<ChainEvent, SourceError> {
    let value = serde_json::to_value(telemetry).map_err(|e| {
        SourceError::Other(format!(
            "Failed to serialize HTTP pull telemetry snapshot: {e}"
        ))
    })?;

    Ok(ChainEventFactory::observability_event(
        writer_id,
        ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
            name: "http_pull.snapshot".to_string(),
            value,
            tags: None,
        }),
    ))
}

impl<D: PullDecoder> HttpPullSource<D> {
    pub fn new(decoder: D, config: HttpPullConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HttpPullSourceInner {
                cursor: None,
                buffer: VecDeque::new(),
                exhausted: false,
                telemetry: HttpPullTelemetry::default(),
                scheduled_wait: None,
                transient_attempts: 0,
                poisoned: false,
                pending_error: None,
            })),
            decoder,
            writer_id: None,
            config,
        }
    }
}

impl<D: PullDecoder> HttpPollSource<D> {
    pub fn new(decoder: D, config: HttpPollConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HttpPollSourceInner {
                cursor: None,
                buffer: VecDeque::new(),
                first_fetch: true,
                cycle_exhausted: false,
                telemetry: HttpPullTelemetry::default(),
                scheduled_wait: None,
                transient_attempts: 0,
                poisoned: false,
            })),
            decoder,
            writer_id: None,
            config,
        }
    }
}

#[derive(Debug)]
enum FetchError {
    Timeout(String),
    Transport(String),
    Decode { status: u16, error: DecodeError },
    Validation { status: u16, message: String },
}

impl FetchError {
    fn into_source_error(self) -> SourceError {
        match self {
            FetchError::Timeout(msg) => SourceError::Timeout(msg),
            FetchError::Transport(msg) => SourceError::Transport(msg),
            FetchError::Decode {
                error: DecodeError::Parse(msg),
                ..
            } => SourceError::Deserialization(msg),
            FetchError::Decode {
                error: DecodeError::Transient(msg),
                ..
            } => SourceError::Transport(msg),
            FetchError::Decode {
                error:
                    DecodeError::RateLimited {
                        message,
                        retry_after,
                    },
                ..
            } => {
                let retry_hint = retry_after
                    .map(|d| format!(" (retry_after_ms={})", d.as_millis()))
                    .unwrap_or_default();
                SourceError::Transport(format!("rate_limited{retry_hint}: {message}"))
            }
            FetchError::Decode {
                error: DecodeError::Fatal(msg),
                ..
            } => SourceError::Other(msg),
            FetchError::Validation { status, message } => {
                SourceError::Other(format!("validation_error (HTTP {status}): {message}"))
            }
        }
    }
}

fn apply_default_headers(request: &mut RequestSpec, default_headers: &HeaderMap) {
    for (name, value) in default_headers.iter() {
        if !request.headers.contains_key(name) {
            request.headers.insert(name, value.clone());
        }
    }
}

fn map_http_client_error(err: HttpClientError) -> FetchError {
    match err {
        HttpClientError::Timeout(message) => FetchError::Timeout(message),
        HttpClientError::Connection(message) | HttpClientError::Transport(message) => {
            FetchError::Transport(message)
        }
        HttpClientError::Cancelled => FetchError::Transport("cancelled".to_string()),
    }
}

async fn fetch_decode_once<D: PullDecoder>(
    decoder: &D,
    client: &dyn HttpClient,
    default_headers: &HeaderMap,
    cursor: Option<&D::Cursor>,
) -> Result<(u16, DecodeResult<D::Cursor, D::Item>), FetchError> {
    let mut request = decoder.request_spec(cursor);
    apply_default_headers(&mut request, default_headers);

    let http_response = client.execute(request).await.map_err(map_http_client_error)?;
    let status = http_response.status;

    match decoder.decode(cursor, &http_response) {
        Ok(result) => Ok((status, result)),
        Err(DecodeError::Fatal(message)) => Err(FetchError::Validation { status, message }),
        Err(err) => Err(FetchError::Decode { status, error: err }),
    }
}

#[async_trait]
impl<D: PullDecoder> AsyncFiniteSourceHandler for HttpPullSource<D> {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let mut inner = self.inner.lock().await;

        if let Some(error_event) = inner.pending_error.take() {
            let mut out = vec![error_event];
            if out.len() < self.config.max_batch_size && !inner.buffer.is_empty() {
                out.extend(inner.drain_batch(self.config.max_batch_size - out.len()));
            }
            return Ok(Some(out));
        }

        if !inner.buffer.is_empty() {
            return Ok(Some(inner.drain_batch(self.config.max_batch_size)));
        }

        if inner.poisoned {
            return Ok(None);
        }

        if inner.exhausted {
            return Ok(None);
        }

        let writer_id = self
            .writer_id
            .expect("bind_writer_id must be called before next()");

        if let Some(wait) = inner.scheduled_wait.as_ref() {
            let wake_at = wait.wake_at;
            if Instant::now() < wake_at {
                tokio::time::sleep_until(wake_at).await;
            }
            inner.scheduled_wait = None;
            finish_wait(&mut inner.telemetry);
            let telemetry_event = telemetry_to_observability_event(writer_id, &inner.telemetry)?;
            inner.buffer.push_back(telemetry_event);
            return Ok(Some(inner.drain_batch(self.config.max_batch_size)));
        }

        while inner.buffer.is_empty() && !inner.poisoned && !inner.exhausted {
            let cursor = inner.cursor.clone();

            inner.telemetry.requests_total = inner.telemetry.requests_total.saturating_add(1);

            match fetch_decode_once(
                &self.decoder,
                self.config.client.as_ref(),
                &self.config.default_headers,
                cursor.as_ref(),
            )
            .await
            {
                Ok((status, result)) => {
                    observe_http_status(&mut inner.telemetry, status);
                    inner.telemetry.last_success_unix_secs = Some(unix_now_secs());
                    inner.transient_attempts = 0;
                    inner.telemetry.events_decoded_total = inner
                        .telemetry
                        .events_decoded_total
                        .saturating_add(result.items.len() as u64);

                    inner.apply_decode_result(writer_id, &self.decoder, result)?;

                    if inner.exhausted {
                        set_terminal(&mut inner.telemetry);
                        let telemetry_event =
                            telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                        inner.buffer.push_back(telemetry_event);
                    }
                }

                Err(FetchError::Validation { status, message }) => {
                    observe_http_status(&mut inner.telemetry, status);

                    let error_event = self.build_validation_error_event(status, &message);
                    inner.poisoned = true;
                    inner.pending_error = Some(error_event);

                    set_terminal(&mut inner.telemetry);
                    let telemetry_event =
                        telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                    inner.buffer.push_back(telemetry_event);

                    let mut out = Vec::new();
                    if let Some(ev) = inner.pending_error.take() {
                        out.push(ev);
                    }
                    if out.len() < self.config.max_batch_size && !inner.buffer.is_empty() {
                        out.extend(inner.drain_batch(self.config.max_batch_size - out.len()));
                    }
                    return Ok(Some(out));
                }

                Err(FetchError::Decode {
                    status,
                    error: DecodeError::Parse(msg),
                }) => {
                    observe_http_status(&mut inner.telemetry, status);
                    return Err(SourceError::Deserialization(msg));
                }

                Err(FetchError::Decode {
                    status,
                    error:
                        DecodeError::RateLimited {
                            message,
                            retry_after,
                        },
                }) => {
                    observe_http_status(&mut inner.telemetry, status);
                    inner.telemetry.rate_limited_total =
                        inner.telemetry.rate_limited_total.saturating_add(1);

                    let delay = retry_after.unwrap_or(Duration::from_secs(5));

                    if delay > self.config.retry.rate_limit_max_wait {
                        let msg = format!(
                            "rate_limited (retry_after_ms={} exceeds max_wait_ms={}): {message}",
                            delay.as_millis(),
                            self.config.retry.rate_limit_max_wait.as_millis()
                        );
                        self.handle_transient_error(
                            &mut inner,
                            writer_id,
                            FetchError::Transport(msg),
                        )?;
                        break;
                    }

                    inner.scheduled_wait = Some(start_wait(
                        &mut inner.telemetry,
                        WaitReason::RateLimit,
                        delay,
                    ));
                    let telemetry_event =
                        telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                    inner.buffer.push_back(telemetry_event);
                    break;
                }

                Err(FetchError::Decode {
                    status,
                    error: DecodeError::Transient(msg),
                }) => {
                    observe_http_status(&mut inner.telemetry, status);
                    self.handle_transient_error(&mut inner, writer_id, FetchError::Transport(msg))?;
                    break;
                }

                Err(err @ FetchError::Transport(_)) | Err(err @ FetchError::Timeout(_)) => {
                    self.handle_transient_error(&mut inner, writer_id, err)?;
                    break;
                }

                Err(err) => return Err(err.into_source_error()),
            }
        }

        if !inner.buffer.is_empty() {
            return Ok(Some(inner.drain_batch(self.config.max_batch_size)));
        }

        if inner.poisoned || inner.exhausted {
            return Ok(None);
        }

        Ok(Some(Vec::new()))
    }
}

#[async_trait]
impl<D: PullDecoder> AsyncInfiniteSourceHandler for HttpPollSource<D> {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let mut inner = self.inner.lock().await;

        let writer_id = self
            .writer_id
            .expect("bind_writer_id must be called before next()");

        loop {
            if !inner.buffer.is_empty() {
                return Ok(inner.drain_batch(self.config.max_batch_size));
            }

            if let Some(wait) = inner.scheduled_wait.as_ref() {
                let wake_at = wait.wake_at;
                let reason = wait.reason.clone();

                if Instant::now() < wake_at {
                    tokio::time::sleep_until(wake_at).await;
                }

                inner.scheduled_wait = None;
                match reason {
                    WaitReason::PollInterval => {
                        inner.cursor = None;
                        inner.cycle_exhausted = false;
                    }
                    WaitReason::RateLimit | WaitReason::Backoff => {}
                }
                finish_wait(&mut inner.telemetry);
                let telemetry_event =
                    telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                inner.buffer.push_back(telemetry_event);
                continue;
            }

            if inner.cycle_exhausted && !inner.first_fetch {
                inner.scheduled_wait = Some(start_wait(
                    &mut inner.telemetry,
                    WaitReason::PollInterval,
                    self.config.poll_interval,
                ));
                let telemetry_event =
                    telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                inner.buffer.push_back(telemetry_event);
                continue;
            }

            inner.first_fetch = false;

            let cursor = inner.cursor.clone();
            inner.telemetry.requests_total = inner.telemetry.requests_total.saturating_add(1);

            match fetch_decode_once(
                &self.decoder,
                self.config.client.as_ref(),
                &self.config.default_headers,
                cursor.as_ref(),
            )
            .await
            {
                Ok((status, result)) => {
                    observe_http_status(&mut inner.telemetry, status);
                    inner.telemetry.last_success_unix_secs = Some(unix_now_secs());
                    inner.transient_attempts = 0;
                    inner.telemetry.events_decoded_total = inner
                        .telemetry
                        .events_decoded_total
                        .saturating_add(result.items.len() as u64);

                    inner.apply_decode_result(writer_id, &self.decoder, result)?;
                }

                Err(FetchError::Validation { status, message }) => {
                    observe_http_status(&mut inner.telemetry, status);

                    let error_event = self.build_validation_error_event(status, &message);
                    inner.buffer.push_back(error_event);

                    inner.cursor = None;
                    inner.cycle_exhausted = false;
                    inner.poisoned = true;

                    inner.scheduled_wait = Some(start_wait(
                        &mut inner.telemetry,
                        WaitReason::PollInterval,
                        self.config.poll_interval,
                    ));
                    let telemetry_event =
                        telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                    inner.buffer.push_back(telemetry_event);
                }

                Err(FetchError::Decode {
                    status,
                    error: DecodeError::Parse(msg),
                }) => {
                    observe_http_status(&mut inner.telemetry, status);
                    return Err(SourceError::Deserialization(msg));
                }

                Err(FetchError::Decode {
                    status,
                    error:
                        DecodeError::RateLimited {
                            message,
                            retry_after,
                        },
                }) => {
                    observe_http_status(&mut inner.telemetry, status);
                    inner.telemetry.rate_limited_total =
                        inner.telemetry.rate_limited_total.saturating_add(1);

                    let delay = retry_after.unwrap_or(Duration::from_secs(5));

                    if delay > self.config.retry.rate_limit_max_wait {
                        let msg = format!(
                            "rate_limited (retry_after_ms={} exceeds max_wait_ms={}): {message}",
                            delay.as_millis(),
                            self.config.retry.rate_limit_max_wait.as_millis()
                        );
                        self.handle_transient_error(
                            &mut inner,
                            writer_id,
                            FetchError::Transport(msg),
                        )?;
                        continue;
                    }

                    inner.scheduled_wait = Some(start_wait(
                        &mut inner.telemetry,
                        WaitReason::RateLimit,
                        delay,
                    ));
                    let telemetry_event =
                        telemetry_to_observability_event(writer_id, &inner.telemetry)?;
                    inner.buffer.push_back(telemetry_event);
                }

                Err(FetchError::Decode {
                    status,
                    error: DecodeError::Transient(msg),
                }) => {
                    observe_http_status(&mut inner.telemetry, status);
                    self.handle_transient_error(&mut inner, writer_id, FetchError::Transport(msg))?;
                }

                Err(err @ FetchError::Transport(_)) | Err(err @ FetchError::Timeout(_)) => {
                    self.handle_transient_error(&mut inner, writer_id, err)?;
                }

                Err(err) => return Err(err.into_source_error()),
            }
        }
    }
}

impl<D: PullDecoder> HttpPullSource<D> {
    fn build_validation_error_event(&self, status: u16, message: &str) -> ChainEvent {
        let writer_id = self
            .writer_id
            .expect("bind_writer_id must be called before next()");
        let error_payload = serde_json::json!({
            "http_status": status,
            "message": message,
        });
        ChainEventFactory::data_event_from(
            writer_id,
            format!("{}.error", self.decoder.event_type()),
            &error_payload,
        )
        .expect("error payload serialization")
        .mark_as_validation_error(format!("HTTP {}: {}", status, message))
    }

    fn handle_transient_error(
        &self,
        inner: &mut HttpPullSourceInner<D>,
        writer_id: WriterId,
        err: FetchError,
    ) -> Result<(), SourceError> {
        if inner.transient_attempts >= self.config.retry.transient_max_retries {
            return Err(err.into_source_error());
        }

        let delay = self
            .config
            .retry
            .transient_backoff
            .get(inner.transient_attempts)
            .copied()
            .unwrap_or(Duration::from_secs(2));

        inner.transient_attempts = inner.transient_attempts.saturating_add(1);
        inner.telemetry.retries_total = inner.telemetry.retries_total.saturating_add(1);
        inner.scheduled_wait = Some(start_wait(&mut inner.telemetry, WaitReason::Backoff, delay));
        let telemetry_event = telemetry_to_observability_event(writer_id, &inner.telemetry)?;
        inner.buffer.push_back(telemetry_event);
        Ok(())
    }
}

impl<D: PullDecoder> HttpPollSource<D> {
    fn build_validation_error_event(&self, status: u16, message: &str) -> ChainEvent {
        let writer_id = self
            .writer_id
            .expect("bind_writer_id must be called before next()");
        let error_payload = serde_json::json!({
            "http_status": status,
            "message": message,
        });
        ChainEventFactory::data_event_from(
            writer_id,
            format!("{}.error", self.decoder.event_type()),
            &error_payload,
        )
        .expect("error payload serialization")
        .mark_as_validation_error(format!("HTTP {}: {}", status, message))
    }

    fn handle_transient_error(
        &self,
        inner: &mut HttpPollSourceInner<D>,
        writer_id: WriterId,
        err: FetchError,
    ) -> Result<(), SourceError> {
        if inner.transient_attempts >= self.config.retry.transient_max_retries {
            return Err(err.into_source_error());
        }

        let delay = self
            .config
            .retry
            .transient_backoff
            .get(inner.transient_attempts)
            .copied()
            .unwrap_or(Duration::from_secs(2));

        inner.transient_attempts = inner.transient_attempts.saturating_add(1);
        inner.telemetry.retries_total = inner.telemetry.retries_total.saturating_add(1);
        inner.scheduled_wait = Some(start_wait(&mut inner.telemetry, WaitReason::Backoff, delay));
        let telemetry_event = telemetry_to_observability_event(writer_id, &inner.telemetry)?;
        inner.buffer.push_back(telemetry_event);
        Ok(())
    }
}

impl<D: PullDecoder> HttpPullSourceInner<D> {
    fn drain_batch(&mut self, max_batch_size: usize) -> Vec<ChainEvent> {
        let max_batch_size = max_batch_size.max(1);
        let count = max_batch_size.min(self.buffer.len());
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(ev) = self.buffer.pop_front() {
                out.push(ev);
            }
        }
        out
    }

    fn apply_decode_result(
        &mut self,
        writer_id: WriterId,
        decoder: &D,
        result: DecodeResult<D::Cursor, D::Item>,
    ) -> Result<(), SourceError> {
        let event_type = decoder.event_type();
        let mut events = Vec::with_capacity(result.items.len());
        for item in &result.items {
            let event = ChainEventFactory::data_event_from(writer_id, &event_type, item)
                .map_err(|e| SourceError::Deserialization(e.to_string()))?;
            events.push(event);
        }
        self.buffer.extend(events);
        self.cursor = result.next_cursor;
        if self.cursor.is_none() {
            self.exhausted = true;
        }
        Ok(())
    }
}

impl<D: PullDecoder> HttpPollSourceInner<D> {
    fn drain_batch(&mut self, max_batch_size: usize) -> Vec<ChainEvent> {
        let max_batch_size = max_batch_size.max(1);
        let count = max_batch_size.min(self.buffer.len());
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(ev) = self.buffer.pop_front() {
                out.push(ev);
            }
        }
        out
    }

    fn apply_decode_result(
        &mut self,
        writer_id: WriterId,
        decoder: &D,
        result: DecodeResult<D::Cursor, D::Item>,
    ) -> Result<(), SourceError> {
        let event_type = decoder.event_type();
        let mut events = Vec::with_capacity(result.items.len());
        for item in &result.items {
            let event = ChainEventFactory::data_event_from(writer_id, &event_type, item)
                .map_err(|e| SourceError::Deserialization(e.to_string()))?;
            events.push(event);
        }
        self.buffer.extend(events);
        self.cursor = result.next_cursor;
        self.cycle_exhausted = self.cursor.is_none();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
    use obzenflow_core::http_client::MockHttpClient;
    use obzenflow_core::StageId;
    use std::sync::Arc;

    fn mock_client() -> (Arc<MockHttpClient>, Arc<dyn HttpClient>) {
        let client = Arc::new(MockHttpClient::new());
        let trait_object: Arc<dyn HttpClient> = client.clone();
        (client, trait_object)
    }

    #[derive(Debug, Clone)]
    struct TestDecoder {
        base_url: obzenflow_core::http_client::Url,
    }

    impl TestDecoder {
        fn new(base_url: obzenflow_core::http_client::Url) -> Self {
            Self { base_url }
        }
    }

    impl PullDecoder for TestDecoder {
        type Cursor = u32;
        type Item = serde_json::Value;

        fn event_type(&self) -> String {
            "test.item.v1".to_string()
        }

        fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
            let page = cursor.copied().unwrap_or(1);
            let mut url = self.base_url.clone();
            url.query_pairs_mut().append_pair("page", &page.to_string());
            let mut request = RequestSpec::get(url);
            request
                .headers
                .insert("authorization", "Bearer decoder".parse().unwrap());
            request
        }

        fn decode_success(
            &self,
            _cursor: Option<&Self::Cursor>,
            response: &HttpResponse,
        ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
            let value: serde_json::Value = response
                .json()
                .map_err(|e| DecodeError::Parse(e.to_string()))?;
            let items = value
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            let next_cursor = value.get("next").and_then(|v| v.as_u64()).map(|v| v as u32);
            Ok(DecodeResult { items, next_cursor })
        }
    }

    #[tokio::test]
    async fn http_pull_source_paginates_using_mock_http_client() {
        let (mock, client) = mock_client();

        mock.enqueue(HttpResponse::new(
            200,
            HeaderMap::new(),
            serde_json::to_vec(&serde_json::json!({
                "items": [{"n": 1}, {"n": 2}],
                "next": 2
            }))
            .expect("json"),
        ));

        mock.enqueue(HttpResponse::new(
            200,
            HeaderMap::new(),
            serde_json::to_vec(&serde_json::json!({
                "items": [{"n": 3}],
                "next": null
            }))
            .expect("json"),
        ));

        let decoder = TestDecoder::new("http://example.invalid/items".parse().unwrap());
        let config = HttpPullConfig {
            client,
            default_headers: HeaderMap::new(),
            max_batch_size: 10,
            retry: HttpRetryConfig {
                transient_max_retries: 0,
                transient_backoff: vec![],
                rate_limit_max_wait: Duration::from_secs(1),
            },
        };

        let mut source = HttpPullSource::new(decoder, config);
        source.bind_writer_id(WriterId::from(StageId::new()));

        let batch1 = source.next().await.unwrap().unwrap();
        let items1: Vec<_> = batch1
            .iter()
            .filter(|e| e.event_type() == "test.item.v1")
            .map(|e| e.payload())
            .collect();
        assert_eq!(
            items1,
            vec![serde_json::json!({"n": 1}), serde_json::json!({"n": 2})]
        );

        let batch2 = source.next().await.unwrap().unwrap();
        let items2: Vec<_> = batch2
            .iter()
            .filter(|e| e.event_type() == "test.item.v1")
            .map(|e| e.payload())
            .collect();
        assert_eq!(items2, vec![serde_json::json!({"n": 3})]);

        let done = source.next().await.unwrap();
        assert!(done.is_none());
    }

    #[test]
    fn default_headers_are_applied_without_overriding_decoder_headers() {
        let mut default_headers = HeaderMap::new();
        default_headers.insert("authorization", "Bearer config".parse().unwrap());

        let mut request = RequestSpec::get("http://example.invalid/".parse().unwrap());
        apply_default_headers(&mut request, &default_headers);
        assert_eq!(
            request
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer config"
        );

        let mut request = RequestSpec::get("http://example.invalid/".parse().unwrap());
        request
            .headers
            .insert("authorization", "Bearer decoder".parse().unwrap());
        apply_default_headers(&mut request, &default_headers);
        assert_eq!(
            request
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer decoder"
        );
    }

    #[tokio::test]
    async fn http_pull_pending_error_emits_once_then_eof() {
        let (_mock, client) = mock_client();
        let decoder = TestDecoder::new("http://example.invalid/items".parse().unwrap());
        let config = HttpPullConfig {
            client,
            default_headers: HeaderMap::new(),
            max_batch_size: 1000,
            retry: HttpRetryConfig::default(),
        };
        let mut source = HttpPullSource::new(decoder, config);
        source.bind_writer_id(WriterId::from(StageId::new()));

        let error_event = source.build_validation_error_event(401, "unauthorized");
        {
            let mut inner = source.inner.lock().await;
            inner.poisoned = true;
            inner.pending_error = Some(error_event);
        }

        let first = source.next().await.unwrap().unwrap();
        assert_eq!(first.len(), 1);
        assert!(matches!(
            first[0].processing_info.status,
            ProcessingStatus::Error {
                kind: Some(ErrorKind::Validation),
                ..
            }
        ));

        let done = source.next().await.unwrap();
        assert!(done.is_none());
    }

    #[tokio::test]
    async fn http_poll_drains_buffer_without_network() {
        let (_mock, client) = mock_client();
        let decoder = TestDecoder::new("http://example.invalid/items".parse().unwrap());
        let config = HttpPollConfig {
            client,
            default_headers: HeaderMap::new(),
            max_batch_size: 2,
            retry: HttpRetryConfig::default(),
            poll_interval: Duration::from_secs(1),
        };
        let mut source = HttpPollSource::new(decoder, config);
        let writer_id = WriterId::from(StageId::new());
        source.bind_writer_id(writer_id);

        {
            let mut inner = source.inner.lock().await;
            inner.buffer.push_back(ChainEventFactory::data_event(
                writer_id,
                "test.item.v1",
                serde_json::json!({"n": 1}),
            ));
            inner.buffer.push_back(ChainEventFactory::data_event(
                writer_id,
                "test.item.v1",
                serde_json::json!({"n": 2}),
            ));
            inner.buffer.push_back(ChainEventFactory::data_event(
                writer_id,
                "test.item.v1",
                serde_json::json!({"n": 3}),
            ));
        }

        let batch1 = source.next().await.unwrap();
        assert_eq!(batch1.len(), 2);
        let batch2 = source.next().await.unwrap();
        assert_eq!(batch2.len(), 1);
    }

    #[tokio::test]
    async fn http_poll_sleeps_for_poll_interval_on_cycle_exhaustion() {
        tokio::time::pause();

        let (_mock, client) = mock_client();
        let decoder = TestDecoder::new("http://example.invalid/items".parse().unwrap());
        let config = HttpPollConfig {
            client,
            default_headers: HeaderMap::new(),
            max_batch_size: 10,
            retry: HttpRetryConfig {
                transient_max_retries: 0,
                transient_backoff: vec![],
                rate_limit_max_wait: Duration::from_secs(1),
            },
            poll_interval: Duration::from_secs(10),
        };

        let mut source = HttpPollSource::new(decoder, config);
        source.bind_writer_id(WriterId::from(StageId::new()));
        let mut source2 = source.clone();

        {
            let mut inner = source.inner.lock().await;
            inner.first_fetch = false;
            inner.cycle_exhausted = true;
        }

        let waiting = source.next().await.unwrap();
        assert_eq!(waiting.len(), 1);

        {
            let inner = source.inner.lock().await;
            assert!(inner.scheduled_wait.is_some());
            assert!(matches!(inner.telemetry.state, HttpPullState::Waiting));
            assert!(matches!(
                inner.telemetry.wait_reason,
                Some(WaitReason::PollInterval)
            ));
        }

        let handle = tokio::spawn(async move { source2.next().await });
        tokio::task::yield_now().await;
        assert!(
            !handle.is_finished(),
            "next() should block on poll_interval"
        );

        tokio::time::advance(Duration::from_secs(10)).await;
        let after_wait = handle.await.expect("join handle").expect("next ok");
        assert_eq!(after_wait.len(), 1);

        {
            let inner = source.inner.lock().await;
            assert!(inner.scheduled_wait.is_none());
            assert!(matches!(inner.telemetry.state, HttpPullState::Fetching));
            assert!(inner.telemetry.wait_reason.is_none());
            assert!(inner.cursor.is_none());
            assert!(!inner.cycle_exhausted);
        }
    }
}
