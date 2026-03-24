// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::observability::{HttpPullState, HttpPullTelemetry, WaitReason};
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::http_client::{HeaderMap, HttpClient, HttpClientError};
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::typing::SourceTyping;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::Instant;

pub use obzenflow_core::http_client::{HttpResponse, RequestSpec};

type RequestSpecFn<C> = Arc<dyn Fn(Option<&C>) -> RequestSpec + Send + Sync>;
type DecodeSuccessFn<C, T> =
    Arc<dyn Fn(Option<&C>, &HttpResponse) -> Result<DecodeResult<C, T>, DecodeError> + Send + Sync>;

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

impl From<serde_json::Error> for DecodeError {
    fn from(value: serde_json::Error) -> Self {
        DecodeError::Parse(value.to_string())
    }
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
    request_spec: RequestSpecFn<C>,
    decode_success: DecodeSuccessFn<C, T>,
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

type ListRequestFn = Arc<dyn Fn() -> RequestSpec + Send + Sync>;
type ListParseFn<K> =
    Arc<dyn for<'a> Fn(&'a HttpResponse) -> Result<Vec<K>, DecodeError> + Send + Sync>;
type DetailRequestFn<K> = Arc<dyn for<'a> Fn(&'a K) -> RequestSpec + Send + Sync>;
type DetailPathFn<K> = Arc<dyn for<'a> Fn(&'a K) -> String + Send + Sync>;
type ItemParseFn<T> =
    Arc<dyn for<'a> Fn(&'a HttpResponse) -> Result<Option<T>, DecodeError> + Send + Sync>;
type OnSkipFn<K> = Arc<dyn for<'a> Fn(&'a K) + Send + Sync>;

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ListDetailState<K> {
    pending: VecDeque<K>,
}

/// Builder for [`ListDetailDecoder`].
///
/// Use this as the primary construction surface so each decoder step is named rather than passed
/// positionally as a closure argument.
#[derive(Clone)]
pub struct ListDetailDecoderBuilder<K, T> {
    event_type: String,
    base_url: Option<obzenflow_core::http_client::Url>,
    list_request: Option<ListRequestFn>,
    list_path: Option<String>,
    parse_list: Option<ListParseFn<K>>,
    detail_request: Option<DetailRequestFn<K>>,
    detail_path: Option<DetailPathFn<K>>,
    parse_item: Option<ItemParseFn<T>>,
    max_list_items: Option<usize>,
    on_skip: Option<OnSkipFn<K>>,
}

impl<K, T> Debug for ListDetailDecoderBuilder<K, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListDetailDecoderBuilder")
            .field("event_type", &self.event_type)
            .field("key_type", &std::any::type_name::<K>())
            .field("item_type", &std::any::type_name::<T>())
            .field("has_base_url", &self.base_url.is_some())
            .field("has_list_request", &self.list_request.is_some())
            .field("has_list_path", &self.list_path.is_some())
            .field("has_parse_list", &self.parse_list.is_some())
            .field("has_detail_request", &self.detail_request.is_some())
            .field("has_detail_path", &self.detail_path.is_some())
            .field("has_parse_item", &self.parse_item.is_some())
            .field("max_list_items", &self.max_list_items)
            .field("has_on_skip", &self.on_skip.is_some())
            .finish()
    }
}

impl<K: 'static, T> ListDetailDecoderBuilder<K, T> {
    fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_type: event_type.into(),
            base_url: None,
            list_request: None,
            list_path: None,
            parse_list: None,
            detail_request: None,
            detail_path: None,
            parse_item: None,
            max_list_items: None,
            on_skip: None,
        }
    }

    /// Set a shared base URL for `.list_path(...)` and `.detail_path(...)`.
    pub fn base_url(mut self, base_url: obzenflow_core::http_client::Url) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Set the list request directly.
    pub fn list_request<F>(mut self, list_request: F) -> Self
    where
        F: Fn() -> RequestSpec + Send + Sync + 'static,
    {
        self.list_request = Some(Arc::new(list_request));
        self.list_path = None;
        self
    }

    /// Set the list request as a `GET` to `list_url`.
    pub fn list_url(self, list_url: obzenflow_core::http_client::Url) -> Self {
        self.list_request(move || RequestSpec::get(list_url.clone()))
    }

    /// Set the list request as a `GET` to `base_url.join(list_path)`.
    pub fn list_path(mut self, list_path: impl Into<String>) -> Self {
        self.list_path = Some(list_path.into());
        self.list_request = None;
        self
    }

    /// Set the list parser.
    pub fn parse_list<F>(mut self, parse_list: F) -> Self
    where
        F: Fn(&HttpResponse) -> Result<Vec<K>, DecodeError> + Send + Sync + 'static,
    {
        self.parse_list = Some(Arc::new(parse_list));
        self
    }

    /// Set the detail request directly.
    pub fn detail_request<F>(mut self, detail_request: F) -> Self
    where
        F: Fn(&K) -> RequestSpec + Send + Sync + 'static,
    {
        self.detail_request = Some(Arc::new(detail_request));
        self.detail_path = None;
        self
    }

    /// Set the detail request as a `GET` to a URL produced from each key.
    pub fn detail_url<F>(self, detail_url: F) -> Self
    where
        F: Fn(&K) -> obzenflow_core::http_client::Url + Send + Sync + 'static,
    {
        self.detail_request(move |key| RequestSpec::get(detail_url(key)))
    }

    /// Set the detail request as a `GET` to `base_url.join(detail_path(key))`.
    pub fn detail_path<F>(mut self, detail_path: F) -> Self
    where
        F: Fn(&K) -> String + Send + Sync + 'static,
    {
        self.detail_path = Some(Arc::new(detail_path));
        self.detail_request = None;
        self
    }

    /// Set the item parser.
    pub fn parse_item<F>(mut self, parse_item: F) -> Self
    where
        F: Fn(&HttpResponse) -> Result<Option<T>, DecodeError> + Send + Sync + 'static,
    {
        self.parse_item = Some(Arc::new(parse_item));
        self
    }

    /// Cap the initial list length before detail requests begin.
    pub fn max_list_items(mut self, max_list_items: usize) -> Self {
        self.max_list_items = Some(max_list_items);
        self
    }

    /// Run a callback when `parse_item` returns `None`.
    pub fn on_skip<F>(mut self, on_skip: F) -> Self
    where
        F: Fn(&K) + Send + Sync + 'static,
    {
        self.on_skip = Some(Arc::new(on_skip));
        self
    }

    /// Build the decoder.
    pub fn build(self) -> anyhow::Result<ListDetailDecoder<K, T>> {
        let list_request = match (self.list_request, self.list_path) {
            (Some(list_request), _) => list_request,
            (None, Some(list_path)) => {
                let base_url = self
                    .base_url
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("base_url required when list_path is used"))?;
                let list_url = base_url
                    .join(&list_path)
                    .map_err(|e| anyhow::anyhow!("invalid list_path: {e}"))?;
                Arc::new(move || RequestSpec::get(list_url.clone()))
            }
            (None, None) => anyhow::bail!("list_request or list_path required"),
        };

        let detail_request = match (self.detail_request, self.detail_path) {
            (Some(detail_request), _) => detail_request,
            (None, Some(detail_path)) => {
                let base_url = self
                    .base_url
                    .ok_or_else(|| anyhow::anyhow!("base_url required when detail_path is used"))?;
                let detail_request: DetailRequestFn<K> = Arc::new(move |key: &K| {
                    let detail_path = detail_path(key);
                    let detail_url = base_url
                        .join(&detail_path)
                        .expect("detail_path should produce a valid URL path");
                    RequestSpec::get(detail_url)
                });
                detail_request
            }
            (None, None) => anyhow::bail!("detail_request or detail_path required"),
        };

        Ok(ListDetailDecoder {
            event_type: self.event_type,
            list_request,
            parse_list: self
                .parse_list
                .ok_or_else(|| anyhow::anyhow!("parse_list required"))?,
            detail_request,
            parse_item: self
                .parse_item
                .ok_or_else(|| anyhow::anyhow!("parse_item required"))?,
            max_list_items: self.max_list_items,
            on_skip: self.on_skip,
        })
    }
}

/// Convenience decoder for the "fetch one list, then fetch each item sequentially" pattern.
///
/// - First request fetches a list of keys (`K`), does not emit items.
/// - Subsequent requests fetch each item (`T`) one at a time.
/// - `None` from `parse_item` is treated as "skip".
pub struct ListDetailDecoder<K, T> {
    event_type: String,
    list_request: ListRequestFn,
    parse_list: ListParseFn<K>,
    detail_request: DetailRequestFn<K>,
    parse_item: ItemParseFn<T>,
    max_list_items: Option<usize>,
    on_skip: Option<OnSkipFn<K>>,
}

impl<K, T> Clone for ListDetailDecoder<K, T> {
    fn clone(&self) -> Self {
        Self {
            event_type: self.event_type.clone(),
            list_request: self.list_request.clone(),
            parse_list: self.parse_list.clone(),
            detail_request: self.detail_request.clone(),
            parse_item: self.parse_item.clone(),
            max_list_items: self.max_list_items,
            on_skip: self.on_skip.clone(),
        }
    }
}

impl<K: 'static, T> ListDetailDecoder<K, T> {
    /// Start a named-step builder for the list/detail decoder.
    pub fn builder<E>(event_type: E) -> ListDetailDecoderBuilder<K, T>
    where
        E: Into<String>,
    {
        ListDetailDecoderBuilder::new(event_type)
    }

    #[doc(hidden)]
    pub fn new<E, PL, DR, PI>(
        event_type: E,
        list_url: obzenflow_core::http_client::Url,
        parse_list: PL,
        detail_request: DR,
        parse_item: PI,
    ) -> Self
    where
        E: Into<String>,
        PL: Fn(&HttpResponse) -> Result<Vec<K>, DecodeError> + Send + Sync + 'static,
        DR: Fn(&K) -> RequestSpec + Send + Sync + 'static,
        PI: Fn(&HttpResponse) -> Result<Option<T>, DecodeError> + Send + Sync + 'static,
    {
        Self::builder(event_type)
            .list_url(list_url)
            .parse_list(parse_list)
            .detail_request(detail_request)
            .parse_item(parse_item)
            .build()
            .expect("ListDetailDecoder::new should provide all required builder fields")
    }

    #[doc(hidden)]
    pub fn new_with_list_request<E, LR, PL, DR, PI>(
        event_type: E,
        list_request: LR,
        parse_list: PL,
        detail_request: DR,
        parse_item: PI,
    ) -> Self
    where
        E: Into<String>,
        LR: Fn() -> RequestSpec + Send + Sync + 'static,
        PL: Fn(&HttpResponse) -> Result<Vec<K>, DecodeError> + Send + Sync + 'static,
        DR: Fn(&K) -> RequestSpec + Send + Sync + 'static,
        PI: Fn(&HttpResponse) -> Result<Option<T>, DecodeError> + Send + Sync + 'static,
    {
        Self::builder(event_type)
            .list_request(list_request)
            .parse_list(parse_list)
            .detail_request(detail_request)
            .parse_item(parse_item)
            .build()
            .expect(
                "ListDetailDecoder::new_with_list_request should provide all required builder fields",
            )
    }

    pub fn max_list_items(mut self, max_list_items: usize) -> Self {
        self.max_list_items = Some(max_list_items);
        self
    }

    pub fn on_skip<F>(mut self, on_skip: F) -> Self
    where
        F: Fn(&K) + Send + Sync + 'static,
    {
        self.on_skip = Some(Arc::new(on_skip));
        self
    }
}

impl<K, T> Debug for ListDetailDecoder<K, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListDetailDecoder")
            .field("event_type", &self.event_type)
            .field("key_type", &std::any::type_name::<K>())
            .field("item_type", &std::any::type_name::<T>())
            .field("max_list_items", &self.max_list_items)
            .field("has_on_skip", &self.on_skip.is_some())
            .finish()
    }
}

impl<K, T> PullDecoder for ListDetailDecoder<K, T>
where
    K: Clone + Debug + Send + Sync + 'static,
    T: Serialize + Send + 'static,
{
    type Cursor = ListDetailState<K>;
    type Item = T;

    fn event_type(&self) -> String {
        self.event_type.clone()
    }

    fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
        match cursor {
            None => (self.list_request)(),
            Some(ListDetailState { pending }) => {
                let key = pending
                    .front()
                    .expect("ListDetailDecoder pending queue should not be empty (cursor values must come from the decoder)");
                tracing::debug!(event_type = %self.event_type, key = ?key, "detail fetch");
                (self.detail_request)(key)
            }
        }
    }

    fn decode_success(
        &self,
        cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        match cursor {
            None => {
                let mut keys = (self.parse_list)(response)?;
                if let Some(max_list_items) = self.max_list_items {
                    let total = keys.len();
                    if total > max_list_items {
                        keys.truncate(max_list_items);
                        tracing::info!(
                            event_type = %self.event_type,
                            total,
                            kept = keys.len(),
                            max_list_items,
                            "list truncated"
                        );
                    }
                }

                if keys.is_empty() {
                    return Ok(DecodeResult {
                        items: Vec::new(),
                        next_cursor: None,
                    });
                }

                let pending = keys.into_iter().collect::<VecDeque<_>>();
                Ok(DecodeResult {
                    items: Vec::new(),
                    next_cursor: Some(ListDetailState { pending }),
                })
            }
            Some(ListDetailState { pending }) => {
                let key = pending
                    .front()
                    .expect("ListDetailDecoder pending queue should not be empty (cursor values must come from the decoder)");
                let item = (self.parse_item)(response)?;
                if item.is_none() {
                    if let Some(on_skip) = self.on_skip.as_ref() {
                        on_skip(key);
                    }
                }

                let mut pending = pending.clone();
                let _ = pending.pop_front();

                let next_cursor = if pending.is_empty() {
                    None
                } else {
                    Some(ListDetailState { pending })
                };

                Ok(DecodeResult {
                    items: item.into_iter().collect(),
                    next_cursor,
                })
            }
        }
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
#[non_exhaustive]
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

    /// Suggested stage poll timeout for `async_source!` when no explicit timeout is provided.
    ///
    /// NOTE: This is a suggestion/hint. If `None`, async finite sources fall back to the
    /// descriptor default poll timeout (currently 30s).
    pub poll_timeout: Option<Duration>,
}

/// Configuration for HTTP poll source (infinite mode) (FLOWIP-084e OT-1/2/8/9).
#[non_exhaustive]
#[derive(Clone)]
pub struct HttpPollConfig {
    pub client: Arc<dyn HttpClient>,
    pub default_headers: HeaderMap,
    pub max_batch_size: usize,
    pub retry: HttpRetryConfig,

    /// Suggested stage poll timeout for `async_source!` when no explicit timeout is provided.
    ///
    /// If `None`, async infinite sources keep the descriptor default of no poll timeout.
    pub poll_timeout: Option<Duration>,

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
            .field("poll_timeout", &self.poll_timeout)
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
            .field("poll_timeout", &self.poll_timeout)
            .field("poll_interval", &self.poll_interval)
            .finish()
    }
}

/// Builder for [`HttpPullConfig`] with sensible defaults.
///
/// NOTE: This builder defaults `poll_timeout` to 120s for finite sources because `HttpPullSource`
/// may legitimately wait inside `next()` (e.g., honoring `Retry-After` and backoff). Override with
/// [`Self::poll_timeout_opt(None)`] to inherit the descriptor default instead.
#[derive(Clone)]
pub struct HttpPullConfigBuilder {
    client: Option<Arc<dyn HttpClient>>,
    default_headers: HeaderMap,
    max_batch_size: usize,
    retry: HttpRetryConfig,
    poll_timeout: Option<Duration>,
}

impl Default for HttpPullConfigBuilder {
    fn default() -> Self {
        Self {
            client: None,
            default_headers: HeaderMap::new(),
            max_batch_size: 10,
            retry: HttpRetryConfig::default(),
            poll_timeout: Some(Duration::from_secs(120)),
        }
    }
}

impl std::fmt::Debug for HttpPullConfigBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpPullConfigBuilder")
            .field("client", &self.client.as_ref().map(|_| "<dyn HttpClient>"))
            .field("default_headers_len", &self.default_headers.len())
            .field("max_batch_size", &self.max_batch_size)
            .field("retry", &self.retry)
            .field("poll_timeout", &self.poll_timeout)
            .finish()
    }
}

impl HttpPullConfigBuilder {
    pub fn client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn default_headers(mut self, default_headers: HeaderMap) -> Self {
        self.default_headers = default_headers;
        self
    }

    /// Insert a single header into [`Self::default_headers`].
    ///
    /// Panics if `name` or `value` are invalid HTTP header syntax.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        let name: http::header::HeaderName = name.parse().expect("valid HTTP header name");
        let value: http::header::HeaderValue = value.parse().expect("valid HTTP header value");
        self.default_headers.insert(name, value);
        self
    }

    pub fn max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn retry(mut self, retry: HttpRetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn poll_timeout(mut self, poll_timeout: Duration) -> Self {
        self.poll_timeout = Some(poll_timeout);
        self
    }

    pub fn poll_timeout_opt(mut self, poll_timeout: Option<Duration>) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }

    pub fn build(self) -> anyhow::Result<HttpPullConfig> {
        let client = self
            .client
            .ok_or_else(|| anyhow::anyhow!("client required"))?;
        if self.max_batch_size == 0 {
            anyhow::bail!("max_batch_size must be > 0");
        }

        Ok(HttpPullConfig {
            client,
            default_headers: self.default_headers,
            max_batch_size: self.max_batch_size,
            retry: self.retry,
            poll_timeout: self.poll_timeout,
        })
    }
}

/// Builder for [`HttpPollConfig`] with sensible defaults.
#[derive(Clone)]
pub struct HttpPollConfigBuilder {
    client: Option<Arc<dyn HttpClient>>,
    default_headers: HeaderMap,
    max_batch_size: usize,
    retry: HttpRetryConfig,
    poll_timeout: Option<Duration>,
    poll_interval: Option<Duration>,
}

impl Default for HttpPollConfigBuilder {
    fn default() -> Self {
        Self {
            client: None,
            default_headers: HeaderMap::new(),
            max_batch_size: 10,
            retry: HttpRetryConfig::default(),
            poll_timeout: None,
            poll_interval: None,
        }
    }
}

impl std::fmt::Debug for HttpPollConfigBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpPollConfigBuilder")
            .field("client", &self.client.as_ref().map(|_| "<dyn HttpClient>"))
            .field("default_headers_len", &self.default_headers.len())
            .field("max_batch_size", &self.max_batch_size)
            .field("retry", &self.retry)
            .field("poll_timeout", &self.poll_timeout)
            .field("poll_interval", &self.poll_interval)
            .finish()
    }
}

impl HttpPollConfigBuilder {
    pub fn client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn default_headers(mut self, default_headers: HeaderMap) -> Self {
        self.default_headers = default_headers;
        self
    }

    /// Insert a single header into [`Self::default_headers`].
    ///
    /// Panics if `name` or `value` are invalid HTTP header syntax.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        let name: http::header::HeaderName = name.parse().expect("valid HTTP header name");
        let value: http::header::HeaderValue = value.parse().expect("valid HTTP header value");
        self.default_headers.insert(name, value);
        self
    }

    pub fn max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn retry(mut self, retry: HttpRetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn poll_timeout(mut self, poll_timeout: Duration) -> Self {
        self.poll_timeout = Some(poll_timeout);
        self
    }

    pub fn poll_timeout_opt(mut self, poll_timeout: Option<Duration>) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }

    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = Some(poll_interval);
        self
    }

    pub fn build(self) -> anyhow::Result<HttpPollConfig> {
        let client = self
            .client
            .ok_or_else(|| anyhow::anyhow!("client required"))?;
        if self.max_batch_size == 0 {
            anyhow::bail!("max_batch_size must be > 0");
        }

        let poll_interval = self
            .poll_interval
            .ok_or_else(|| anyhow::anyhow!("poll_interval required"))?;
        if poll_interval.is_zero() {
            anyhow::bail!("poll_interval must be > 0");
        }

        Ok(HttpPollConfig {
            client,
            default_headers: self.default_headers,
            max_batch_size: self.max_batch_size,
            retry: self.retry,
            poll_timeout: self.poll_timeout,
            poll_interval,
        })
    }
}

impl HttpPullConfig {
    pub fn builder() -> HttpPullConfigBuilder {
        HttpPullConfigBuilder::default()
    }
}

impl HttpPollConfig {
    pub fn builder() -> HttpPollConfigBuilder {
        HttpPollConfigBuilder::default()
    }
}

/// Finite (EOF-terminating) HTTP pull source (FLOWIP-084e).
///
/// NOTE: This source may perform in-handler waits (e.g., honoring `Retry-After` and backoff).
/// Configure a suitable poll timeout via [`HttpPullConfig::builder()`] so `async_source!` can omit
/// the timeout tuple, or override explicitly using `async_source!((source, Some(timeout)))`.
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

impl<D: PullDecoder> SourceTyping for HttpPullSource<D> {
    type Output = D::Item;
}

impl<D: PullDecoder> SourceTyping for HttpPollSource<D> {
    type Output = D::Item;
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

    let http_response = client
        .execute(request)
        .await
        .map_err(map_http_client_error)?;
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

    fn suggested_poll_timeout(&self) -> Option<Duration> {
        self.config.poll_timeout
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

    fn suggested_poll_timeout(&self) -> Option<Duration> {
        self.config.poll_timeout
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
        .mark_as_validation_error(format!("HTTP {status}: {message}"))
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
        .mark_as_validation_error(format!("HTTP {status}: {message}"))
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
    use obzenflow_core::web::HttpMethod;
    use obzenflow_core::StageId;
    use std::sync::Arc;

    fn mock_client() -> (Arc<MockHttpClient>, Arc<dyn HttpClient>) {
        let client = Arc::new(MockHttpClient::new());
        let trait_object: Arc<dyn HttpClient> = client.clone();
        (client, trait_object)
    }

    fn test_list_detail_decoder() -> ListDetailDecoder<u32, serde_json::Value> {
        ListDetailDecoder::builder("test.item.v1")
            .list_url("http://example.invalid/list".parse().unwrap())
            .parse_list(|response| {
                let ids: Vec<u32> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(ids)
            })
            .detail_url(|id: &u32| format!("http://example.invalid/item/{id}").parse().unwrap())
            .parse_item(|response| {
                let item: Option<serde_json::Value> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(item)
            })
            .build()
            .expect("decoder build ok")
    }

    fn test_list_detail_path_decoder() -> ListDetailDecoder<u32, serde_json::Value> {
        ListDetailDecoder::builder("test.item.v1")
            .base_url("http://example.invalid/api/".parse().unwrap())
            .list_path("list")
            .parse_list(|response| {
                let ids: Vec<u32> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(ids)
            })
            .detail_path(|id: &u32| format!("item/{id}"))
            .parse_item(|response| {
                let item: Option<serde_json::Value> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(item)
            })
            .build()
            .expect("decoder build ok")
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
            poll_timeout: Some(Duration::from_secs(120)),
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
            poll_timeout: Some(Duration::from_secs(120)),
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
            poll_timeout: None,
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
            poll_timeout: None,
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

    #[test]
    fn http_pull_config_builder_requires_client() {
        assert!(HttpPullConfig::builder().build().is_err());
    }

    #[test]
    fn http_pull_config_builder_has_sensible_defaults() {
        let (_mock, client) = mock_client();
        let config = HttpPullConfig::builder()
            .client(client)
            .build()
            .expect("build ok");
        assert_eq!(config.default_headers.len(), 0);
        assert_eq!(config.max_batch_size, 10);
        assert_eq!(config.poll_timeout, Some(Duration::from_secs(120)));
        assert_eq!(config.retry.transient_max_retries, 2);
    }

    #[test]
    fn http_pull_config_builder_rejects_zero_max_batch_size() {
        let (_mock, client) = mock_client();
        assert!(HttpPullConfig::builder()
            .client(client)
            .max_batch_size(0)
            .build()
            .is_err());
    }

    #[test]
    fn http_pull_config_builder_header_inserts_default_header() {
        let (_mock, client) = mock_client();
        let config = HttpPullConfig::builder()
            .client(client)
            .header("authorization", "Bearer test")
            .build()
            .expect("build ok");
        assert_eq!(
            config
                .default_headers
                .get("authorization")
                .expect("header present")
                .to_str()
                .expect("utf-8 header value"),
            "Bearer test"
        );
    }

    #[test]
    fn http_pull_config_builder_poll_timeout_opt_allows_disabling_default() {
        let (_mock, client) = mock_client();
        let config = HttpPullConfig::builder()
            .client(client)
            .poll_timeout_opt(None)
            .build()
            .expect("build ok");
        assert_eq!(config.poll_timeout, None);
    }

    #[test]
    fn http_pull_config_builder_applies_overrides() {
        let (_mock, client) = mock_client();
        let retry = HttpRetryConfig {
            transient_max_retries: 0,
            transient_backoff: vec![],
            rate_limit_max_wait: Duration::from_secs(3),
        };

        let config = HttpPullConfig::builder()
            .client(client)
            .max_batch_size(7)
            .retry(retry.clone())
            .poll_timeout(Duration::from_secs(5))
            .build()
            .expect("build ok");
        assert_eq!(config.max_batch_size, 7);
        assert_eq!(
            config.retry.transient_max_retries,
            retry.transient_max_retries
        );
        assert_eq!(config.poll_timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn http_poll_config_builder_requires_poll_interval() {
        let (_mock, client) = mock_client();
        assert!(HttpPollConfig::builder().client(client).build().is_err());
    }

    #[test]
    fn http_poll_config_builder_rejects_zero_max_batch_size() {
        let (_mock, client) = mock_client();
        assert!(HttpPollConfig::builder()
            .client(client)
            .poll_interval(Duration::from_secs(1))
            .max_batch_size(0)
            .build()
            .is_err());
    }

    #[test]
    fn http_poll_config_builder_rejects_zero_poll_interval() {
        let (_mock, client) = mock_client();
        assert!(HttpPollConfig::builder()
            .client(client)
            .poll_interval(Duration::ZERO)
            .build()
            .is_err());
    }

    #[test]
    fn http_poll_config_builder_defaults_poll_timeout_to_none() {
        let (_mock, client) = mock_client();
        let config = HttpPollConfig::builder()
            .client(client)
            .poll_interval(Duration::from_secs(1))
            .build()
            .expect("build ok");
        assert_eq!(config.poll_timeout, None);
        assert_eq!(config.max_batch_size, 10);
    }

    #[test]
    fn http_poll_config_builder_applies_overrides() {
        let (_mock, client) = mock_client();
        let retry = HttpRetryConfig {
            transient_max_retries: 0,
            transient_backoff: vec![],
            rate_limit_max_wait: Duration::from_secs(3),
        };

        let config = HttpPollConfig::builder()
            .client(client)
            .poll_interval(Duration::from_secs(2))
            .max_batch_size(7)
            .retry(retry.clone())
            .poll_timeout(Duration::from_secs(11))
            .build()
            .expect("build ok");
        assert_eq!(config.max_batch_size, 7);
        assert_eq!(
            config.retry.transient_max_retries,
            retry.transient_max_retries
        );
        assert_eq!(config.poll_timeout, Some(Duration::from_secs(11)));
        assert_eq!(config.poll_interval, Duration::from_secs(2));
    }

    #[test]
    fn list_detail_decoder_new_with_list_request_uses_request_spec_from_fn() {
        let decoder = ListDetailDecoder::new_with_list_request(
            "test.item.v1",
            || RequestSpec::post("http://example.invalid/list".parse().unwrap()),
            |response| {
                let ids: Vec<u32> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(ids)
            },
            |id: &u32| {
                RequestSpec::get(format!("http://example.invalid/item/{id}").parse().unwrap())
            },
            |response| {
                let item: Option<serde_json::Value> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(item)
            },
        );

        let req = decoder.request_spec(None);
        assert_eq!(req.method, HttpMethod::Post);
        assert_eq!(req.url.as_str(), "http://example.invalid/list");
    }

    #[test]
    fn list_detail_decoder_builder_requires_parse_item() {
        let result = ListDetailDecoder::<u32, serde_json::Value>::builder("test.item.v1")
            .base_url("http://example.invalid/".parse().unwrap())
            .list_path("list")
            .parse_list(|response| {
                let ids: Vec<u32> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(ids)
            })
            .detail_path(|id: &u32| format!("item/{id}"))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn list_detail_decoder_builder_supports_base_url_path_sugar() {
        let decoder = test_list_detail_path_decoder();

        let req = decoder.request_spec(None);
        assert_eq!(req.url.as_str(), "http://example.invalid/api/list");

        let list = HttpResponse::new(200, HeaderMap::new(), "[7]");
        let out = decoder.decode_success(None, &list).expect("decode ok");
        let cursor = out.next_cursor.expect("cursor expected");

        let req = decoder.request_spec(Some(&cursor));
        assert_eq!(req.url.as_str(), "http://example.invalid/api/item/7");
    }

    #[tokio::test]
    async fn http_pull_source_with_list_detail_decoder_fetches_list_then_items() {
        let (mock, client) = mock_client();

        mock.enqueue(HttpResponse::new(200, HeaderMap::new(), "[1,2]"));
        mock.enqueue(HttpResponse::new(200, HeaderMap::new(), r#"{"n": 1}"#));
        mock.enqueue(HttpResponse::new(200, HeaderMap::new(), r#"{"n": 2}"#));

        let decoder = test_list_detail_path_decoder();

        let config = HttpPullConfig::builder()
            .client(client)
            .max_batch_size(10)
            .build()
            .expect("build ok");

        let mut source = HttpPullSource::new(decoder, config);
        source.bind_writer_id(WriterId::from(StageId::new()));

        let batch1 = source.next().await.unwrap().unwrap();
        let items1: Vec<_> = batch1
            .iter()
            .filter(|e| e.event_type() == "test.item.v1")
            .map(|e| e.payload())
            .collect();
        assert_eq!(items1, vec![serde_json::json!({"n": 1})]);

        let batch2 = source.next().await.unwrap().unwrap();
        let items2: Vec<_> = batch2
            .iter()
            .filter(|e| e.event_type() == "test.item.v1")
            .map(|e| e.payload())
            .collect();
        assert_eq!(items2, vec![serde_json::json!({"n": 2})]);

        let done = source.next().await.unwrap();
        assert!(done.is_none());
    }

    #[test]
    fn list_detail_decoder_fetches_list_then_items_until_exhausted() {
        let decoder = test_list_detail_decoder();

        let list = HttpResponse::new(200, HeaderMap::new(), "[1,2]");
        let out = decoder.decode_success(None, &list).expect("decode ok");
        assert!(out.items.is_empty());
        let cursor = out.next_cursor.expect("cursor expected");

        let req = decoder.request_spec(Some(&cursor));
        assert_eq!(req.url.as_str(), "http://example.invalid/item/1");

        let item = HttpResponse::new(200, HeaderMap::new(), r#"{"n": 1}"#);
        let out = decoder
            .decode_success(Some(&cursor), &item)
            .expect("decode ok");
        assert_eq!(out.items, vec![serde_json::json!({"n": 1})]);
        let cursor = out.next_cursor.expect("cursor expected");

        let req = decoder.request_spec(Some(&cursor));
        assert_eq!(req.url.as_str(), "http://example.invalid/item/2");

        let item = HttpResponse::new(200, HeaderMap::new(), r#"{"n": 2}"#);
        let out = decoder
            .decode_success(Some(&cursor), &item)
            .expect("decode ok");
        assert_eq!(out.items, vec![serde_json::json!({"n": 2})]);
        assert!(out.next_cursor.is_none());
    }

    #[test]
    fn list_detail_decoder_skips_null_items_and_calls_on_skip() {
        let skips = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let skips_for_cb = skips.clone();

        let decoder = ListDetailDecoder::builder("test.item.v1")
            .list_url("http://example.invalid/list".parse().unwrap())
            .parse_list(|response| {
                let ids: Vec<u32> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(ids)
            })
            .detail_url(|id: &u32| format!("http://example.invalid/item/{id}").parse().unwrap())
            .parse_item(|response| {
                let item: Option<serde_json::Value> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(item)
            })
            .on_skip(move |id| {
                assert_eq!(*id, 1);
                skips_for_cb.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
            .build()
            .expect("decoder build ok");

        let list = HttpResponse::new(200, HeaderMap::new(), "[1,2]");
        let out = decoder.decode_success(None, &list).expect("decode ok");
        let cursor = out.next_cursor.expect("cursor expected");

        let item = HttpResponse::new(200, HeaderMap::new(), "null");
        let out = decoder
            .decode_success(Some(&cursor), &item)
            .expect("decode ok");
        assert!(out.items.is_empty());
        assert_eq!(skips.load(std::sync::atomic::Ordering::SeqCst), 1);

        let cursor = out.next_cursor.expect("cursor expected");
        let req = decoder.request_spec(Some(&cursor));
        assert_eq!(req.url.as_str(), "http://example.invalid/item/2");
    }

    #[test]
    fn list_detail_decoder_caps_list_size_with_max_list_items() {
        let decoder = ListDetailDecoder::builder("test.item.v1")
            .list_url("http://example.invalid/list".parse().unwrap())
            .parse_list(|response| {
                let ids: Vec<u32> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(ids)
            })
            .detail_url(|id: &u32| format!("http://example.invalid/item/{id}").parse().unwrap())
            .parse_item(|response| {
                let item: Option<serde_json::Value> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;
                Ok(item)
            })
            .max_list_items(2)
            .build()
            .expect("decoder build ok");

        let list = HttpResponse::new(200, HeaderMap::new(), "[1,2,3]");
        let out = decoder.decode_success(None, &list).expect("decode ok");
        let cursor = out.next_cursor.expect("cursor expected");

        let item = HttpResponse::new(200, HeaderMap::new(), r#"{"n": 1}"#);
        let out = decoder
            .decode_success(Some(&cursor), &item)
            .expect("decode ok");
        let cursor = out.next_cursor.expect("cursor expected");

        let item = HttpResponse::new(200, HeaderMap::new(), r#"{"n": 2}"#);
        let out = decoder
            .decode_success(Some(&cursor), &item)
            .expect("decode ok");
        assert!(out.next_cursor.is_none());
    }
}
