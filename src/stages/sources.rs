// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Source constructors and custom typed source handlers.
//!
//! Sources are the entry points of every pipeline. This module re-exports the
//! built-in sources and the contracts for implementing application sources.
//!
//! ## In-process sources
//!
//! [`once`], [`finite`], [`finite_from_fn`], [`async_finite`], [`infinite`], and
//! [`async_infinite`] construct source adapters from application-owned values or
//! producer functions. [`generate`] accepts `FnMut() -> Option<T>` and
//! [`from_receiver`] transfers a Tokio receiver. Producers and receivers move into
//! one execution without `Clone`; use topology fan-out for multiple consumers.
//!
//! ## CSV sources
//!
//! [`CsvSource`] (via [`CsvSourceBuilder`]) is cold, reusable configuration.
//! The supervisor opens an independent [`CsvReader`] only when live input is
//! required. Building and cloning it do no file I/O, so strict replay works even
//! when the original CSV is unavailable. Input/header errors occur at startup.
//! A user-owned [`CsvDecoder`] value declares the emitted [`CsvDecoder::Output`].
//! Its default method uses serde when the CSV and domain shapes match;
//! [`CsvRowDecoder`] provides string-preserving [`CsvRow`] output.
//!
//! ## Hosted ingress sources
//!
//! [`HostedIngressSource`] receives admitted push submissions. A user-owned
//! [`IngressDecoder`] value declares the emitted [`IngressDecoder::Output`],
//! matching the output ownership used by CSV and HTTP pull decoders. Use the
//! [`crate::application::ingress::ingress_source`] or
//! [`crate::application::ingress::http_ingress`] to construct it.
//!
//! ## HTTP pull sources
//!
//! [`HttpPullSource`] configures a finite paginated HTTP source and
//! [`HttpPollSource`] configures repeated polling with [`HttpPollConfig`]. Each
//! opens an independent reader under supervision. [`PullDecoder::Output`]
//! declares the domain type; the runtime owns subsequent poll requests.
//!
//! Integration authors implement one of [`FiniteSourceConnector`],
//! [`AsyncFiniteSourceConnector`], [`InfiniteSourceConnector`] or
//! [`AsyncInfiniteSourceConnector`]. Each returns its corresponding typed handler
//! and receives only [`SourceReaderInitContext`], never runtime controls.
//!
//! The default HTTP client requires the `http-pull` feature.
//! Applications supplying their own [`HttpClient`] do not require that feature.

/// CSV file source, decoder contract, and string-preserving row support.
pub use obzenflow_adapters::sources::{
    CsvDecodeError, CsvDecoder, CsvReader, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};

/// In-process source adapters constructed from values and producer functions.
pub use obzenflow_adapters::sources::{
    async_finite, async_infinite, finite, finite_from_fn, from_receiver, generate, infinite, once,
};

pub use obzenflow_adapters::sources::http_pull::{HttpRetryConfig, ListDetailState};
/// Hosted-ingress source and its application-owned decoder contract.
pub use obzenflow_adapters::sources::{HostedIngressSource, IngressDecodeError, IngressDecoder};

/// HTTP pull and poll sources, decoders, and configuration types.
pub use obzenflow_adapters::sources::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollReader, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder,
    HttpPullReader, HttpPullSource, HttpResponse, ListDetailDecoder, ListDetailDecoderBuilder,
    PullDecoder,
};

/// HTTP primitives re-exported from `obzenflow_core` for building request specs.
pub use obzenflow_core::http_client::{
    Bytes, HeaderMap, HttpClient, HttpClientError, HttpMethod, RequestSpec, Url,
};

/// Default HTTP pull and poll configuration composed by the infra layer.
pub use obzenflow_infra::http_client::{
    http_poll_config, http_pull_config, HttpClientFactoryError,
};

pub use obzenflow_runtime::stages::common::handlers::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
pub use obzenflow_runtime::stages::source::{
    AsyncFiniteSourceConnector, AsyncFiniteSourceTyped, AsyncInfiniteSourceConnector,
    AsyncInfiniteSourceTyped, FallibleAsyncFiniteSourceTyped, FallibleAsyncInfiniteSourceTyped,
    FallibleFiniteSourceTyped, FallibleInfiniteSourceTyped, FiniteSourceConnector,
    FiniteSourceTyped, InfiniteSourceConnector, InfiniteSourceTyped, SourceReaderInitContext,
};
