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
//! producer functions.
//!
//! ## CSV sources
//!
//! [`CsvSource`] (via [`CsvSourceBuilder`]) reads rows from CSV files on disk.
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
//! [`HttpPullSource`] performs a single HTTP request and decodes the response
//! body using a [`PullDecoder`], whose associated [`PullDecoder::Output`]
//! declares the emitted domain type. [`HttpPollSource`] wraps the same logic in
//! a polling loop controlled by [`HttpPollConfig`].
//!
//! The default HTTP client requires the `http-pull` feature.
//! Applications supplying their own [`HttpClient`] do not require that feature.

/// CSV file source, decoder contract, and string-preserving row support.
pub use obzenflow_adapters::sources::{
    CsvDecodeError, CsvDecoder, CsvRecord, CsvRow, CsvRowDecoder, CsvSource, CsvSourceBuilder,
};

/// In-process source adapters constructed from values and producer functions.
pub use obzenflow_adapters::sources::{
    async_finite, async_infinite, finite, finite_from_fn, infinite, once,
};

pub use obzenflow_adapters::sources::http_pull::{HttpRetryConfig, ListDetailState};
/// Hosted-ingress source and its application-owned decoder contract.
pub use obzenflow_adapters::sources::{HostedIngressSource, IngressDecodeError, IngressDecoder};

/// HTTP pull and poll sources, decoders, and configuration types.
pub use obzenflow_adapters::sources::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder, HttpPullSource,
    HttpResponse, ListDetailDecoder, ListDetailDecoderBuilder, PullDecoder,
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
    AsyncFiniteSourceTyped, AsyncInfiniteSourceTyped, FallibleAsyncFiniteSourceTyped,
    FallibleAsyncInfiniteSourceTyped, FallibleFiniteSourceTyped, FallibleInfiniteSourceTyped,
    FiniteSourceTyped, InfiniteSourceTyped,
};
