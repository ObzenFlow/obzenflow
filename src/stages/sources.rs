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
//! [`ValuesSource::new`] transfers values into one finite execution. It creates
//! the iterator only on the first live poll and emits one item per poll, without
//! cloning or collecting. A single value uses `ValuesSource::new([value])`.
//! [`ChannelSource::new`] transfers a Tokio receiver into one asynchronous
//! infinite execution. Channel closure is a source error; orderly drain closes
//! the receiver. Use topology fan-out for multiple consumers.
//!
//! ## Construction catalogue
//!
//! | Integration | Entry point |
//! |---|---|
//! | Owned values | [`ValuesSource::new`] |
//! | Tokio receiver | [`ChannelSource::new`] |
//! | CSV or TSV | [`CsvSource::builder`] |
//! | HTTP pull | [`HttpPullSource::new`] |
//! | HTTP polling | [`HttpPollSource::new`] |
//! | Hosted HTTP ingress | `FlowApplication::builder().http_ingress(decoder, config)` |
//!
//! New polling behaviour implements the appropriate typed handler:
//! [`TypedFiniteSourceHandler`], [`TypedAsyncFiniteSourceHandler`],
//! [`TypedInfiniteSourceHandler`] or [`TypedAsyncInfiniteSourceHandler`].
//! Resource-owning integrations use the corresponding connector and reader.
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
//! matching the output ownership used by CSV and HTTP pull decoders. Register
//! it through `FlowApplication::builder().http_ingress(decoder, config)` (or
//! `.ingress` for another transport). Custom hosting can explicitly compose
//! bundles with [`crate::application::ingress::ingress_source`] or
//! [`crate::application::ingress::http_ingress`].
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

/// In-process sources owning values or a channel receiver.
pub use obzenflow_adapters::sources::{ChannelSource, ValuesSource};

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
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};
