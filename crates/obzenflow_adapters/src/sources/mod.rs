// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod csv;
mod functional;
pub mod http;
pub mod http_pull;

pub use csv::{
    CsvDecodeError, CsvDecoder, CsvReader, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};
pub use functional::{
    async_finite, async_infinite, finite, finite_from_fn, from_receiver, generate, infinite, once,
};
pub use http::{HostedIngressSource, HttpSourceConfig, IngressDecodeError, IngressDecoder};

pub use http_pull::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollReader, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder,
    HttpPullReader, HttpPullSource, HttpResponse, ListDetailDecoder, ListDetailDecoderBuilder,
    PullDecoder,
};
