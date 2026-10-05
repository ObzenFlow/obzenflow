// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod csv;
pub mod http;
pub mod http_pull;
mod in_process;

pub use csv::{
    CsvDecodeError, CsvDecoder, CsvReader, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};
pub use http::{HostedIngressSource, HttpSourceConfig, IngressDecodeError, IngressDecoder};
pub use in_process::{ChannelSource, ValuesSource};

pub use http_pull::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollReader, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder,
    HttpPullReader, HttpPullSource, HttpResponse, ListDetailDecoder, ListDetailDecoderBuilder,
    PullDecoder,
};
