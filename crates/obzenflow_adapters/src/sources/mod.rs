// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod csv;
pub mod http;
pub mod http_pull;
mod in_process;
#[cfg(feature = "yaml")]
pub mod yaml;

pub use csv::{
    CsvDecodeError, CsvDecoder, CsvReader, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};
pub use http::{HostedIngressSource, HttpSourceConfig, IngressDecodeError, IngressDecoder};
pub use in_process::{ChannelSource, ValuesSource};
#[cfg(feature = "yaml")]
pub use yaml::{
    YamlDecodeError, YamlDecoder, YamlReader, YamlRecord, YamlSelection, YamlSource,
    YamlSourceBuilder,
};

pub use http_pull::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollReader, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder,
    HttpPullReader, HttpPullSource, HttpResponse, ListDetailDecoder, ListDetailDecoderBuilder,
    PullDecoder,
};
