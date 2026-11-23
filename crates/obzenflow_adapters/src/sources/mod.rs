// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod csv;
mod file_system;
mod file_version;
pub mod http;
pub mod http_pull;
mod in_process;
pub mod yaml;

pub use csv::{
    CsvDecodeError, CsvDecoder, CsvPosition, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};
pub use file_system::{CsvFile, CsvLine, CsvRead, FileSystem, YamlFile};
pub use http::{HostedSource, HttpSourceConfig, IngressDecodeError, IngressDecoder, IngressRecord};
pub use in_process::{
    ChannelFeed, ChannelRead, ChannelSource, ValueFeed, ValuesRead, ValuesSource,
};
pub use yaml::{
    YamlDecodeError, YamlDecoder, YamlLine, YamlNode, YamlRead, YamlRecord, YamlSelection,
    YamlSource, YamlSourceBuilder,
};

pub use http_pull::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollReader, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder,
    HttpPullReader, HttpPullSource, HttpResponse, ListDetailDecoder, ListDetailDecoderBuilder,
    PullDecoder,
};
