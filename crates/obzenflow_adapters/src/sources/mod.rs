// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod csv;
pub mod http;
pub mod http_pull;

pub use csv::{CsvRow, CsvSource, CsvSourceBuilder};
pub use http::{HttpSource, HttpSourceConfig};

pub use http_pull::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollConfigBuilder, HttpPollSource, HttpPullConfig, HttpPullConfigBuilder, HttpPullSource,
    HttpResponse, ListDetailDecoder, PullDecoder,
};
