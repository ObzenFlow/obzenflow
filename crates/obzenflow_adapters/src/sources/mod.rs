// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sources

pub mod csv;
mod file_system;
mod file_version;
pub mod http;
mod in_process;
pub mod yaml;

pub use csv::{
    CsvDecodeError, CsvDecoder, CsvPosition, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};
pub use file_system::{CsvFile, CsvFileSystem, CsvLine, CsvPage, CsvRead, FileSystem, YamlFile, YamlFileSystem};
pub use http::{HostedSource, HttpSourceConfig, IngressDecodeError, IngressDecoder, IngressRecord};
pub use in_process::{
    ChannelSource, ValueFeed, ValuesRead, ValuesSource,
};
pub use yaml::{
    YamlDecodeError, YamlDecoder, YamlLine, YamlNode, YamlPage, YamlRead, YamlRecord, YamlSelection,
    YamlSource, YamlSourceBuilder,
};
