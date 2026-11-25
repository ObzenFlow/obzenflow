// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The framework's file system and CSV register system, and the CSV and YAML
//! sources and the CSV sink that read and write files through them.

pub mod csv;
pub mod register;
mod system;
mod version;
pub mod yaml;

pub use csv::{
    CsvDecodeError, CsvDecoder, CsvPosition, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};
pub use register::{Appended, CsvProjection, CsvRegisterPort, CsvSink, CsvSinkBuilder};
pub use system::{
    CsvFile, CsvFileSystem, CsvLine, CsvPage, CsvRead, FileSystem, YamlFile, YamlFileSystem,
};
pub use yaml::{
    YamlDecodeError, YamlDecoder, YamlLine, YamlNode, YamlPage, YamlRead, YamlRecord,
    YamlSelection, YamlSource, YamlSourceBuilder,
};
