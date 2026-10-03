// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sinks

pub mod console;
pub mod csv;
#[cfg(feature = "postgres")]
pub mod postgres;

pub use console::{
    ConsoleFormatError, ConsoleOutput, ConsoleSink, DebugFormatter, Formatter, JsonFormatter,
    JsonPrettyFormatter, OutputDestination, SnapshotTableFormatter, TableConsoleSink,
    TableFormatter,
};

pub use csv::{CsvProjection, CsvSink, CsvSinkBuilder};
