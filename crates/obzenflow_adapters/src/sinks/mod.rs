// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sinks

pub mod console;
pub mod csv;
pub mod discard;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod tracing;

pub use console::{
    ConsoleFormatError, ConsoleOutput, ConsoleSink, ConsoleWriter, DebugFormatter, Formatter,
    JsonFormatter, JsonPrettyFormatter, OutputDestination, SnapshotTableFormatter,
    TableConsoleSink, TableFormatter,
};

pub use csv::{CsvProjection, CsvSink, CsvSinkBuilder};
pub use discard::DiscardSink;
pub use tracing::TracingSink;
