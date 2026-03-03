// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sinks

pub mod console;
pub mod csv;

pub use console::{
    ConsoleSink, DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination,
    SnapshotTableFormatter, TableFormatter,
};

pub use csv::{CsvSink, CsvSinkBuilder};
