// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common sink implementations (FLOWIP-083 series).

pub use obzenflow_adapters::sinks::{
    ConsoleSink, CsvSink, CsvSinkBuilder, DebugFormatter, Formatter, JsonFormatter,
    JsonPrettyFormatter, OutputDestination, TableFormatter,
};
