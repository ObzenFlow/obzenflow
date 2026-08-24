// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ready-to-use sink adapters for outputting data from a flow.
//!
//! Sinks are the terminal stages of a pipeline. This module re-exports the
//! built-in sink implementations from [`obzenflow_adapters::sinks`] so that
//! most applications only need `obzenflow` in their dependency list.
//!
//! ## Console sinks
//!
//! [`ConsoleSink`] prints events to stdout using a pluggable [`Formatter`].
//! Built-in formatters include [`DebugFormatter`], [`JsonFormatter`],
//! [`JsonPrettyFormatter`], and [`TableFormatter`].
//!
//! ## CSV sinks
//!
//! [`CsvSink`] writes typed events to CSV files on disk. A user-owned
//! [`CsvProjection`] value declares the accepted input and CSV-facing row with
//! associated types, matching the framework's handler style.

/// Console and CSV sinks, formatters, and output configuration.
pub use obzenflow_adapters::sinks::{
    console, debug, json, json_pretty, table, ConsoleSink, CsvProjection, CsvSink, CsvSinkBuilder,
    DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination,
    TableFormatter,
};

/// Feature-gated PostgreSQL sink and its typed parameter-binding surface.
///
/// The connector witnesses its exact input type, so the generic `sink!` arm
/// proves arrow equality before erasing the writer.
#[cfg(feature = "postgres")]
pub mod postgres {
    pub use obzenflow_adapters::sinks::postgres::*;
}
