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
//! [`CsvSink`] (via [`CsvSinkBuilder`]) writes events to CSV files on disk.
//! The output destination is controlled by [`OutputDestination`].

/// Console and CSV sinks, formatters, and output configuration.
pub use obzenflow_adapters::sinks::{
    ConsoleSink, CsvSink, CsvSinkBuilder, DebugFormatter, Formatter, JsonFormatter,
    JsonPrettyFormatter, OutputDestination, TableFormatter,
};
