// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink constructors, formatting, and custom sink authoring contracts.
//!
//! Sinks are the terminal stages of a pipeline. This module re-exports the
//! built-in destinations and the contracts for implementing application sinks.
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

pub use obzenflow_adapters::sinks::csv::CsvWriter;
/// Console and CSV sinks, formatters, and output configuration.
pub use obzenflow_adapters::sinks::{
    console, debug, json, json_pretty, table, ConsoleSink, CsvProjection, CsvSink, CsvSinkBuilder,
    DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination,
    SnapshotTableFormatter, TableFormatter,
};

pub use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
pub use obzenflow_runtime::effects::SinkRedeliverySafety;
pub use obzenflow_runtime::stages::common::handlers::WithRedeliverySafety;
pub use obzenflow_runtime::stages::sink::{
    DeliveryContext, DeliveryProvenance, InlineSink, PendingSinkInput, SinkAuditOutcome,
    SinkBufferedOutcome, SinkCommitReceipt, SinkConnector, SinkDescription,
    SinkDestinationErrorCode, SinkInputOrder, SinkOperationError,
    SinkOperationErrorConversionError, SinkOperationResult, SinkPrimaryOutcome,
    SinkTerminalOutcome, SinkTyped, SinkWriteContext, SinkWriteFailure,
    SinkWriteFailureDisposition, SinkWritePhase, SinkWriteReport, SinkWriteResult, SinkWriter,
    SinkWriterInitContext, SinkWriterLifecycleReport,
};

/// Feature-gated PostgreSQL sink and its typed parameter-binding surface.
///
/// The connector witnesses its exact input type, so the generic `sink!` arm
/// proves arrow equality before erasing the writer.
#[cfg(feature = "postgres")]
pub use obzenflow_adapters::sinks::postgres;
