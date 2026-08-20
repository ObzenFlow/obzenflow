// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink stage implementations
//!
//! Sinks are journal-based stages that process events and write delivery facts.

pub mod journal_sink;
mod operation_failure;
pub mod typed;

pub use crate::stages::common::handlers::sink::{
    DeliveryContext, DeliveryProvenance, InlineSink, PendingSinkInput, SinkAuditOutcome,
    SinkBufferedOutcome, SinkCommitReceipt, SinkConnector, SinkDescription,
    SinkDestinationErrorCode, SinkOperationError, SinkOperationErrorConversionError,
    SinkOperationResult, SinkPrimaryOutcome, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteFailure, SinkWriteFailureDisposition, SinkWritePhase, SinkWriteReport,
    SinkWriteResult, SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
};
#[doc(hidden)]
pub use operation_failure::{
    record_sink_lifecycle_operation_failure, SinkLifecycleFailureCommit,
    SinkLifecycleFailureRecorded,
};
pub use typed::{SetSinkRedeliverySafety, SinkTyped};
