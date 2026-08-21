// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink handler components

pub mod connector;
pub mod error;
pub mod traits;
pub mod typed;

pub use connector::{
    InlineSink, SinkConnector, SinkDescription, SinkInputOrder, SinkWriterInitContext,
    WithRedeliverySafety,
};
pub use error::{
    SinkDestinationErrorCode, SinkOperationError, SinkOperationErrorConversionError,
    SinkOperationResult, SinkWriteFailure, SinkWriteFailureDisposition, SinkWritePhase,
    SinkWriteResult,
};
#[doc(hidden)]
pub use traits::UnifiedSinkHandler;
#[doc(hidden)]
pub use traits::{CommitReceipt, SinkConsumeReport, SinkHandler, SinkLifecycleReport};
#[doc(hidden)]
pub use typed::SinkWriterAdapter;
pub use typed::{
    DeliveryContext, DeliveryProvenance, PendingSinkInput, SinkAuditOutcome, SinkBufferedOutcome,
    SinkCommitReceipt, SinkPrimaryOutcome, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    SinkWriter, SinkWriterLifecycleReport,
};
