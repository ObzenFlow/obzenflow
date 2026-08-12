// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink stage implementations
//!
//! Sinks are journal-based stages that process events and write delivery facts.

pub mod journal_sink;
pub mod typed;

pub use crate::stages::common::handlers::sink::{
    DeliveryContext, DeliveryProvenance, InlineSink, PendingSinkInput, SinkAuditOutcome,
    SinkBufferedOutcome, SinkCommitReceipt, SinkConnector, SinkDescription, SinkPrimaryOutcome,
    SinkTerminalOutcome, SinkWriteContext, SinkWriteReport, SinkWriter, SinkWriterInitContext,
    SinkWriterLifecycleReport,
};
pub use typed::{SetSinkRedeliverySafety, SinkTyped};
