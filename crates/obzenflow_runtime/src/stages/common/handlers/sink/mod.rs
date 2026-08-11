// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink handler components

pub mod traits;
pub mod typed;

#[doc(hidden)]
pub use traits::UnifiedSinkHandler;
#[doc(hidden)]
pub use traits::{CommitReceipt, SinkConsumeReport, SinkHandler, SinkLifecycleReport};
#[doc(hidden)]
pub use typed::TypedSinkHandlerAdapter;
pub use typed::{
    DeliveryContext, DeliveryProvenance, PendingSinkInput, SinkAuditOutcome, SinkBufferedOutcome,
    SinkDeliveryDeclaration, SinkInputContext, SinkPrimaryOutcome, SinkTerminalOutcome,
    TypedCommitReceipt, TypedSinkConsumeReport, TypedSinkHandler, TypedSinkLifecycleReport,
};
