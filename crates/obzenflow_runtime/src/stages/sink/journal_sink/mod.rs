// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal sink stage implementation
//!
//! Journal sinks are the standard terminal stages in a pipeline that consume events
//! and write them to external destinations (databases, files, APIs, etc.).
//!
//! Key features:
//! - Flush semantics for data durability
//! - Graceful draining to prevent data loss
//! - Automatic completion tracking

pub mod boundary;
pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, WriterId};

/// Create a sink-authored delivery event at the final journal boundary.
///
/// The runtime owns receipt identity. Handler-supplied destinations are
/// therefore ignored and every delivery row, including lifecycle commit
/// receipts, is stamped from the descriptor snapshot resolved by the builder.
pub(super) fn journalled_delivery_event(
    writer_id: WriterId,
    receipt_destination: &str,
    mut payload: DeliveryPayload,
) -> ChainEvent {
    payload.destination.clear();
    payload.destination.push_str(receipt_destination);
    ChainEventFactory::delivery_event(writer_id, payload)
}

// Re-export public API
pub use boundary::{
    SinkDeliveryAdmission, SinkDeliveryAttemptOutcome, SinkDeliveryBoundary, SinkDeliveryPermit,
    SinkDeliveryRejection, SinkPolicyEvidence, SinkPolicyEvidenceBatch, SinkPolicyEvidenceError,
    MAX_SINK_POLICY_EVIDENCE_ENTRIES,
};
pub use builder::JournalSinkBuilder;
pub use config::JournalSinkConfig;
pub use handle::{JournalSinkHandle, JournalSinkHandleExt};

// Re-export FSM types for users who need them
pub use fsm::{JournalSinkEvent, JournalSinkState};

#[cfg(test)]
mod tests {
    use super::journalled_delivery_event;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::ChainEventContent;
    use obzenflow_core::{StageId, WriterId};

    #[test]
    fn final_journal_boundary_overwrites_handler_authored_destination() {
        let mut payload = DeliveryPayload::success(DeliveryMethod::Noop, None);
        payload.destination = "handler-owned".to_string();

        let event = journalled_delivery_event(
            WriterId::from(StageId::new()),
            "descriptor.snapshot",
            payload,
        );
        let ChainEventContent::Delivery(payload) = event.content else {
            panic!("delivery factory must create a delivery event");
        };
        assert_eq!(payload.destination, "descriptor.snapshot");
    }
}
