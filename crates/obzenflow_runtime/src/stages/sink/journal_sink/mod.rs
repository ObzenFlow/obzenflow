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

use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryOutcome, DeliveryPayload, DeliverySubject, SinkAuditPayload, SinkLifecycleOperation,
};
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
    parent: &crate::messaging::DeliveredRecord<obzenflow_core::event::ChainPayload>,
    mut payload: DeliveryOutcome,
) -> ChainEvent {
    payload.destination.clear();
    payload.destination.push_str(receipt_destination);
    ChainEventFactory::delivery_event(
        writer_id,
        DeliveryPayload {
            subject: DeliverySubject::from_record(parent.record()),
            outcome: payload,
        },
    )
    .with_causality(
        obzenflow_core::event::provenance::causality_context::CausalityContext::with_parent(
            *parent.id(),
        ),
    )
}

pub(super) fn journalled_sink_audit(
    writer_id: WriterId,
    destination: &str,
    operation: SinkLifecycleOperation,
    mut outcome: DeliveryOutcome,
) -> ChainEvent {
    outcome.destination = destination.to_string();
    ChainEventFactory::execution_event(
        writer_id,
        obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SinkAudit(
            SinkAuditPayload { operation, outcome },
        ),
    )
}

/// The retained receipt describes the prefix that will exist if its append
/// succeeds. Live counters are updated separately after acknowledgement.
pub(super) fn with_committed_receipt_snapshot(
    event: ChainEvent,
    instrumentation: &crate::metrics::instrumentation::StageInstrumentation,
) -> ChainEvent {
    let mut snapshot = instrumentation.capture_accounting();
    snapshot.accounting.events_emitted_total =
        snapshot.accounting.events_emitted_total.saturating_add(1);
    snapshot.project_emission(&event);
    snapshot.attach_to(event)
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
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryOutcome};
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::{StageId, WriterId};

    #[test]
    fn final_journal_boundary_overwrites_handler_authored_destination() {
        let mut payload = DeliveryOutcome::success(DeliveryMethod::Noop, None);
        payload.destination = "handler-owned".to_string();

        let event = journalled_delivery_event(
            WriterId::from(StageId::new()),
            "descriptor.snapshot",
            &crate::testing::causal_fixture::committed_input(
                obzenflow_core::JournalWriterId::new(),
                obzenflow_core::event::ChainEventFactory::data_event(
                    StageId::new().into(),
                    "test.input",
                    std::num::NonZeroU32::MIN,
                    serde_json::json!({}),
                ),
            )
            .into(),
            payload,
        );
        let ChainPayload::Delivery(payload) = event.payload else {
            panic!("delivery factory must create a delivery event");
        };
        assert_eq!(payload.destination, "descriptor.snapshot");
    }
}
