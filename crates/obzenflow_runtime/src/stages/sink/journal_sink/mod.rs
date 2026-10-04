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

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::messaging::DeliveredRecord;
use crate::stages::common::handlers::UnifiedSinkHandler;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::observer::dispatch::run_sink_delivered_observers;
use crate::stages::observer::SinkDeliverySuccessContext;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryOutcome, DeliveryPayload, DeliveryResult, DeliverySubject, SinkAuditPayload,
    SinkLifecycleOperation,
};
use obzenflow_core::event::{ChainEventFactory, ChainPayload, JournalRecord};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope, WriterId};

/// Dispatch provenance belongs to the input, not to the later write or
/// lifecycle operation that happens to settle a buffered receipt.
#[derive(Clone, Copy)]
pub(crate) struct SinkDeliveryObservation {
    pub scope: MiddlewareExecutionScope,
    pub position: StageInputPosition,
}

/// Called after the journal append and subscription settlement both succeed.
/// Retire terminal metadata even for outcomes that do not notify observers.
pub(super) fn observe_committed_delivery<H: UnifiedSinkHandler>(
    ctx: &mut fsm::JournalSinkResources<H>,
    parent: &DeliveredRecord<ChainPayload>,
    written: &JournalRecord<ChainPayload>,
) {
    if ctx.pending_delivery_observations.is_empty() {
        return;
    }
    let ChainPayload::Delivery(receipt) = &written.payload else {
        return;
    };
    if matches!(receipt.result, DeliveryResult::Buffered { .. }) {
        return;
    }
    let Some(observation) = ctx
        .pending_delivery_observations
        .remove(&receipt.subject.input)
    else {
        return;
    };
    let input = parent.authored();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &ctx.flow_id.to_string(),
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Sink,
    );
    let observation_context = SinkDeliverySuccessContext::new(
        ctx.flow_id,
        &flow_context,
        &input,
        observation.position,
        receipt,
    );
    run_sink_delivered_observers(&ctx.observers, observation.scope, &observation_context);
}

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
