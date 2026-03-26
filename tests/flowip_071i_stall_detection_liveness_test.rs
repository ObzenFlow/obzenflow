// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Regression test for stall detection semantics (FLOWIP-071i).
//!
//! This FlowIP exists because the prior patch series was piecemeal and failed to
//! address the root issue:
//! - `02010ff` (PATCH #25): "Prevent false reader_stalled aborts on idle upstreams"
//! - `11fbdfb` (PATCH #26): "Prevent false reader_stalled aborts during EOF checks"
//!
//! Despite those patches, demos like `payment_gateway_resilience_demo` and
//! `hn_ai_digest_demo` still intermittently aborted with:
//! `SystemEventType::ContractStatus { pass: false, reason: Other(\"reader_stalled\") }`.
//!
//! That is fundamentally wrong: a stall is a liveness signal, not a transport
//! contract violation. Emitting it as `ContractStatus(pass=false)` poisons the
//! global contract barrier and causes PipelineSupervisor to abort during running
//! or drain, producing nondeterministic failures that have nothing to do with
//! at-least-once integrity.

use std::sync::Arc;

use obzenflow_core::control_middleware::NoControlMiddleware;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::system_event::SystemEvent;
use obzenflow_core::event::types::{Count, DurationMs, SeqNo};
use obzenflow_core::event::{ChainEvent, ChainEventContent, SystemEventType};
use obzenflow_core::journal::Journal;
use obzenflow_core::{JournalOwner, StageId, WriterId};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime::messaging::upstream_subscription::{
    ContractConfig, ContractStatus, ContractsWiring, ReaderProgress, UpstreamSubscription,
};

#[tokio::test]
async fn stall_detection_does_not_emit_system_contract_failure() {
    let upstream_stage = StageId::new();
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(MemoryJournal::with_owner(
        JournalOwner::stage(upstream_stage),
    ));

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .expect("create subscription");

    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);

    let contract_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage_id)));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage_id)));

    let config = ContractConfig {
        progress_min_events: Count(100),
        progress_max_interval: DurationMs(10_000),
        stall_threshold: DurationMs(100),
        stall_cooloff: DurationMs(0),
        stall_checks_before_emit: 1,
    };

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id,
        contract_journal: contract_journal.clone(),
        config,
        system_journal: Some(system_journal.clone()),
        reader_stage: Some(stage_id),
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    // Simulate a reader that has observed data in the past, then goes quiet long
    // enough to trip the stall threshold.
    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].reader_seq = SeqNo(1);
    reader_progress[0].last_progress_seq = reader_progress[0].reader_seq;
    reader_progress[0].last_read_instant =
        Some(tokio::time::Instant::now() - std::time::Duration::from_millis(250));

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(
        matches!(status, ContractStatus::Stalled(s) if s == upstream_stage),
        "expected stall status to be returned to the stage supervisor"
    );

    // Stall should still be observable via a stage-journal flow signal.
    let contract_events = contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract journal");
    assert!(
        contract_events.iter().any(|env| matches!(
            &env.event.content,
            ChainEventContent::FlowControl(FlowControlPayload::ReaderStalled { .. })
        )),
        "expected ReaderStalled flow control evidence in contract journal"
    );

    // But stall must NOT be emitted as a system ContractStatus failure, since
    // that is interpreted as a hard contract violation by PipelineSupervisor.
    let system_events = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");
    assert!(
        !system_events.iter().any(|env| matches!(
            &env.event.event,
            SystemEventType::ContractStatus { pass: false, .. }
        )),
        "stall detection must not emit ContractStatus(pass=false) into system journal"
    );
}
