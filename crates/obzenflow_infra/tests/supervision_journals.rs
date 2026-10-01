// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, Journal, JournalOwner, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use std::sync::Arc;

fn stage_running(stage: StageId) -> ChainEvent {
    ChainEventFactory::execution_event(
        stage.into(),
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: stage }),
    )
    .with_flow_context(FlowContext::new("child", stage))
}

#[tokio::test]
async fn atomic_group_budgets_reject_before_commit_and_large_groups_stream_in_order() {
    use obzenflow_core::journal::limits::{MAX_GROUP_RECORDS, MAX_RECORD_BYTES};
    for disk in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let stage = StageId::new();
        let journal: Arc<dyn Journal<ChainEvent>> = if disk {
            Arc::new(
                DiskJournal::with_owner(dir.path().join("groups.log"), JournalOwner::stage(stage))
                    .unwrap(),
            )
        } else {
            Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage)))
        };
        let first = journal
            .append(stage_running(stage), Default::default())
            .await
            .unwrap();
        let events = (0..=MAX_GROUP_RECORDS)
            .map(|_| stage_running(stage))
            .collect();
        assert!(journal
            .append_group("too-many", events, Default::default())
            .await
            .is_err());
        let oversized = ChainEventFactory::data_event(
            stage.into(),
            "large",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"body": "x".repeat(MAX_RECORD_BYTES)}),
        );
        assert!(journal.append(oversized, Default::default()).await.is_err());
        let over_bytes = (0..10)
            .map(|_| {
                ChainEventFactory::data_event(
                    stage.into(),
                    "large",
                    std::num::NonZeroU32::MIN,
                    serde_json::json!({"body": "x".repeat(7 * 1024 * 1024)}),
                )
            })
            .collect();
        assert!(journal
            .append_group("too-large", over_bytes, Default::default())
            .await
            .is_err());
        assert_eq!(
            journal.committed_position().await.unwrap(),
            1,
            "rejection must not advance the committed clock"
        );
        let group = journal
            .append_group(
                "valid-large-prefix",
                (0..256).map(|_| stage_running(stage)).collect(),
                Default::default(),
            )
            .await
            .unwrap();
        let mut reader = journal.reader_from(0).await.unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().id(), first.id());
        for (index, row) in group.iter().enumerate() {
            let observed = reader.next().await.unwrap().unwrap();
            assert_eq!(observed.id(), row.id());
            assert_eq!(observed.local_sequence(), index as u64 + 2);
        }
        assert_eq!(journal.committed_position().await.unwrap(), 257);
    }
}
