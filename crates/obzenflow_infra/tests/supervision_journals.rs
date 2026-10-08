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
async fn physical_group_limits_reject_before_commit_and_members_stream_in_order() {
    use obzenflow_core::journal::limits::MAX_GROUP_RECORDS;
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
        if disk {
            // Inline application payloads exceed the physical 64 MiB frame.
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
        }
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

#[tokio::test]
async fn large_in_process_documents_roundtrip_through_memory_and_reopened_disk() {
    use obzenflow_adapters::sources::ValuesSource;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::stages::source::TypedFiniteSourceHandler;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Document {
        body: String,
    }
    impl TypedPayload for Document {
        const EVENT_TYPE: &'static str = "document";
    }

    for disk in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("documents.log");
        let stage = StageId::new();
        let journal: Arc<dyn Journal<ChainEvent>> = if disk {
            Arc::new(DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap())
        } else {
            Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage)))
        };
        // The public source and event-authoring APIs admit documents above the
        // retired 8 MiB logical cap. Both live and reopened readers must agree.
        let mut source = ValuesSource::new([Document {
            body: "document content ".repeat(600_000),
        }]);
        let document = source.next().unwrap().unwrap().pop().unwrap();
        assert!(document.body.len() > 8 * 1024 * 1024);
        let event = ChainEventFactory::data_event_from(
            stage.into(),
            Document::EVENT_TYPE,
            std::num::NonZeroU32::MIN,
            &document,
        )
        .unwrap();
        let committed = journal.append(event, Default::default()).await.unwrap();
        let mut reader = journal.reader_from(0).await.unwrap();
        let observed = reader.next().await.unwrap().unwrap();
        assert_eq!(observed.id(), committed.id());
        let ChainPayload::Fact(payload) = &observed.payload else {
            panic!("document must remain a business fact");
        };
        assert_eq!(payload["body"].as_str(), Some(document.body.as_str()));
        assert!(reader.next().await.unwrap().is_none());
        drop(reader);
        drop(journal);

        if disk {
            let reopened =
                DiskJournal::<ChainEvent>::with_owner(path, JournalOwner::stage(stage)).unwrap();
            let mut reader = reopened.reader_from(0).await.unwrap();
            let observed = reader.next().await.unwrap().unwrap();
            assert_eq!(observed.id(), committed.id());
            let ChainPayload::Fact(payload) = &observed.payload else {
                panic!("reopened document must remain a business fact");
            };
            assert_eq!(payload["body"].as_str(), Some(document.body.as_str()));
        }
    }
}
