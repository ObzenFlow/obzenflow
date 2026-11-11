// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::id::StageId;

use uuid::Uuid;

#[tokio::test]
async fn test_basic_append_and_read() {
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!(
        "target/test-logs/test_basic_append_and_read_{test_id}"
    ));
    std::fs::create_dir_all(&test_dir).unwrap();
    // Create a test journal with a proper owner
    let test_stage_id = obzenflow_core::StageId::new();
    let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
    let log_path = test_dir.join("test_flow_1.log");
    let log =
        DiskJournal::<ChainEvent>::with_owner(log_path, owner, JournalSync::BeforeAck).unwrap();

    let writer_id = WriterId::from(StageId::new());
    let event = ChainEventFactory::data_event(
        writer_id,
        "test.event",
        std::num::NonZeroU32::MIN,
        serde_json::json!({"data": "test value"}),
    );

    // Append event
    let envelope = log.append(event.clone(), Default::default()).await.unwrap();

    // Read back
    let events = log.read_causally_ordered().await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type(), "test.event");
    assert_eq!(events[0].payload()["data"], "test value");

    // Read by ID
    let event_by_id = log
        .read_event(&envelope.envelope.provenance.event.id)
        .await
        .unwrap();
    assert!(event_by_id.is_some());
    assert_eq!(
        event_by_id.unwrap().envelope.provenance.event.id,
        envelope.envelope.provenance.event.id
    );

    // Cleanup
    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn atomic_group_uses_one_frame_and_all_read_surfaces_expand_every_member() {
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/atomic_group_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("group.log");
    let owner = obzenflow_core::JournalOwner::stage(StageId::new());
    let log =
        DiskJournal::<ChainEvent>::with_owner(log_path.clone(), owner, JournalSync::BeforeAck)
            .unwrap();
    let writer_id = WriterId::from(StageId::new());
    let events: Vec<_> = (0..3)
        .map(|index| {
            ChainEventFactory::data_event(
                writer_id,
                "atomic.member",
                std::num::NonZeroU32::MIN,
                serde_json::json!({ "index": index }),
            )
        })
        .collect();
    let ids: Vec<_> = events.iter().map(|event| event.id).collect();

    let written = log
        .append_group("effect-outcome:test", events, Default::default())
        .await
        .expect("atomic group append");
    assert_eq!(written.len(), 3);
    let bytes = std::fs::read(&log_path).unwrap();
    assert_eq!(
        codec::frame::frame_length(&bytes).unwrap(),
        bytes.len(),
        "the whole group must have one physical commit marker"
    );
    codec::frame::validate(&bytes).unwrap();

    let all = log.read_all_unordered().await.unwrap();
    assert_eq!(all.len(), 3);
    for id in ids {
        assert!(log.read_event(&id).await.unwrap().is_some());
    }
    let mut from_second = log.reader_from(1).await.unwrap();
    assert_eq!(
        from_second.next().await.unwrap().unwrap().payload()["index"],
        1
    );
    assert_eq!(log.read_last_n(2).await.unwrap().len(), 2);

    drop(log);
    let reopened = DiskJournal::<ChainEvent>::with_owner(
        log_path,
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    assert_eq!(reopened.read_all_unordered().await.unwrap().len(), 3);
    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn test_causal_ordering() {
    let test_id = Uuid::new_v4();
    let test_dir =
        std::path::PathBuf::from(format!("target/test-logs/test_causal_ordering_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    // Create a test journal with a proper owner
    let test_system_id = obzenflow_core::SystemId::new();
    let owner = obzenflow_core::JournalOwner::system(test_system_id);
    let log_path = test_dir.join("test_flow_2.log");
    let log =
        DiskJournal::<ChainEvent>::with_owner(log_path, owner, JournalSync::BeforeAck).unwrap();

    let writer1 = WriterId::from(StageId::new());
    let writer2 = WriterId::from(StageId::new());

    // First event from writer1
    let event1 = ChainEventFactory::data_event(
        writer1,
        "event.1",
        std::num::NonZeroU32::MIN,
        serde_json::json!({"seq": 1}),
    );
    let envelope1 = log.append(event1, Default::default()).await.unwrap();

    // Second event from writer2, causally dependent on event1
    let event2 = ChainEventFactory::data_event(
        writer2,
        "event.2",
        std::num::NonZeroU32::MIN,
        serde_json::json!({"seq": 2}),
    );
    let envelope2 = log
        .append(
            event2,
            AppendOptions::from_record(Some(&envelope1)).unwrap(),
        )
        .await
        .unwrap();

    // Verify vector clocks show causal relationship
    assert!(CausalOrderingService::happened_before(
        &envelope1.envelope.provenance.journal.vector_clock,
        &envelope2.envelope.provenance.journal.vector_clock
    ));

    // Read all events
    let events = log.read_causally_ordered().await.unwrap();
    assert_eq!(events.len(), 2);

    // Verify causal order
    assert_eq!(
        events[0].envelope.provenance.event.id,
        envelope1.envelope.provenance.event.id
    );
    assert_eq!(
        events[1].envelope.provenance.event.id,
        envelope2.envelope.provenance.event.id
    );

    // Cleanup
    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn read_causally_ordered_fails_loud_on_mid_file_corruption() {
    // FLOWIP-120q: a corrupt committed record must fail loud rather than be
    // silently skipped to produce a shortened stream.
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/read_all_corrupt_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("corrupt.log");
    let writer_id = WriterId::from(StageId::new());

    let log = DiskJournal::<ChainEvent>::with_owner(
        log_path.clone(),
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    for i in 0..3 {
        let e = ChainEventFactory::data_event(
            writer_id,
            "e",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"i": i}),
        );
        log.append(e, Default::default()).await.unwrap();
    }

    // Flip a byte inside the first record's body (mid-file): its CRC no
    // longer matches, so it is committed corruption.
    let mut bytes = std::fs::read(&log_path).unwrap();
    let brace = bytes.iter().position(|&b| b == b'{').unwrap();
    bytes[brace + 5] ^= 0xff;
    std::fs::write(&log_path, &bytes).unwrap();

    assert!(
        log.read_causally_ordered().await.is_err(),
        "mid-file corruption must fail loud"
    );

    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn live_tail_reader_handles_concurrent_appends_without_misalignment() {
    // FLOWIP-120q: a live-tail reader sharing the per-path lock with the
    // writer must read every committed record under concurrent append, never
    // misaligning into a mid-record "malformed" read and never erroring.
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/live_tail_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("tail.log");
    let log = Arc::new(
        DiskJournal::<ChainEvent>::with_owner(
            log_path,
            obzenflow_core::JournalOwner::stage(StageId::new()),
            JournalSync::BeforeAck,
        )
        .unwrap(),
    );
    let writer_id = WriterId::from(StageId::new());

    const N: usize = 50;
    let writer_log = log.clone();
    let writer = tokio::spawn(async move {
        for i in 0..N {
            let e = ChainEventFactory::data_event(
                writer_id,
                "tail.event",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"i": i}),
            );
            writer_log.append(e, Default::default()).await.unwrap();
            tokio::task::yield_now().await;
        }
    });

    let mut reader = log.reader().await.unwrap();
    let mut seen = 0usize;
    let mut polls = 0u32;
    while seen < N {
        match reader.next().await {
            Ok(Some(_)) => seen += 1,
            Ok(None) => {
                tokio::task::yield_now().await;
                polls += 1;
                assert!(polls < 1_000_000, "live-tail reader stalled at {seen}/{N}");
            }
            Err(e) => panic!("live-tail reader must not error on concurrent append: {e}"),
        }
    }
    writer.await.unwrap();
    assert_eq!(seen, N);

    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn live_tail_reader_retains_its_prefix_until_corruption_is_repaired() {
    // A checked physical boundary cannot discharge a missing commitment.
    // Sequential readers retain the last admitted prefix, while the explicit
    // best-effort tail view remains available for observation.
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/resume_corrupt_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("resume.log");
    let writer_id = WriterId::from(StageId::new());

    let log = DiskJournal::<ChainEvent>::with_owner(
        log_path.clone(),
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    for i in 0..3 {
        let e = ChainEventFactory::data_event(
            writer_id,
            "e",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"i": i}),
        );
        log.append(e, Default::default()).await.unwrap();
    }

    // Flip a byte inside the second record's body so its CRC no longer matches: committed corruption
    // with intact records on either side.
    let original = std::fs::read(&log_path).unwrap();
    let mut bytes = original.clone();
    let first_end = codec::frame::frame_length(&bytes).unwrap();
    let second_end = first_end + codec::frame::frame_length(&bytes[first_end..]).unwrap();
    let target = second_end - codec::frame::TRAILER_LEN - 1;
    bytes[target] ^= 0xff;
    std::fs::write(&log_path, &bytes).unwrap();

    let mut reader = log.reader().await.unwrap();
    assert!(
        reader.next().await.unwrap().is_some(),
        "the first record reads cleanly"
    );
    for _ in 0..3 {
        assert!(reader.next().await.is_err());
        assert_eq!(reader.position(), 1);
        assert!(!reader.initial_prefix_complete().unwrap());
    }
    assert_eq!(log.read_last_n(1).await.unwrap()[0].local_sequence(), 3);

    std::fs::write(&log_path, original).unwrap();
    for sequence in 2..=3 {
        assert_eq!(
            reader.next().await.unwrap().unwrap().local_sequence(),
            sequence
        );
        assert_eq!(reader.position(), sequence);
    }
    assert!(reader.initial_prefix_complete().unwrap());

    // Without a checked header, continuation has no known frame boundary.
    // Repeated polls must report the same position instead of searching
    // inside the payload for bytes that resemble another record.
    bytes[first_end + 12] ^= 1;
    std::fs::write(&log_path, &bytes).unwrap();
    let mut reader = log.reader().await.unwrap();
    assert!(reader.next().await.unwrap().is_some());
    for _ in 0..3 {
        assert!(reader.next().await.is_err());
        assert_eq!(reader.position(), 1);
    }

    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn read_event_fails_loud_on_externally_truncated_record() {
    // A committed record truncated externally must fail loud, not return None.
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/read_event_trunc_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("trunc.log");
    let writer_id = WriterId::from(StageId::new());

    let log = DiskJournal::<ChainEvent>::with_owner(
        log_path.clone(),
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    let env = log
        .append(
            ChainEventFactory::data_event(
                writer_id,
                "e",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"i": 1}),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    let id = env.envelope.provenance.event.id;

    // Truncate the committed record's frame (drop part of the commit trailer),
    // leaving the index pointing at now-torn bytes.
    let bytes = std::fs::read(&log_path).unwrap();
    std::fs::write(&log_path, &bytes[..bytes.len() - 5]).unwrap();

    assert!(
        log.read_event(&id).await.is_err(),
        "a truncated indexed record must fail loud, not return None"
    );

    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn the_event_index_waits_for_the_first_read_event() {
    let directory = tempfile::tempdir().expect("temporary journal directory");
    let path = directory.path().join("lazy.log");
    let stage = StageId::new();
    let first = {
        let journal = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        )
        .unwrap();
        let first = journal
            .append(
                crate::journal::observability::tests::event(stage, 1),
                Default::default(),
            )
            .await
            .unwrap();
        assert!(
            journal.index.read().await.is_none(),
            "appends build no index"
        );
        *first.id()
    };

    let journal = DiskJournal::<ChainEvent>::with_owner(
        path,
        JournalOwner::stage(stage),
        JournalSync::BeforeAck,
    )
    .unwrap();
    assert!(
        journal.index.read().await.is_none(),
        "reopening builds no index"
    );
    assert!(journal.read_event(&first).await.unwrap().is_some());
    let second = journal
        .append(
            crate::journal::observability::tests::event(stage, 2),
            Default::default(),
        )
        .await
        .unwrap();
    assert!(journal.read_event(second.id()).await.unwrap().is_some());
    assert_eq!(
        journal.index.read().await.as_ref().map(HashMap::len),
        Some(2),
        "once built, the index follows every append"
    );
}

#[tokio::test]
async fn reader_from_fails_loud_on_mid_file_corruption() {
    // FLOWIP-120t: from_position drives the same dispose path as next(), so a
    // corrupt committed record before the target position fails loud instead
    // of being silently counted and skipped (the old read_line behaviour).
    let test_id = Uuid::new_v4();
    let test_dir =
        std::path::PathBuf::from(format!("target/test-logs/reader_from_corrupt_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("corrupt.log");
    let writer_id = WriterId::from(StageId::new());

    let log = DiskJournal::<ChainEvent>::with_owner(
        log_path.clone(),
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    for i in 0..4 {
        let e = ChainEventFactory::data_event(
            writer_id,
            "e",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"i": i}),
        );
        log.append(e, Default::default()).await.unwrap();
    }

    // Flip a byte in the second record's body so its CRC fails: mid-file
    // committed corruption with intact records on either side.
    let mut bytes = std::fs::read(&log_path).unwrap();
    let first_end = codec::frame::frame_length(&bytes).unwrap();
    let second_end = first_end + codec::frame::frame_length(&bytes[first_end..]).unwrap();
    let target = second_end - codec::frame::TRAILER_LEN - 1;
    bytes[target] ^= 0xff;
    std::fs::write(&log_path, &bytes).unwrap();

    assert!(
        log.reader_from(3).await.is_err(),
        "from_position past a mid-file corrupt record must fail loud"
    );

    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn reader_from_stops_short_at_torn_tail_without_counting_it() {
    // FLOWIP-120t: an unterminated final record (torn tail) is not a
    // committed position, so from_position stops at it rather than counting
    // it as a position the way the old read_line skip did.
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/reader_from_torn_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("torn.log");
    let writer_id = WriterId::from(StageId::new());

    let log = DiskJournal::<ChainEvent>::with_owner(
        log_path.clone(),
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    for i in 0..3 {
        let e = ChainEventFactory::data_event(
            writer_id,
            "e",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"i": i}),
        );
        log.append(e, Default::default()).await.unwrap();
    }

    // Drop the final commit-marker byte: the last record becomes a torn tail.
    let mut bytes = std::fs::read(&log_path).unwrap();
    assert_eq!(*bytes.last().unwrap(), b'O');
    bytes.pop();
    std::fs::write(&log_path, &bytes).unwrap();

    // Only two records are committed; asking for position 3 stops at 2.
    let reader = log.reader_from(3).await.unwrap();
    assert_eq!(
        reader.position(),
        2,
        "a torn tail must not be counted as a committed position"
    );

    std::fs::remove_dir_all(&test_dir).unwrap();
}
