// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk::log_record::serialize_atomic_group;
use crate::journal::disk::observations::DiskObservationReader;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::id::{JournalId, StageId};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::{Journal, JournalConfig, ObservabilityPolicy};
use obzenflow_core::{FlowId, WriterId};
use tokio::sync::Barrier;

use uuid::Uuid;

#[test]
fn ordinary_reader_lock_wait_leaves_blocking_worker_available_for_append() {
    use obzenflow_core::event::payloads::execution_payload::{
        ExecutionPayload, StageLifecycleFact,
    };
    use obzenflow_core::event::provenance::FlowContext;
    use std::time::Duration;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for grouped in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let stage = StageId::new();
            let journal = DiskJournal::<ChainEvent>::with_owner(
                directory.path().join("lock-wait.log"),
                JournalOwner::stage(stage),
                JournalSync::BeforeAck,
            )
            .unwrap();
            let mut reader = journal.reader_from(0).await.unwrap();
            let make_report = || {
                ChainEventFactory::execution_event(
                    stage.into(),
                    ExecutionPayload::StageLifecycle(StageLifecycleFact::Running {
                        stage_id: stage,
                    }),
                )
                .with_flow_context(FlowContext::new("child", stage))
            };
            // Pause the actual append after it acquires the journal write
            // lock, before it can prepare/submit its physical write. Calling
            // the private commit unit permits safe cleanup of the old bug
            // before any append job has been submitted.
            let preparation_gate = journal.last_commit.write().await;
            let frontier = CausalFrontier::default();
            let mut append = Box::pin(async {
                if grouped {
                    journal
                        .append_records("reports", vec![make_report(), make_report()], &frontier)
                        .await
                } else {
                    journal
                        .append_record(make_report(), &frontier)
                        .await
                        .map(|row| vec![row])
                }
            });
            assert!(futures::poll!(append.as_mut()).is_pending());
            assert!(journal.read_write_lock.try_read().is_err());
            {
                let mut next = Box::pin(reader.next());
                assert!(futures::poll!(next.as_mut()).is_pending());
            }
            // This job is behind any submitted reader job. The writer must
            // be able to use this sole worker without first releasing its
            // write lock. Release all gates before asserting on failure so
            // a regression cannot hang runtime teardown.
            let deadline = Duration::from_secs(5);
            let mut worker = tokio::task::spawn_blocking(|| ());
            let available = tokio::time::timeout(deadline, &mut worker).await;
            if available.is_err() {
                drop(append);
                drop(preparation_gate);
                drop(reader);
                tokio::time::timeout(deadline, worker)
                    .await
                    .expect("reader cleanup releases the worker")
                    .unwrap();
                panic!("ordinary reader lock wait occupied the worker required by the append");
            }
            available.unwrap().unwrap();
            drop(preparation_gate);
            let receipts = tokio::time::timeout(deadline, append)
                .await
                .expect("append must finish with one blocking worker")
                .unwrap();
            for receipt in &receipts {
                let scan = tokio::time::timeout(deadline, reader.next())
                    .await
                    .unwrap()
                    .unwrap();
                let Some(row) = scan else {
                    panic!("committed report must be delivered");
                };
                assert_eq!(row.id(), receipt.id());
                assert_eq!(reader.position(), receipt.local_sequence());
            }
            assert!(reader.next().await.unwrap().is_none());
            assert!(reader.is_at_end());
            assert_eq!(
                journal.committed_position().await.unwrap(),
                receipts.len() as u64
            );
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_append_retains_index_clock_and_writer_serialisation() {
    use futures::FutureExt;
    for grouped in [false, true] {
        let directory = tempfile::tempdir().expect("temporary journal directory");
        let path = directory.path().join("retained.log");
        let stage = StageId::new();
        let writer = WriterId::from(stage);
        let journal = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            obzenflow_core::JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        )
        .unwrap();
        journal
            .configure(JournalConfig {
                observability: ObservabilityPolicy::Periodic {
                    interval: std::time::Duration::from_secs(60),
                },
            })
            .unwrap();
        // Hold metadata publication after the physical frame is written.
        let index_guard = journal.index.write().await;
        let first = crate::journal::observability::tests::event(stage, 1);
        let second = crate::journal::observability::tests::event(stage, 2);
        let observation_reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        let first_id = first.id;
        let pending_journal = journal.clone();
        let mut receipt = Box::pin(async move {
            if grouped {
                pending_journal
                    .append_group("retained", vec![first, second], Default::default())
                    .await
                    .map(|_| ())
            } else {
                pending_journal
                    .append(first, Default::default())
                    .await
                    .map(|_| ())
            }
        });
        assert!(futures::poll!(receipt.as_mut()).is_pending());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while std::fs::metadata(&path).unwrap().len() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Bytes are present, but the retained commit has not published its
        // protected metadata. Neither lookup nor a second writable opener
        // may promote this frame (or individual group members) into view.
        assert!(DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck
        )
        .is_err());
        let reopened = journal.clone();
        let physical_len = std::fs::metadata(&path).unwrap().len();
        for reader in [
            &observation_reader as &dyn obzenflow_core::journal::JournalObservationReader,
            reopened.observation_reader().unwrap(),
        ] {
            assert!(matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), reader.latest_observations(writer)).await.unwrap().unwrap(),
                obzenflow_core::journal::ObservationLookup::Ready { committed_len: 0, observation } if observation.is_empty()
            ));
        }
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            physical_len,
            "opening another handle must not truncate an unsettled append"
        );
        drop(receipt);
        let next = crate::journal::observability::tests::event(stage, 3);
        let mut next_receipt = Box::pin(journal.append(next, Default::default()));
        assert!(next_receipt.as_mut().now_or_never().is_none());
        assert!(journal.read_write_lock.try_read().is_err());
        drop(index_guard);
        let appended = tokio::time::timeout(std::time::Duration::from_secs(5), next_receipt)
            .await
            .unwrap()
            .unwrap();
        assert!(appended.envelope.observability.is_none());
        assert_eq!(
            appended
                .envelope
                .provenance
                .journal
                .vector_clock
                .get(&appended.causal_coordinate()),
            if grouped { 3 } else { 2 }
        );
        assert!(journal.read_event(&first_id).await.unwrap().is_some());
        assert_eq!(
            journal.read_all_unordered().await.unwrap().len(),
            if grouped { 3 } else { 2 }
        );
        for reader in [
            &observation_reader as &dyn obzenflow_core::journal::JournalObservationReader,
            reopened.observation_reader().unwrap(),
        ] {
            let obzenflow_core::journal::ObservationLookup::Ready {
                committed_len,
                observation,
            } = reader.latest_observations(writer).await.unwrap()
            else {
                panic!("the confirmed boundary must be shared across handles");
            };
            assert_eq!(committed_len, if grouped { 3 } else { 2 });
            assert_eq!(
                observation.len(),
                1,
                "the periodic gate selects one packet for the whole group"
            );
        }
    }
}

#[tokio::test]
async fn failed_appends_do_not_publish_observations_and_reopen_recovers_with_a_surviving_reader() {
    use obzenflow_core::journal::{JournalObservationReader, ObservationLookup};
    use std::io::Write;

    for grouped in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("failed.log");
        let stage = StageId::new();
        let journal = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        )
        .unwrap();
        let observed = crate::journal::observability::tests::event(stage, 1);
        let expected = journal
            .append(observed.clone(), Default::default())
            .await
            .unwrap();
        let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        let committed_end = std::fs::metadata(&path).unwrap().len();
        // A read-only descriptor makes both writing and rollback fail through
        // the real append path, including retained commitment and poisoning.
        *journal.write_file.lock().unwrap() = StdFile::open(&path).unwrap();
        let failed = if grouped {
            journal
                .append_group(
                    "failed",
                    vec![observed.clone(), observed.clone()],
                    Default::default(),
                )
                .await
                .map(|_| ())
        } else {
            journal
                .append(observed.clone(), Default::default())
                .await
                .map(|_| ())
        };
        assert!(matches!(
            failed,
            Err(JournalError::CommitIndeterminate { .. })
        ));
        assert!(journal.poisoned.load(Ordering::SeqCst));
        assert!(
            matches!(reader.latest_observations(stage.into()).await.unwrap(),
            ObservationLookup::Ready { committed_len: 1, observation } if observation.len() == 1)
        );
        drop(journal);

        // Simulate an indeterminate append's torn bytes. The observation
        // handle outlives the writer; it must not disable recovery on reopen.
        Write::write_all(
            &mut StdFile::options().append(true).open(&path).unwrap(),
            &crate::journal::disk::codec::frame::MAGIC,
        )
        .unwrap();
        assert!(matches!(
            reader.latest_observations(stage.into()).await.unwrap(),
            ObservationLookup::Ready {
                committed_len: 1,
                ..
            }
        ));
        let reopened = DiskJournal::<ChainEvent>::new(
            directory.path().to_owned(),
            "failed",
            JournalSync::BeforeAck,
        )
        .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), committed_end);
        assert_eq!(
            serde_json::to_value(reopened.read_all_unordered().await.unwrap()).unwrap(),
            serde_json::to_value(vec![expected]).unwrap()
        );
        drop(reopened);
        let writable =
            DiskJournal::with_owner(path, JournalOwner::stage(stage), JournalSync::BeforeAck)
                .unwrap();
        writable.append(observed, Default::default()).await.unwrap();
        assert!(matches!(
            reader.latest_observations(stage.into()).await.unwrap(),
            ObservationLookup::Ready {
                committed_len: 2,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn confirmed_observation_prefix_does_not_cross_an_indeterminate_frame() {
    use obzenflow_core::journal::{JournalObservationReader, ObservationLookup};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("indeterminate.log");
    let stage = StageId::new();
    let first = DiskJournal::<ChainEvent>::with_owner(
        path.clone(),
        JournalOwner::stage(stage),
        JournalSync::BeforeAck,
    )
    .unwrap();
    let committed = first
        .append(
            crate::journal::observability::tests::event(stage, 1),
            Default::default(),
        )
        .await
        .unwrap();
    let second = first.clone();
    let observer = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
    let identity = crate::journal::disk::identity::read_identity(&path).unwrap();
    let mut previous = Some(JournalClock::from_record(&committed).unwrap());
    let uncertain = crate::journal::disk::identity::fixture_record(
        identity,
        crate::journal::observability::tests::event(stage, 2),
        &mut previous,
    );
    let complete_frame = crate::journal::disk::log_record::serialize_record(&uncertain).unwrap();
    let failing = first.clone();
    // Retain a complete frame, then panic before commitment bookkeeping.
    let result = retain_commit::<()>(first.poisoned.clone(), async move {
        let _guard = failing.read_write_lock.write().await;
        let mut file = failing.write_file.lock().unwrap();
        FrameSink::write_all(&mut *file, &complete_frame).unwrap();
        FrameSink::sync(&mut *file).unwrap();
        panic!("commit bookkeeping fault after complete frame write");
    })
    .await;
    assert!(matches!(
        result,
        Err(JournalError::CommitIndeterminate { .. })
    ));
    // All clones reject writes after an indeterminate publication.
    assert!(second
        .append(
            crate::journal::observability::tests::event(stage, 3),
            Default::default()
        )
        .await
        .is_err());
    assert!(matches!(
        second
            .observation_reader()
            .unwrap()
            .latest_observations(stage.into())
            .await
            .unwrap(),
        ObservationLookup::Ready {
            committed_len: 1,
            ..
        }
    ));
    drop(first);
    drop(second);
    let recovered = DiskJournal::<ChainEvent>::with_owner(
        path,
        JournalOwner::stage(stage),
        JournalSync::BeforeAck,
    )
    .unwrap();
    assert_eq!(recovered.read_all_unordered().await.unwrap().len(), 2);
    assert!(matches!(
        observer.latest_observations(stage.into()).await.unwrap(),
        ObservationLookup::Ready {
            committed_len: 2,
            ..
        }
    ));
}

#[tokio::test]
async fn test_concurrent_writers() {
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!(
        "target/test-logs/test_concurrent_writers_{test_id}"
    ));
    std::fs::create_dir_all(&test_dir).unwrap();
    // Create a test journal with a proper owner
    let test_system_id = obzenflow_core::SystemId::new();
    let owner = obzenflow_core::JournalOwner::system(test_system_id);
    let log_path = test_dir.join("test_flow_3.log");
    let log = Arc::new(
        DiskJournal::<ChainEvent>::with_owner(log_path, owner, JournalSync::BeforeAck).unwrap(),
    );

    // Spawn multiple concurrent writers
    let mut handles = vec![];

    for i in 0..5 {
        let log_clone = log.clone();
        let handle = tokio::spawn(async move {
            let writer_id = WriterId::from(StageId::new());
            let event = ChainEventFactory::data_event(
                writer_id,
                "concurrent.event",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"writer": i}),
            );
            log_clone.append(event, Default::default()).await
        });
        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    // Verify all events were written
    let events = log.read_causally_ordered().await.unwrap();
    assert_eq!(events.len(), 5);

    // Verify each event has unique writer
    let writer_ids: std::collections::HashSet<_> = events
        .iter()
        .map(|e| e.writer_id().as_ulid().to_string())
        .collect();
    assert_eq!(writer_ids.len(), 5);

    // Cleanup
    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[tokio::test]
async fn test_same_writer_concurrent_appends_have_unique_writer_seq() {
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!(
        "target/test-logs/test_same_writer_concurrent_appends_have_unique_writer_seq_{test_id}"
    ));
    std::fs::create_dir_all(&test_dir).unwrap();

    let test_stage_id = obzenflow_core::StageId::new();
    let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
    let log_path = test_dir.join("same_writer_concurrent.log");
    let log = Arc::new(
        DiskJournal::<ChainEvent>::with_owner(log_path, owner, JournalSync::BeforeAck).unwrap(),
    );

    let writer_id = WriterId::from(StageId::new());

    let task_count: usize = 20;
    let barrier = Arc::new(Barrier::new(task_count));

    let mut handles = Vec::with_capacity(task_count);
    for i in 0..task_count {
        let log_clone = log.clone();
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            let event = ChainEventFactory::data_event(
                writer_id,
                "concurrent.same_writer",
                std::num::NonZeroU32::MIN,
                serde_json::json!({ "i": i }),
            );
            log_clone.append(event, Default::default()).await
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let events = log.read_causally_ordered().await.unwrap();
    assert_eq!(events.len(), task_count);

    let writer_seqs: Vec<u64> = events
        .iter()
        .map(|e| {
            e.envelope
                .provenance
                .journal
                .vector_clock
                .get(&CausalCoordinate::new(
                    e.envelope.provenance.journal.journal_writer_id,
                ))
        })
        .collect();

    let expected: Vec<u64> = (1..=task_count as u64).collect();
    assert_eq!(writer_seqs, expected);

    std::fs::remove_dir_all(&test_dir).unwrap();
}

#[derive(Clone, Copy)]
enum FailMode {
    Ok,
    FailBeforeWrite,
    PartialThenFail(usize),
    FailOnSync,
    FailWriteAndRollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkCall {
    WriteAll,
    Sync,
    Rollback,
}

/// In-memory FrameSink so append_frame's commit/rollback path is tested
/// without inducing real filesystem failures (FLOWIP-120t).
struct MockSink {
    buf: Vec<u8>,
    mode: FailMode,
    calls: Vec<SinkCall>,
}

impl MockSink {
    fn new(mode: FailMode) -> Self {
        Self {
            buf: b"PRE".to_vec(),
            mode,
            calls: Vec::new(),
        }
    }
}

impl FrameSink for MockSink {
    fn end_offset(&mut self) -> std::io::Result<u64> {
        Ok(self.buf.len() as u64)
    }
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.calls.push(SinkCall::WriteAll);
        match self.mode {
            FailMode::FailBeforeWrite => Err(std::io::Error::other("fail before write")),
            FailMode::PartialThenFail(k) => {
                let n = k.min(bytes.len());
                self.buf.extend_from_slice(&bytes[..n]);
                Err(std::io::Error::other("fail after partial write"))
            }
            FailMode::FailWriteAndRollback => {
                self.buf.extend_from_slice(&bytes[..1]);
                Err(std::io::Error::other("fail after partial write"))
            }
            FailMode::Ok | FailMode::FailOnSync => {
                self.buf.extend_from_slice(bytes);
                Ok(())
            }
        }
    }
    fn sync(&mut self) -> std::io::Result<()> {
        self.calls.push(SinkCall::Sync);
        match self.mode {
            FailMode::FailOnSync => Err(std::io::Error::other("fail on sync")),
            _ => Ok(()),
        }
    }
    fn rollback_to(&mut self, offset: u64) -> std::io::Result<()> {
        self.calls.push(SinkCall::Rollback);
        match self.mode {
            FailMode::FailWriteAndRollback => Err(std::io::Error::other("rollback failed")),
            _ => {
                self.buf.truncate(offset as usize);
                Ok(())
            }
        }
    }
}

#[test]
fn append_frame_commits_on_success() {
    let mut sink = MockSink::new(FailMode::Ok);
    let rec = b"record-bytes";
    let committed =
        append_frame(&mut sink, rec, Path::new("x.log"), JournalSync::BeforeAck).unwrap();
    assert_eq!(committed.offset, 3);
    assert_eq!(committed.next_offset, 3 + rec.len() as u64);
    assert_eq!(sink.buf, b"PRErecord-bytes");
}

#[test]
fn append_frame_success_requires_sync() {
    let mut sink = MockSink::new(FailMode::Ok);
    append_frame(
        &mut sink,
        b"record",
        Path::new("x.log"),
        JournalSync::BeforeAck,
    )
    .unwrap();
    assert_eq!(
        sink.calls,
        [SinkCall::WriteAll, SinkCall::Sync],
        "an acknowledged append must have synced before it returned"
    );
}

#[test]
fn append_frame_sync_off_skips_sync() {
    let mut sink = MockSink::new(FailMode::Ok);
    let committed =
        append_frame(&mut sink, b"record", Path::new("x.log"), JournalSync::Off).unwrap();
    assert_eq!(committed.next_offset, 3 + b"record".len() as u64);
    assert_eq!(sink.calls, [SinkCall::WriteAll]);
}

#[test]
fn append_frame_rolls_back_when_write_fails_before_any_bytes() {
    let mut sink = MockSink::new(FailMode::FailBeforeWrite);
    let err = append_frame(
        &mut sink,
        b"rec",
        Path::new("x.log"),
        JournalSync::BeforeAck,
    )
    .unwrap_err();
    assert!(matches!(err, AppendFailure::RolledBack(_)));
    assert_eq!(sink.buf, b"PRE", "no bytes written, file unchanged");
}

#[test]
fn append_frame_rolls_back_a_partial_write() {
    let mut sink = MockSink::new(FailMode::PartialThenFail(2));
    let err = append_frame(
        &mut sink,
        b"record",
        Path::new("x.log"),
        JournalSync::BeforeAck,
    )
    .unwrap_err();
    assert!(matches!(err, AppendFailure::RolledBack(_)));
    assert_eq!(
        sink.buf, b"PRE",
        "partial bytes truncated back to pre-append EOF"
    );
    assert!(!sink.calls.contains(&SinkCall::Sync));
}

#[test]
fn atomic_group_rolls_back_at_every_partial_write_boundary() {
    let journal_id = JournalId::new();
    let identity = crate::journal::disk::identity::JournalIdentity {
        run_id: FlowId::new(),
        journal_id,
    };
    let mut previous = None;
    let writer_id = WriterId::from(StageId::new());
    let records: Vec<_> = (0..3)
        .map(|index| {
            let event = ChainEventFactory::data_event(
                writer_id,
                "atomic.member",
                std::num::NonZeroU32::MIN,
                serde_json::json!({ "index": index }),
            );
            let mut record =
                crate::journal::disk::identity::fixture_record(identity, event, &mut previous);
            record.envelope.provenance.journal.journal_group_id =
                Some("effect-outcome:test".into());
            record.envelope.provenance.journal.journal_group_member =
                Some(JournalGroupMember { index, size: 3 });
            record
        })
        .collect();
    let frame = serialize_atomic_group("effect-outcome:test", &records).unwrap();

    for fail_after in 0..=frame.len() {
        let mut sink = MockSink::new(FailMode::PartialThenFail(fail_after));
        let failure = append_frame(
            &mut sink,
            &frame,
            Path::new("group.log"),
            JournalSync::BeforeAck,
        )
        .expect_err("every injected write failure must abort the group");
        assert!(matches!(failure, AppendFailure::RolledBack(_)));
        assert_eq!(
            sink.buf, b"PRE",
            "no group member may remain visible after failure at byte {fail_after}"
        );
    }
}

#[test]
fn append_frame_failed_sync_poisons() {
    let mut sink = MockSink::new(FailMode::FailOnSync);
    let err = append_frame(
        &mut sink,
        b"record",
        Path::new("x.log"),
        JournalSync::BeforeAck,
    )
    .unwrap_err();
    assert!(matches!(err, AppendFailure::Poisoned(_)));
    assert_eq!(
        sink.calls,
        [SinkCall::WriteAll, SinkCall::Sync],
        "a frame whose sync failed may be on storage, so it is not rolled back"
    );
}

#[test]
fn append_frame_poisons_when_rollback_also_fails() {
    let mut sink = MockSink::new(FailMode::FailWriteAndRollback);
    let err = append_frame(
        &mut sink,
        b"record",
        Path::new("x.log"),
        JournalSync::BeforeAck,
    )
    .unwrap_err();
    assert!(matches!(err, AppendFailure::Poisoned(_)));
}

#[tokio::test]
async fn poisoned_journal_rejects_appends() {
    let test_id = Uuid::new_v4();
    let test_dir = std::path::PathBuf::from(format!("target/test-logs/poisoned_{test_id}"));
    std::fs::create_dir_all(&test_dir).unwrap();
    let log_path = test_dir.join("poison.log");
    let writer_id = WriterId::from(StageId::new());

    let log = DiskJournal::<ChainEvent>::with_owner(
        log_path,
        obzenflow_core::JournalOwner::stage(StageId::new()),
        JournalSync::BeforeAck,
    )
    .unwrap();
    log.append(
        ChainEventFactory::data_event(
            writer_id,
            "e",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"i": 0}),
        ),
        Default::default(),
    )
    .await
    .unwrap();

    // Simulate an unrecoverable rollback failure.
    log.poisoned.store(true, Ordering::SeqCst);

    assert!(
        log.append(
            ChainEventFactory::data_event(
                writer_id,
                "e",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"i": 1})
            ),
            Default::default(),
        )
        .await
        .is_err(),
        "a poisoned journal must reject further appends"
    );

    std::fs::remove_dir_all(&test_dir).unwrap();
}
