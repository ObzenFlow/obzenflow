// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::{DiskJournal, MemoryJournal};
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{
    CausalFrontier, ChainEvent, ChainEventFactory, ChainPayload, SupervisorRecord,
};
use obzenflow_core::journal::reader::JournalReportReader;
use obzenflow_core::{EventId, FlowId, Journal, JournalOwner, StageId};
use std::future::Future;
use std::task::Poll;

fn report(stage: StageId) -> ChainEvent {
    ChainEventFactory::create_with_context(
        stage.into(),
        ChainPayload::Execution(ExecutionPayload::StageLifecycle(
            StageLifecycleFact::Running { stage_id: stage },
        )),
        FlowContext::new("child", stage),
    )
}
fn business(stage: StageId) -> ChainEvent {
    ChainEventFactory::data_event(
        stage.into(),
        "business",
        serde_json::json!({"body":"payload"}),
    )
}

async fn populate(
    journal: &dyn Journal<ChainEvent>,
    stage: StageId,
    grouped: bool,
) -> Vec<JournalRecord<ChainPayload>> {
    let events = vec![
        business(stage),
        report(stage),
        business(stage),
        report(stage),
        business(stage),
    ];
    if grouped {
        journal
            .append_group("mixed", events, Default::default())
            .await
            .unwrap()
    } else {
        let mut rows = Vec::new();
        for event in events {
            rows.push(journal.append(event, Default::default()).await.unwrap());
        }
        rows
    }
}

async fn drain(reader: &mut dyn JournalReportReader<ChainEvent>) -> Vec<(u64, EventId)> {
    let mut rows = Vec::new();
    let mut position = reader.position();
    for _ in 0..100 {
        match reader
            .next_report(ReportScanBudget {
                records: 2,
                bytes: 1024,
            })
            .await
            .unwrap()
            .item
        {
            ReportScanItem::Record(row) => {
                assert!(
                    CausalFrontier::from_record(&row).is_ok(),
                    "core must admit returned evidence"
                );
                assert_eq!(row.local_sequence(), reader.position());
                rows.push((row.local_sequence(), *row.id()));
            }
            ReportScanItem::Progress => assert!(reader.position() > position),
            ReportScanItem::Tail { committed_end } => {
                assert!(committed_end);
                return rows;
            }
        }
        assert!(reader.position() >= position);
        position = reader.position();
    }
    panic!("bounded fixture failed to settle");
}

#[tokio::test]
async fn disk_and_memory_match_full_projection_and_resume_inside_groups() {
    for grouped in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let stage = StageId::new();
        let disk = DiskJournal::<ChainEvent>::with_owner_in_run(
            dir.path().join("data.log"),
            JournalOwner::stage(stage),
            FlowId::new(),
        )
        .unwrap();
        let memory = MemoryJournal::<ChainEvent>::with_owner(JournalOwner::stage(stage));
        for journal in [&disk as &dyn Journal<ChainEvent>, &memory] {
            let rows = populate(journal, stage, grouped).await;
            let mut full = journal.reader().await.unwrap();
            let mut expected = Vec::new();
            while let Some(row) = full.next().await.unwrap() {
                if let Some(report) = SupervisorRecord::from_chain(row) {
                    expected.push((report.position(), *report.id()));
                }
            }
            for resume in 0..=rows.len() as u64 {
                let mut reader = journal.report_reader_from(resume).await.unwrap();
                assert_eq!(reader.position(), resume);
                assert_eq!(
                    drain(reader.as_mut()).await,
                    expected
                        .iter()
                        .copied()
                        .filter(|(position, _)| *position > resume)
                        .collect::<Vec<_>>()
                );
                assert_eq!(reader.position(), 5);
                assert!(reader.initial_prefix_complete().unwrap());
            }
        }
    }
}

#[tokio::test]
async fn business_prefix_yields_progress_and_late_reports_remain_visible() {
    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let journal = DiskJournal::<ChainEvent>::with_owner(
        dir.path().join("data.log"),
        JournalOwner::stage(stage),
    )
    .unwrap();
    for _ in 0..10 {
        journal
            .append(business(stage), Default::default())
            .await
            .unwrap();
    }
    let mut reader = journal.report_reader_from(0).await.unwrap();
    for through in [3, 6, 9, 10] {
        let scan = reader
            .next_report(ReportScanBudget {
                records: 3,
                bytes: usize::MAX,
            })
            .await
            .unwrap();
        assert!(matches!(scan.item, ReportScanItem::Progress));
        assert_eq!(reader.position(), through);
        assert!(scan.scanned_bytes > 0);
    }
    assert!(reader.initial_prefix_complete().unwrap());
    assert!(matches!(
        reader.next_report(Default::default()).await.unwrap().item,
        ReportScanItem::Tail {
            committed_end: true
        }
    ));
    journal
        .append(
            ChainEventFactory::eof_event(stage.into(), true),
            Default::default(),
        )
        .await
        .unwrap();
    let row = journal
        .append(report(stage), Default::default())
        .await
        .unwrap();
    assert_eq!(drain(reader.as_mut()).await, vec![(12, *row.id())]);
    assert!(reader.initial_prefix_complete().unwrap());
}

#[tokio::test]
async fn cancelling_a_pending_scan_retains_its_job_and_delivers_every_report() {
    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let path = dir.path().join("data.log");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let rows = populate(&journal, stage, true).await;
    let lock = Arc::new(RwLock::new(()));
    let guard = lock.write().await;
    let mut reader = DiskReportReader::<ChainEvent>::new(path, *journal.id(), lock.clone(), 5, 0)
        .await
        .unwrap();
    {
        let mut pending = Box::pin(reader.next_report(Default::default()));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert!(reader.job.is_some());
    assert!(
        reader.state.is_none(),
        "in-flight state must not be replaced"
    );
    assert_eq!(JournalReportReader::position(&reader), 0);
    // A cancelled caller must not discard the reader's place in the lock
    // queue. This later writer stays behind the retained acquisition.
    let mut later_writer = Box::pin(lock.clone().write_owned());
    assert!(futures::poll!(later_writer.as_mut()).is_pending());
    drop(guard);
    assert!(futures::poll!(later_writer.as_mut()).is_pending());
    drop(later_writer);
    assert_eq!(
        drain(&mut reader).await,
        vec![(2, *rows[1].id()), (4, *rows[3].id())]
    );
}

#[tokio::test]
async fn writer_between_frames_yields_verified_progress_without_a_false_tail() {
    use std::sync::Mutex;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let path = dir.path().join("data.log");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let rows = populate(&journal, stage, false).await;
    let first_bytes = frame::frame_length(&std::fs::read(&path).unwrap()).unwrap();
    let lock = super::super::journal::shared_state_for_path(&path).0;
    let mut reader = DiskReportReader::<ChainEvent>::new(path, *journal.id(), lock.clone(), 5, 0)
        .await
        .unwrap();
    let held = Arc::new(Mutex::new(None));
    let writer = held.clone();
    reader.state.as_mut().unwrap().after_frame_read = Some(Box::new(move || {
        *writer.lock().unwrap() = Some(
            lock.try_write_owned()
                .expect("copied-frame processing must release the read lock"),
        );
    }));
    let scan = tokio::time::timeout(
        Duration::from_secs(5),
        reader.next_report(Default::default()),
    )
    .await;
    // Unblock even a regressed scanner before an assertion can fail.
    if scan.is_err() {
        held.lock().unwrap().take();
    }
    let scan = scan
        .expect("a contended scan must yield its verified prefix")
        .unwrap();
    assert!(matches!(scan.item, ReportScanItem::Progress));
    assert_eq!(scan.scanned_bytes, first_bytes);
    assert_eq!(JournalReportReader::position(&reader), 1);
    assert!(!JournalReportReader::is_at_end(&reader));
    assert!(!JournalReportReader::initial_prefix_complete(&reader).unwrap());
    assert_eq!(reader.stall_polls, 0);
    {
        let mut next = Box::pin(reader.next_report(Default::default()));
        assert!(futures::poll!(next.as_mut()).is_pending());
    }
    assert_eq!(JournalReportReader::position(&reader), 1);
    held.lock().unwrap().take();
    assert_eq!(
        drain(&mut reader).await,
        vec![(2, *rows[1].id()), (4, *rows[3].id())]
    );
}

#[tokio::test]
async fn cancelled_copied_group_retains_reports_and_decodes_without_the_lock() {
    use std::sync::Mutex;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let path = dir.path().join("data.log");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let rows = populate(&journal, stage, true).await;
    let lock = super::super::journal::shared_state_for_path(&path).0;
    let mut reader = DiskReportReader::<ChainEvent>::new(path, *journal.id(), lock.clone(), 5, 0)
        .await
        .unwrap();
    let (entered, copied) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let resume = Mutex::new(resume);
    reader.state.as_mut().unwrap().after_frame_read = Some(Box::new(move || {
        entered.send(()).unwrap();
        resume
            .into_inner()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    }));
    {
        let next = reader.next_report(Default::default());
        tokio::pin!(next);
        tokio::select! {
            _ = &mut next => panic!("scan must pause after copying the frame"),
            result = copied => result.unwrap(),
        }
    }
    assert_eq!(JournalReportReader::position(&reader), 0);
    let writer = lock
        .try_write()
        .expect("decoding must not retain the read guard");
    release.send(()).unwrap();
    for index in [1, 3] {
        let scan = tokio::time::timeout(
            Duration::from_secs(5),
            reader.next_report(Default::default()),
        )
        .await
        .unwrap()
        .unwrap();
        let ReportScanItem::Record(row) = scan.item else {
            panic!("retained group report must be delivered");
        };
        assert_eq!(row.id(), rows[index].id());
        assert_eq!(JournalReportReader::position(&reader), (index + 1) as u64);
    }
    drop(writer);
    assert!(drain(&mut reader).await.is_empty());
    assert_eq!(JournalReportReader::position(&reader), 5);
}

#[tokio::test]
async fn dropping_a_reader_cancels_its_queued_lock_acquisition() {
    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let path = dir.path().join("data.log");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let lock = super::super::journal::shared_state_for_path(&path).0;
    let mut reader = DiskReportReader::<ChainEvent>::new(path, *journal.id(), lock.clone(), 0, 0)
        .await
        .unwrap();
    let writer = lock.write().await;
    {
        let mut next = Box::pin(reader.next_report(Default::default()));
        assert!(futures::poll!(next.as_mut()).is_pending());
    }
    drop(reader);
    drop(writer);
    assert!(
        lock.try_write().is_ok(),
        "no detached lock waiter may survive the reader"
    );
}

#[tokio::test]
async fn incomplete_group_has_no_delivery_then_commits_and_corruption_stays_failed() {
    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let path = dir.path().join("data.log");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let rows = populate(&journal, stage, true).await;
    let complete = std::fs::read(&path).unwrap();
    std::fs::write(&path, &complete[..complete.len() - 1]).unwrap();
    let mut reader = journal.report_reader_from(0).await.unwrap();
    for _ in 0..2 {
        assert!(matches!(
            reader.next_report(Default::default()).await.unwrap().item,
            ReportScanItem::Tail {
                committed_end: false
            }
        ));
        assert_eq!(reader.position(), 0);
        assert!(!reader.initial_prefix_complete().unwrap());
    }
    let mut stalled = journal.report_reader_from(0).await.unwrap();
    for _ in 0..super::super::reader::MAX_STALL_POLLS {
        assert!(matches!(
            stalled.next_report(Default::default()).await.unwrap().item,
            ReportScanItem::Tail {
                committed_end: false
            }
        ));
    }
    assert!(stalled.next_report(Default::default()).await.is_err());
    assert_eq!(stalled.position(), 0);
    std::fs::write(&path, &complete).unwrap();
    assert!(stalled.next_report(Default::default()).await.is_err());
    assert_eq!(
        drain(reader.as_mut()).await,
        vec![(2, *rows[1].id()), (4, *rows[3].id())]
    );
    let mut corrupt = complete.clone();
    corrupt[frame::HEADER_LEN + 1] ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    let mut reader = journal.report_reader_from(0).await.unwrap();
    assert!(reader.next_report(Default::default()).await.is_err());
    std::fs::write(&path, complete).unwrap();
    assert!(
        reader.next_report(Default::default()).await.is_err(),
        "retry cannot pass a failed prefix"
    );
    assert_eq!(reader.position(), 0);
}

#[tokio::test]
async fn routing_gap_cannot_advance_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let stage = StageId::new();
    let path = dir.path().join("data.log");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    populate(&journal, stage, false).await;
    let bytes = std::fs::read(&path).unwrap();
    let first = frame::frame_length(&bytes).unwrap();
    std::fs::write(&path, &bytes[first..]).unwrap();
    let mut reader = journal.report_reader_from(0).await.unwrap();
    assert!(reader.next_report(Default::default()).await.is_err());
    assert_eq!(reader.position(), 0);
}
