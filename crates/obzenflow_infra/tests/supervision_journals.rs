// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, ChainPayload, SupervisorRecord};
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_runtime::supervised_base::report_reader::{ReportRead, ReportReaders};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

fn report(stage: StageId) -> ChainEvent {
    ChainEventFactory::create_with_context(
        stage.into(),
        ChainPayload::Execution(ExecutionPayload::StageLifecycle(
            StageLifecycleFact::Running { stage_id: stage },
        )),
        FlowContext::new("child", stage),
    )
}

async fn next(readers: &mut ReportReaders) -> ReportRead {
    tokio::time::timeout(
        Duration::from_secs(10),
        std::future::poll_fn(|cx| readers.poll_next(cx)),
    )
    .await
    .expect("reader made no progress")
    .expect("reader failure")
}

async fn next_record(readers: &mut ReportReaders) -> SupervisorRecord {
    loop {
        if let ReportRead::Record(record) = next(readers).await {
            return *record;
        }
    }
}

#[tokio::test]
async fn physical_reports_continue_after_data_eof_and_filtered_prefixes() {
    for disk in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let stage = StageId::new();
        let run = FlowId::new();
        let journal: Arc<dyn Journal<ChainEvent>> = if disk {
            Arc::new(
                DiskJournal::with_owner_in_run(
                    dir.path().join("child.log"),
                    JournalOwner::stage(stage),
                    run,
                )
                .unwrap(),
            )
        } else {
            Arc::new(MemoryJournal::with_owner_in_run(
                JournalOwner::stage(stage),
                run,
            ))
        };
        for _ in 0..130 {
            journal
                .append(
                    ChainEventFactory::data_event(
                        stage.into(),
                        "business",
                        serde_json::json!({"body": "x".repeat(8192)}),
                    ),
                    Default::default(),
                )
                .await
                .unwrap();
        }
        journal
            .append(
                ChainEventFactory::eof_event(stage.into(), true),
                Default::default(),
            )
            .await
            .unwrap();
        let boundary = journal.committed_position().await.unwrap();
        let mut readers = ReportReaders::default();
        readers.stage(journal.clone());
        let mut covered = 0;
        while covered < boundary {
            match next(&mut readers).await {
                ReportRead::Coverage { through, .. } => {
                    assert!(through >= covered);
                    covered = through;
                }
                ReportRead::Record(_) => panic!("business rows are not parent reports"),
            }
        }
        let late = journal
            .append(report(stage), Default::default())
            .await
            .unwrap();
        let observed = next_record(&mut readers).await;
        assert_eq!(observed.id(), late.id());
        assert_eq!(observed.journal_id(), *journal.id());
        assert_eq!(observed.position(), boundary + 1);
        assert_eq!(
            observed.commitment().unwrap().reference,
            obzenflow_core::event::CausalCommit::from_record(&late)
                .unwrap()
                .reference
        );
        assert_eq!(
            observed.journal().vector_clock,
            late.envelope.provenance.journal.vector_clock
        );
    }
}

#[tokio::test]
async fn missing_commitments_fail_before_report_coverage_crosses_the_gap() {
    use obzenflow_core::event::CausalError;
    use obzenflow_core::journal::JournalError;
    use obzenflow_runtime::supervised_base::report_reader::ReportReaderError;

    for missing_business_record in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("child.log");
        let stage = StageId::new();
        let journal = Arc::new(
            DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage))
                .unwrap(),
        );
        journal
            .append(report(stage), Default::default())
            .await
            .unwrap();
        let first_end = std::fs::metadata(&path).unwrap().len() as usize;
        let middle = if missing_business_record {
            ChainEventFactory::data_event(stage.into(), "business", serde_json::json!({}))
        } else {
            report(stage)
        };
        journal.append(middle, Default::default()).await.unwrap();
        let middle_end = std::fs::metadata(&path).unwrap().len() as usize;
        journal
            .append(report(stage), Default::default())
            .await
            .unwrap();
        let boundary = journal.committed_position().await.unwrap();
        assert_eq!(boundary, 3);

        let original = std::fs::read(&path).unwrap();
        std::fs::write(
            &path,
            [
                original[..first_end].to_vec(),
                original[middle_end..].to_vec(),
            ]
            .concat(),
        )
        .unwrap();
        let mut readers = ReportReaders::default();
        readers.stage(journal.clone());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match std::future::poll_fn(|cx| readers.poll_next(cx)).await {
                    Ok(ReportRead::Record(record)) => assert_eq!(record.position(), 1),
                    Ok(ReportRead::Coverage { through, .. }) => assert!(through <= 1),
                    Err(error) => {
                        assert!(matches!(
                            error.downcast_ref::<ReportReaderError>(),
                            Some(ReportReaderError::Read {
                                journal: id,
                                source: JournalError::Causal(CausalError::ConflictingCommitment),
                            }) if id == journal.id()
                        ));
                        break;
                    }
                }
            }
        })
        .await
        .expect("missing report evidence must fail, not stall");
        let diagnostics = readers.diagnostics();
        assert!(diagnostics[0].scanned_through <= 1);
        assert!(diagnostics[0].delivered_through <= 1);
        assert!(!readers.initial_prefix_complete());
        obzenflow_runtime::testing::pipeline::report_gap_cannot_complete_pipeline(
            || {
                Box::new(obzenflow_infra::journal::MemoryJournalFactory::new(
                    FlowId::new(),
                ))
            },
            journal,
            2,
        )
        .await;
    }
}

#[tokio::test]
async fn a_hundred_ready_journals_are_fair_and_keep_their_own_order() {
    for disk in [false, true] {
        let started = std::time::Instant::now();
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let mut readers = ReportReaders::default();
        let mut targets = HashMap::new();
        for index in 0..100 {
            let stage = StageId::new();
            let journal: Arc<dyn Journal<ChainEvent>> = if disk {
                Arc::new(
                    DiskJournal::with_owner_in_run(
                        directory.path().join(format!("{index}.log")),
                        JournalOwner::stage(stage),
                        run,
                    )
                    .unwrap(),
                )
            } else {
                Arc::new(MemoryJournal::with_owner_in_run(
                    JournalOwner::stage(stage),
                    run,
                ))
            };
            let count = if index == 0 { 1_000 } else { 1 };
            for _ in 0..count {
                journal
                    .append(report(stage), Default::default())
                    .await
                    .unwrap();
            }
            targets.insert(*journal.id(), count);
            readers.stage(journal);
        }
        // All readers get to fill one handoff. None can prefetch an unbounded tail.
        tokio::time::timeout(Duration::from_secs(10), async {
            while readers
                .diagnostics()
                .iter()
                .filter(|reader| reader.ready)
                .count()
                != 100
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut seen = HashSet::new();
        let mut positions = HashMap::new();
        let mut first_round_records = 0;
        let mut coverage = HashMap::new();
        while !targets
            .iter()
            .all(|(journal, count)| coverage.get(journal).copied().unwrap_or(0) >= *count)
        {
            match next(&mut readers).await {
                ReportRead::Record(record) => {
                    let prior = positions
                        .insert(record.journal_id(), record.position())
                        .unwrap_or(0);
                    assert_eq!(record.position(), prior + 1);
                    if seen.len() < 100 {
                        first_round_records += 1;
                        seen.insert(record.journal_id());
                    }
                }
                ReportRead::Coverage { journal, through } => {
                    assert_eq!(
                        positions.get(&journal).copied().unwrap_or(0),
                        through,
                        "coverage cannot overtake delivered protected reports"
                    );
                    coverage.insert(journal, through);
                }
            }
        }
        assert_eq!(seen.len(), 100);
        assert_eq!(
            first_round_records, 100,
            "a hot reader cannot take a second turn before a ready sibling"
        );
        let diagnostics = readers.diagnostics();
        assert!(diagnostics
            .iter()
            .all(|reader| reader.high_water_record_bytes
                <= 2 * (512 * 1024 + obzenflow_core::journal::limits::MAX_RECORD_BYTES)));
        assert_eq!(
            diagnostics
                .iter()
                .map(|reader| reader.selected_records)
                .sum::<u64>(),
            1099
        );
        eprintln!("backend={}, 100 journals, 1,099 protected reports, elapsed={:?}, retained high-water sum={} canonical bytes",
        if disk { "disk" } else { "memory" }, started.elapsed(), diagnostics.iter().map(|reader| reader.high_water_record_bytes).sum::<usize>());
    }
}

#[tokio::test]
async fn forwarded_authors_do_not_publish_another_stages_reports() {
    let owner = StageId::new();
    let foreign = StageId::new();
    let journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(owner)));
    journal
        .append(report(foreign), Default::default())
        .await
        .unwrap();
    let local = journal
        .append(report(owner), Default::default())
        .await
        .unwrap();
    let mut readers = ReportReaders::default();
    readers.stage(journal.clone());
    let observed = next_record(&mut readers).await;
    assert_eq!(observed.id(), local.id());
    assert_eq!(observed.position(), 2);
    assert_eq!(
        local.envelope.provenance.journal.vector_clock.clocks.len(),
        1,
        "forwarding advances the destination component, not a new author component"
    );
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
            .append(report(stage), Default::default())
            .await
            .unwrap();
        let events = (0..=MAX_GROUP_RECORDS).map(|_| report(stage)).collect();
        assert!(journal
            .append_group("too-many", events, Default::default())
            .await
            .is_err());
        let oversized = ChainEventFactory::data_event(
            stage.into(),
            "large",
            serde_json::json!({"body": "x".repeat(MAX_RECORD_BYTES)}),
        );
        assert!(journal.append(oversized, Default::default()).await.is_err());
        let over_bytes = (0..10)
            .map(|_| {
                ChainEventFactory::data_event(
                    stage.into(),
                    "large",
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
                (0..256).map(|_| report(stage)).collect(),
                Default::default(),
            )
            .await
            .unwrap();
        let mut readers = ReportReaders::default();
        readers.stage(journal.clone());
        assert_eq!(next_record(&mut readers).await.id(), first.id());
        for (index, row) in group.iter().enumerate() {
            let observed = next_record(&mut readers).await;
            assert_eq!(observed.id(), row.id());
            assert_eq!(observed.position(), index as u64 + 2);
        }
        assert_eq!(journal.committed_position().await.unwrap(), 257);
    }
}
