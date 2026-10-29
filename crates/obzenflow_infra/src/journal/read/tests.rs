// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::DiskJournal;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::execution_payload::{
    SourceInputPinnedFact, SourceRecordRejectedFact, SourceResumeFact,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainEventFactory, SourceDiagnostic, SourceDiagnosticReason, SystemEventFactory,
};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, RunManifestStage, JOURNAL_SCHEMA_VERSION, OBSERVABILITY_CAPTURE_CAPABILITY,
    RUN_MANIFEST_FILENAME,
};
use obzenflow_core::source::SourceInputIdentity;
use obzenflow_core::{Journal, JournalOwner, SystemId};
use std::io::Write;

struct Run {
    dir: tempfile::TempDir,
    manifest: RunManifest,
    system: DiskJournal<SystemEvent>,
    data: DiskJournal<ChainEvent>,
    error: DiskJournal<ChainEvent>,
    factory: SystemEventFactory,
    stage: StageId,
}

impl Run {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pipeline = SystemId::new();
        let run_id = FlowId::new();
        let stage = StageId::new();
        let system = DiskJournal::with_owner_in_run(
            dir.path().join("system.log"),
            JournalOwner::system(pipeline),
            run_id,
        )
        .unwrap();
        let data = DiskJournal::with_owner_in_run(
            dir.path().join("data.log"),
            JournalOwner::stage(stage),
            run_id,
        )
        .unwrap();
        let error = DiskJournal::<ChainEvent>::with_owner_in_run(
            dir.path().join("error.log"),
            JournalOwner::stage(stage),
            run_id,
        )
        .unwrap();
        let manifest = RunManifest {
            metrics_journals: None,
            journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
            obzenflow_version: env!("CARGO_PKG_VERSION").into(),
            flow_id: run_id.to_string(),
            recorded_flow_id: None,
            pipeline_writer_id: pipeline.into(),
            flow_name: "reader_test".into(),
            created_at: chrono::Utc::now(),
            replay: None,
            resume: None,
            effective_config: None,
            system_journal_file: "system.log".into(),
            stages: [(
                "source".into(),
                RunManifestStage {
                    dsl_var: "source".into(),
                    stage_type: StageType::FiniteSource,
                    is_effectful: false,
                    stage_id: stage.to_string(),
                    stage_logic_version: "1".into(),
                    data_journal_file: "data.log".into(),
                    error_journal_file: "error.log".into(),
                    inbound: vec![],
                    delivery_order: obzenflow_core::journal::archive::DeliveryOrder::SingleInput,
                    ai_models: Vec::new(),
                    max_open_keys: None,
                },
            )]
            .into(),
            capabilities: [(OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1)].into(),
            bounded_direct_fact_admission: vec![],
        };
        let run = Self {
            dir,
            manifest,
            system,
            data,
            error,
            factory: SystemEventFactory::new(pipeline),
            stage,
        };
        run.save_manifest();
        run
    }

    fn save_manifest(&self) {
        std::fs::write(
            self.dir.path().join(RUN_MANIFEST_FILENAME),
            serde_json::to_vec(&self.manifest).unwrap(),
        )
        .unwrap();
    }

    fn event(&self, index: u64) -> ChainEvent {
        ChainEventFactory::data_event(
            self.stage.into(),
            "reader.fact",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"index": index}),
        )
    }

    async fn data(&self, index: u64) {
        self.data
            .append(self.event(index), Default::default())
            .await
            .unwrap();
    }

    async fn system(&self, event: SystemEvent) {
        self.system.append(event, Default::default()).await.unwrap();
    }

    fn rejection(&self, row: Option<u64>) -> ChainEvent {
        let diagnostic = SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord);
        let diagnostic = match row {
            Some(row) => diagnostic.record(row),
            None => diagnostic,
        };
        ChainEventFactory::execution_event(
            self.stage.into(),
            ExecutionPayload::SourceRecordRejected(SourceRecordRejectedFact {
                kind: ErrorKind::Validation,
                detail: "invalid record".into(),
                diagnostic,
            }),
        )
        .mark_as_error("invalid record", ErrorKind::Validation)
    }

    async fn reject(&self, row: Option<u64>) {
        self.error
            .append(self.rejection(row), Default::default())
            .await
            .unwrap();
    }

    async fn pin(&self, file: &str) {
        let pin = SourceInputPinnedFact {
            identity: SourceInputIdentity::new("csv", &serde_json::json!({ "file": file }))
                .unwrap(),
            beginning: serde_json::from_value(serde_json::json!({"schema_version": 1, "value": 0}))
                .unwrap(),
        };
        self.data
            .append(
                ChainEventFactory::execution_event(
                    self.stage.into(),
                    ExecutionPayload::SourceInputPinned(pin),
                ),
                Default::default(),
            )
            .await
            .unwrap();
    }

    async fn positioned(&self, file: &str) {
        let identity_digest = SourceInputIdentity::new("csv", &serde_json::json!({ "file": file }))
            .unwrap()
            .digest();
        self.data
            .append(
                ChainEventFactory::execution_event(
                    self.stage.into(),
                    ExecutionPayload::SourcePositioned(SourceResumeFact {
                        identity_digest,
                        generation: 1,
                    }),
                ),
                Default::default(),
            )
            .await
            .unwrap();
    }

    async fn open(&self) -> RunSnapshot {
        open_disk_run(self.dir.path()).await.unwrap()
    }
}

async fn available(tail: &mut RunTail) -> Vec<RunRecord> {
    let mut records = vec![];
    while let TailRead::Record(record) = tail.read_next().await.unwrap() {
        records.push(record);
        assert!(records.len() < 100, "fixture must be finite");
    }
    records
}

#[tokio::test]
async fn stage_effectful_capability_survives_manifest_projection_without_records() {
    for stage_type in [StageType::Transform, StageType::Stateful] {
        for capability in [false, true] {
            let mut run = Run::new();
            let stage = run.manifest.stages.get_mut("source").unwrap();
            stage.stage_type = stage_type;
            stage.is_effectful = capability;
            run.save_manifest();
            let snapshot = run.open().await;
            let stages: Vec<_> = snapshot
                .journals()
                .filter_map(|journal| journal.stage.as_ref())
                .collect();
            assert_eq!(
                stages.len(),
                2,
                "data and error journals share stage metadata"
            );
            for stage in stages {
                assert_eq!(stage.stage_type, stage_type);
                assert_eq!(stage.is_effectful, capability);
            }
        }
    }
}

#[tokio::test]
async fn snapshot_cuts_are_fixed_and_transfer_unread_atomic_members_without_duplicates() {
    let run = Run::new();
    run.data
        .append_group(
            "fixture",
            (0..3).map(|i| run.event(i)).collect(),
            Default::default(),
        )
        .await
        .unwrap();
    let mut snapshot = run.open().await;
    let mut independent = run.open().await;
    let first = snapshot.next().await.unwrap().unwrap();
    assert_eq!(first.position, JournalPosition(0));
    assert_eq!(first.kind, RunRecordKind::SourceFact);
    run.data(3).await;
    let mut frozen = vec![];
    while let Some(record) = independent.next().await.unwrap() {
        frozen.push(record);
    }
    assert_eq!(frozen.len(), 3, "appends cannot extend a snapshot");
    assert_eq!(frozen[0].journal.id, first.journal.id);
    let mut tail = snapshot.into_tail();
    let remaining = available(&mut tail).await;
    assert_eq!(
        remaining.iter().map(|r| r.position.0).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(tail.progress().outcome.is_none());
    let json = serde_json::to_value(&first).unwrap();
    assert_eq!(
        json["version"],
        obzenflow_core::journal::read::RUN_RECORD_VERSION
    );
    assert!(json["record"]["envelope"].is_object());
    assert!(json["record"]["payload"].is_object());
    let decoded: RunRecord = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.run, first.run);
}

#[tokio::test]
async fn incomplete_atomic_tail_stays_pending_and_never_extends_the_snapshot() {
    let run = Run::new();
    run.data(0).await;
    let path = run.dir.path().join("data.log");
    let initial_len = std::fs::metadata(&path).unwrap().len() as usize;
    run.data
        .append_group(
            "fixture",
            vec![run.event(1), run.event(2)],
            Default::default(),
        )
        .await
        .unwrap();
    let complete = std::fs::read(&path).unwrap();
    let cut = complete.len() - 1;
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(cut as u64)
        .unwrap();
    assert!(cut > initial_len);
    let mut snapshot = run.open().await;
    let mut tail = run.open().await.into_tail();
    assert_eq!(available(&mut tail).await.len(), 1);
    for _ in 0..12 {
        assert!(matches!(tail.read_next().await.unwrap(), TailRead::Pending));
    }
    assert_eq!(
        std::fs::read(&path).unwrap(),
        complete[..cut],
        "observation must not repair a partial frame"
    );
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&complete[cut..])
        .unwrap();
    assert!(snapshot.next().await.unwrap().is_some());
    assert!(snapshot.next().await.unwrap().is_none());
    assert_eq!(
        available(&mut tail)
            .await
            .iter()
            .map(|r| r.position.0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(std::fs::read(&path).unwrap(), complete);
}

#[tokio::test]
async fn settlement_requires_authority_terminal_drain_and_fresh_stage_coverage() {
    let run = Run::new();
    let mut tail = run.open().await.into_tail();
    assert!(available(&mut tail).await.is_empty()); // EOF before drain does not count.
    let other = SystemEventFactory::new(SystemId::new());
    run.system(other.pipeline_not_started()).await;
    run.system(other.pipeline_drained()).await;
    assert_eq!(available(&mut tail).await.len(), 2);
    assert!(tail.progress().outcome.is_none());
    run.system(run.factory.pipeline_not_started()).await;
    assert_eq!(available(&mut tail).await.len(), 1);
    assert!(tail.progress().settled_prefix.is_none());
    run.data(0).await;
    run.system(run.factory.pipeline_drained()).await;
    let records = available(&mut tail).await;
    assert_eq!(records.len(), 2);
    let settled = tail.progress().settled_prefix.clone().unwrap();
    assert_eq!(settled.end_positions.len(), 3);
    let data = records
        .iter()
        .find(|r| r.journal.kind == RunJournalKind::Data)
        .unwrap();
    assert_eq!(settled.end_positions[&data.journal.id], JournalPosition(1));
    run.system(other.pipeline_running()).await;
    assert_eq!(
        available(&mut tail).await.len(),
        1,
        "settlement never closes the reusable tail"
    );
    assert_eq!(
        tail.progress()
            .settled_prefix
            .as_ref()
            .unwrap()
            .end_positions,
        settled.end_positions
    );
}

#[tokio::test]
async fn every_terminal_outcome_settles_even_when_host_keeps_appending() {
    use obzenflow_core::event::types::DurationMs;
    for expected in [
        RunOutcome::Completed,
        RunOutcome::Failed {
            reason: "failed".into(),
        },
        RunOutcome::Cancelled {
            reason: "cancelled".into(),
        },
        RunOutcome::NotStarted,
    ] {
        let run = Run::new();
        let event = match &expected {
            RunOutcome::Completed => run.factory.pipeline_completed(
                DurationMs(1),
                obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
                    events_in_total: 0,
                    events_out_total: 0,
                    errors_total: 0,
                },
            ),
            RunOutcome::Failed { reason } => {
                run.factory
                    .pipeline_failed(reason.clone(), DurationMs(1), None, None)
            }
            RunOutcome::Cancelled { reason } => {
                run.factory
                    .pipeline_cancelled(reason.clone(), DurationMs(1), None, None)
            }
            RunOutcome::NotStarted => run.factory.pipeline_not_started(),
        };
        run.system(event).await;
        run.system(run.factory.pipeline_drained()).await;
        let mut tail = run.open().await.into_tail();
        let host = SystemEventFactory::new(SystemId::new());
        for _ in 0..10 {
            run.system(host.pipeline_running()).await;
            assert!(matches!(
                tail.read_next().await.unwrap(),
                TailRead::Record(_)
            ));
            if tail.progress().settled_prefix.is_some() {
                break;
            }
        }
        assert_eq!(tail.progress().outcome.as_ref().unwrap().outcome, expected);
        assert!(
            tail.progress().settled_prefix.is_some(),
            "continuous host facts cannot prevent execution coverage"
        );
    }
}

#[tokio::test]
async fn drain_without_terminal_and_duplicate_outcome_poison_the_handle() {
    for terminal_first in [false, true] {
        let run = Run::new();
        if terminal_first {
            run.system(run.factory.pipeline_not_started()).await;
            run.system(run.factory.pipeline_not_started()).await;
        } else {
            run.system(run.factory.pipeline_drained()).await;
        }
        let mut tail = run.open().await.into_tail();
        if terminal_first {
            assert!(matches!(
                tail.read_next().await.unwrap(),
                TailRead::Record(_)
            ));
        }
        assert!(matches!(
            tail.read_next().await,
            Err(JournalReadError::Integrity(_))
        ));
        assert!(tail.read_next().await.is_err());
        assert!(tail.progress().settled_prefix.is_none());
    }
}

#[tokio::test]
async fn committed_corruption_is_an_error_without_repair_or_resuming_past_it() {
    let run = Run::new();
    let mut tail = run.open().await.into_tail();
    run.data(0).await;
    let path = run.dir.path().join("data.log");
    let mut bytes = std::fs::read(&path).unwrap();
    let index = bytes.len() / 2;
    bytes[index] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    assert!(tail.read_next().await.is_err());
    assert!(tail.read_next().await.is_err());
    assert!(open_disk_run(run.dir.path()).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[tokio::test]
async fn terminal_and_drain_cannot_settle_over_an_incomplete_stage_group() {
    let run = Run::new();
    run.data
        .append_group(
            "fixture",
            vec![run.event(0), run.event(1)],
            Default::default(),
        )
        .await
        .unwrap();
    let path = run.dir.path().join("data.log");
    let bytes = std::fs::read(&path).unwrap();
    let cut = bytes.len() - 1;
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(cut as u64)
        .unwrap();
    run.system(run.factory.pipeline_not_started()).await;
    run.system(run.factory.pipeline_drained()).await;
    let mut tail = run.open().await.into_tail();
    assert_eq!(available(&mut tail).await.len(), 2);
    assert!(tail.progress().drained_event_id.is_some());
    for _ in 0..10 {
        assert!(matches!(tail.read_next().await.unwrap(), TailRead::Pending));
        assert!(tail.progress().settled_prefix.is_none());
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&bytes[cut..])
        .unwrap();
    assert_eq!(available(&mut tail).await.len(), 2);
    assert!(tail.progress().settled_prefix.is_some());
}

#[tokio::test]
async fn admission_requires_all_files_current_schema_and_pipeline_writer() {
    let mut run = Run::new();
    assert!(open_disk_run(run.dir.path()).await.is_ok());
    let (major, _) = JOURNAL_SCHEMA_VERSION.split_once('.').unwrap();
    let future_version = format!("{}.0", major.parse::<u64>().unwrap() + 1);
    for version in ["6.0", "7.0", "8.0", "9.0", future_version.as_str()] {
        run.manifest.journal_schema_version = version.into();
        run.save_manifest();
        assert!(
            open_disk_run(run.dir.path()).await.is_err(),
            "schema {version} must be rejected"
        );
    }
    run.manifest.journal_schema_version = JOURNAL_SCHEMA_VERSION.into();
    let pipeline_writer_id = run.manifest.pipeline_writer_id;
    run.manifest.pipeline_writer_id = run.stage.into();
    run.save_manifest();
    assert!(open_disk_run(run.dir.path()).await.is_err());
    run.manifest.pipeline_writer_id = pipeline_writer_id;
    run.save_manifest();
    assert!(open_disk_run(run.dir.path()).await.is_ok());
    std::fs::remove_file(run.dir.path().join("error.log")).unwrap();
    assert!(open_disk_run(run.dir.path()).await.is_err());
    assert!(!run.dir.path().join("error.log").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn admitted_run_cannot_silently_retarget_a_replaced_journal() {
    let run = Run::new();
    let mut tail = run.open().await.into_tail();
    assert!(available(&mut tail).await.is_empty());
    let path = run.dir.path().join("data.log");
    std::fs::rename(&path, run.dir.path().join("original.log")).unwrap();
    std::fs::write(&path, []).unwrap();
    assert!(tail.read_next().await.is_err());
}

#[tokio::test]
async fn an_earlier_runs_rejection_follows_the_row_this_run_re_admitted() {
    use obzenflow_core::journal::AppendOptions;

    let earlier = Run::new();
    let first = earlier
        .data
        .append(earlier.event(0), Default::default())
        .await
        .unwrap();
    let rejection = earlier
        .error
        .append(
            earlier.event(1),
            AppendOptions::from_record(Some(&first)).unwrap(),
        )
        .await
        .unwrap();
    let next = earlier
        .data
        .append(
            earlier.event(2),
            AppendOptions::from_record(Some(&rejection)).unwrap(),
        )
        .await
        .unwrap();
    let shown = Run::new();
    for (index, re_admitted) in [(0, &first), (2, &next)] {
        shown
            .data
            .append(
                shown.event(index),
                AppendOptions::from_record(Some(re_admitted)).unwrap(),
            )
            .await
            .unwrap();
    }

    let mut snapshot = shown.open().await;
    let listed = snapshot
        .list_earlier_runs([earlier.dir.path()])
        .await
        .unwrap();
    let earlier_run = listed[0].identity.clone();
    let mut order = Vec::new();
    while let Some(record) = snapshot.next().await.unwrap() {
        order.push((
            record.run == earlier_run,
            record.journal.kind,
            record.position.0,
        ));
    }
    assert_eq!(
        order,
        [
            (false, RunJournalKind::Data, 0),
            (true, RunJournalKind::Error, 0),
            (false, RunJournalKind::Data, 1),
        ]
    );
}

#[tokio::test]
async fn a_tail_lists_causally_and_hands_over_an_unreached_record_on_detach() {
    use obzenflow_core::journal::AppendOptions;

    let run = Run::new();
    let first = run
        .data
        .append(run.event(0), Default::default())
        .await
        .unwrap();
    run.error
        .append(
            run.event(1),
            AppendOptions::from_record(Some(&first)).unwrap(),
        )
        .await
        .unwrap();
    let earlier = Run::new();
    let row = earlier
        .data
        .append(earlier.event(0), Default::default())
        .await
        .unwrap();
    earlier
        .error
        .append(
            earlier.event(1),
            AppendOptions::from_record(Some(&row)).unwrap(),
        )
        .await
        .unwrap();
    let mut snapshot = run.open().await;
    let earlier_run = snapshot
        .list_earlier_runs([earlier.dir.path()])
        .await
        .unwrap()[0]
        .identity
        .clone();
    let mut tail = snapshot.into_tail();
    let listed: Vec<_> = available(&mut tail)
        .await
        .into_iter()
        .map(|record| record.journal.kind)
        .collect();
    assert_eq!(listed, [RunJournalKind::Data, RunJournalKind::Error]);
    let held = tail
        .next_held()
        .await
        .unwrap()
        .expect("the earlier rejection");
    assert_eq!(held.run, earlier_run);
    assert!(tail.next_held().await.unwrap().is_none());
}

#[tokio::test]
async fn earlier_rejections_keep_their_places_while_each_journal_holds_one() {
    use obzenflow_core::journal::AppendOptions;

    const BURST: u64 = 1_000;
    let earlier = Run::new();
    let first = earlier
        .data
        .append(earlier.event(0), Default::default())
        .await
        .unwrap();
    let mut last = None;
    for row in 0..BURST {
        last = Some(
            earlier
                .error
                .append(
                    earlier.rejection(Some(row)),
                    AppendOptions::from_record(Some(&first)).unwrap(),
                )
                .await
                .unwrap(),
        );
    }
    let second = earlier
        .data
        .append(
            earlier.event(1),
            AppendOptions::from_record(last.as_ref()).unwrap(),
        )
        .await
        .unwrap();
    earlier
        .error
        .append(
            earlier.rejection(Some(BURST)),
            AppendOptions::from_record(Some(&second)).unwrap(),
        )
        .await
        .unwrap();
    let shown = Run::new();
    for (index, re_admitted) in [(0, &first), (1, &second)] {
        shown
            .data
            .append(
                shown.event(index),
                AppendOptions::from_record(Some(re_admitted)).unwrap(),
            )
            .await
            .unwrap();
    }

    let mut snapshot = shown.open().await;
    snapshot
        .list_earlier_runs([earlier.dir.path()])
        .await
        .unwrap();
    let mut order = Vec::new();
    let mut most_held = 0;
    while let Some(record) = snapshot.next().await.unwrap() {
        most_held = most_held.max(snapshot.state.held());
        order.push((record.journal.kind, record.position.0));
    }
    let mut expected = vec![(RunJournalKind::Data, 0)];
    expected.extend((0..BURST).map(|position| (RunJournalKind::Error, position)));
    expected.extend([(RunJournalKind::Data, 1), (RunJournalKind::Error, BURST)]);
    assert_eq!(order, expected);
    assert!(
        most_held <= 4,
        "{most_held} records held for three journals and one earlier journal"
    );
}

#[tokio::test]
async fn a_row_rejected_again_names_the_oldest_run_that_rejected_it() {
    let first = Run::new();
    first.pin("night.csv").await;
    first.reject(Some(3)).await;
    first.reject(Some(5)).await;
    let second = Run::new();
    second.positioned("night.csv").await;
    second.reject(Some(5)).await;
    second.reject(Some(7)).await;
    let shown = Run::new();
    shown.reject(Some(7)).await;
    shown.reject(Some(9)).await;

    let mut snapshot = shown.open().await;
    let runs: Vec<RunIdentity> = snapshot
        .list_earlier_runs([first.dir.path(), second.dir.path()])
        .await
        .unwrap()
        .into_iter()
        .map(|run| run.identity)
        .chain([snapshot.identity().clone()])
        .collect();
    let ordinal = |run: &RunIdentity| runs.iter().position(|listed| listed == run).unwrap();
    let mut repeats = Vec::new();
    let mut second_row_five = None;
    while let Some(record) = snapshot.next().await.unwrap() {
        if record.journal.kind != RunJournalKind::Error {
            continue;
        }
        let repeat = snapshot.repeat_of(&record).await.unwrap();
        let at = (ordinal(&record.run), record.position.0);
        if at == (1, 0) {
            second_row_five = Some(record);
        }
        repeats.push((
            at,
            repeat.map(|first| (ordinal(&first.run), first.position.0)),
        ));
    }
    repeats.sort();
    assert_eq!(
        repeats,
        [
            ((0, 0), None),
            ((0, 1), None),
            ((1, 0), Some((0, 1))),
            ((1, 1), None),
            ((2, 0), Some((1, 1))),
            ((2, 1), None),
        ]
    );
    let again = snapshot
        .repeat_of(&second_row_five.unwrap())
        .await
        .unwrap()
        .expect("a query for an earlier row reads its run again");
    assert_eq!((ordinal(&again.run), again.position.0), (0, 1));
}

#[tokio::test]
async fn a_run_that_rejected_rows_out_of_order_still_names_the_first() {
    let killed = Run::new();
    killed.pin("night.csv").await;
    killed.reject(Some(5)).await;
    killed.reject(Some(3)).await;
    let shown = Run::new();
    shown.reject(Some(3)).await;
    assert_eq!(shown_repeats(&shown, &[&killed]).await, [Some(1)]);
}

#[tokio::test]
async fn rows_repeat_only_while_their_source_pinned_one_input() {
    let unpinned = Run::new();
    unpinned.reject(Some(164)).await;
    let shown = Run::new();
    shown.reject(Some(164)).await;
    assert_eq!(shown_repeats(&shown, &[&unpinned]).await, [None]);

    let monday = Run::new();
    monday.pin("monday.csv").await;
    monday.reject(Some(164)).await;
    let tuesday = Run::new();
    tuesday.positioned("tuesday.csv").await;
    tuesday.reject(Some(164)).await;
    let shown = Run::new();
    shown.reject(Some(164)).await;
    assert_eq!(shown_repeats(&shown, &[&monday, &tuesday]).await, [None]);
}

#[tokio::test]
async fn a_rejection_that_names_no_record_never_repeats() {
    let killed = Run::new();
    killed.pin("night.csv").await;
    killed.reject(None).await;
    let shown = Run::new();
    shown.reject(None).await;
    assert_eq!(shown_repeats(&shown, &[&killed]).await, [None]);
}

async fn shown_repeats(shown: &Run, earlier: &[&Run]) -> Vec<Option<u64>> {
    let mut snapshot = shown.open().await;
    snapshot
        .list_earlier_runs(earlier.iter().map(|run| run.dir.path()))
        .await
        .unwrap();
    let mut repeats = Vec::new();
    while let Some(record) = snapshot.next().await.unwrap() {
        if record.run == *snapshot.identity() && record.journal.kind == RunJournalKind::Error {
            let repeat = snapshot.repeat_of(&record).await.unwrap();
            repeats.push(repeat.map(|first| first.position.0));
        }
    }
    repeats
}
