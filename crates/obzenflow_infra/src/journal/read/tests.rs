// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::DiskJournal;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{ChainEventFactory, SystemEventFactory};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, RunManifestStage, JOURNAL_SCHEMA_VERSION, OBSERVABILITY_CAPTURE_CAPABILITY,
    RUN_MANIFEST_FILENAME,
};
use obzenflow_core::{Journal, JournalOwner, SystemId};
use std::io::Write;

struct Run {
    dir: tempfile::TempDir,
    manifest: RunManifest,
    system: DiskJournal<SystemEvent>,
    data: DiskJournal<ChainEvent>,
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
        let _error = DiskJournal::<ChainEvent>::with_owner_in_run(
            dir.path().join("error.log"),
            JournalOwner::stage(stage),
            run_id,
        )
        .unwrap();
        let manifest = RunManifest {
            journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
            obzenflow_version: env!("CARGO_PKG_VERSION").into(),
            flow_id: run_id.to_string(),
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
                    ordered_delivery: true,
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
    for version in ["6.0", "7.0", "9.0"] {
        run.manifest.journal_schema_version = version.into();
        run.save_manifest();
        assert!(open_disk_run(run.dir.path()).await.is_err());
    }
    run.manifest.journal_schema_version = JOURNAL_SCHEMA_VERSION.into();
    run.manifest.pipeline_writer_id = run.stage.into();
    run.save_manifest();
    assert!(open_disk_run(run.dir.path()).await.is_err());
    run.manifest.pipeline_writer_id = SystemId::new().into();
    run.save_manifest();
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
