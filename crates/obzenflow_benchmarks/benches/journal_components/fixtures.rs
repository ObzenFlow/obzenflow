// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{CausalFrontier, ChainEventFactory, ChainPayload, SupervisorRecord};
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

pub const DEADLINE: Duration = Duration::from_secs(30);

pub fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[derive(Clone, Copy)]
pub enum Backend {
    Memory,
    Disk,
}

impl Backend {
    pub fn name(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }
}

pub fn report(stage: StageId) -> ChainEvent {
    ChainEventFactory::create_with_context(
        stage.into(),
        ChainPayload::Execution(ExecutionPayload::StageLifecycle(
            StageLifecycleFact::Running { stage_id: stage },
        )),
        FlowContext::new("benchmark_child", stage),
    )
}

pub struct History {
    pub journals: Vec<Arc<dyn Journal<ChainEvent>>>,
    pub paths: Vec<PathBuf>,
    pub records: Vec<Vec<ChainJournalRecord>>,
    pub reports: Vec<SupervisorRecord>,
    pub total_records: usize,
    // Retain files until readers and definition-cache lookups have settled.
    pub _directory: tempfile::TempDir,
}

impl History {
    /// `business` ordinary rows followed by `reports` owned lifecycle records.
    /// `group` controls actual physical atomic frames, never logical handoff size.
    pub async fn build(
        backend: Backend,
        stages: &[StageId],
        business: usize,
        reports: usize,
        payload_bytes: usize,
        group: usize,
    ) -> Self {
        assert!(group > 0);
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let mut fixture = Self {
            journals: Vec::new(),
            paths: Vec::new(),
            records: Vec::new(),
            reports: Vec::new(),
            total_records: stages.len() * (business + reports),
            _directory: directory,
        };
        for stage in stages {
            let path = fixture._directory.path().join(format!("{stage}.log"));
            let journal: Arc<dyn Journal<ChainEvent>> = match backend {
                Backend::Memory => Arc::new(MemoryJournal::with_owner_in_run(
                    JournalOwner::stage(*stage),
                    run,
                )),
                Backend::Disk => Arc::new(
                    DiskJournal::with_owner_in_run(path.clone(), JournalOwner::stage(*stage), run)
                        .unwrap(),
                ),
            };
            let mut events = Vec::with_capacity(business + reports);
            for index in 0..business {
                events.push(ChainEventFactory::data_event(
                    (*stage).into(),
                    "bench.business",
                    serde_json::json!({"index": index, "body": "x".repeat(payload_bytes)}),
                ));
            }
            events.extend((0..reports).map(|_| report(*stage)));
            let mut written = Vec::new();
            for (index, chunk) in events.chunks(group).enumerate() {
                if group == 1 {
                    written.push(
                        journal
                            .append(chunk[0].clone(), Default::default())
                            .await
                            .unwrap(),
                    );
                } else {
                    written.extend(
                        journal
                            .append_group(
                                &format!("bench-{index}"),
                                chunk.to_vec(),
                                Default::default(),
                            )
                            .await
                            .unwrap(),
                    );
                }
            }
            assert_eq!(written.len(), business + reports);
            assert_eq!(
                journal.committed_position().await.unwrap(),
                written.len() as u64
            );
            fixture.reports.extend(
                written
                    .iter()
                    .cloned()
                    .filter_map(SupervisorRecord::from_chain),
            );
            fixture.journals.push(journal);
            fixture.paths.push(path);
            fixture.records.push(written);
        }
        assert_eq!(fixture.reports.len(), stages.len() * reports);
        fixture
    }
}

pub async fn causal_record(
    witnesses: usize,
    payload_bytes: usize,
) -> (ChainJournalRecord, CausalFrontier) {
    let run = FlowId::new();
    let mut frontier = CausalFrontier::default();
    for _ in 0..witnesses {
        let stage = StageId::new();
        let journal = MemoryJournal::with_owner_in_run(JournalOwner::stage(stage), run);
        let record = journal
            .append(
                ChainEventFactory::data_event(stage.into(), "bench.parent", Default::default()),
                Default::default(),
            )
            .await
            .unwrap();
        frontier
            .merge(&CausalFrontier::from_record(&record).unwrap())
            .unwrap();
    }
    let stage = StageId::new();
    let journal = MemoryJournal::with_owner_in_run(JournalOwner::stage(stage), run);
    journal
        .append(report(stage), Default::default())
        .await
        .unwrap();
    let record = journal
        .append(
            ChainEventFactory::data_event(
                stage.into(),
                "bench.record",
                serde_json::json!({"body": "x".repeat(payload_bytes)}),
            ),
            obzenflow_core::journal::AppendOptions::new(frontier.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        record.envelope.provenance.journal.causal.witnesses.len(),
        witnesses
    );
    (record, frontier)
}
