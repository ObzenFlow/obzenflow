// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub use obzenflow_benchmarks::support::{runtime, DEADLINE};
use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{CausalFrontier, ChainEventFactory};
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use std::path::PathBuf;
use std::sync::Arc;

pub fn running_fact(stage: StageId) -> ChainEvent {
    ChainEventFactory::execution_event(
        stage.into(),
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: stage }),
    )
    .with_flow_context(FlowContext::new("benchmark_child", stage))
}

pub struct History {
    pub journals: Vec<Arc<dyn Journal<ChainEvent>>>,
    pub paths: Vec<PathBuf>,
    pub records: Vec<Vec<ChainJournalRecord>>,
    // Retain files until readers and definition-cache lookups have settled.
    pub _directory: tempfile::TempDir,
}

impl History {
    /// `business` ordinary rows followed by `lifecycle_facts` owned lifecycle records.
    /// `group` controls actual physical atomic frames, never logical handoff size.
    pub async fn build(
        stages: &[StageId],
        business: usize,
        lifecycle_facts: usize,
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
            _directory: directory,
        };
        for stage in stages {
            let path = fixture._directory.path().join(format!("{stage}.log"));
            let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
                DiskJournal::with_owner_in_run(path.clone(), JournalOwner::stage(*stage), run)
                    .unwrap(),
            );
            let mut events = Vec::with_capacity(business + lifecycle_facts);
            for index in 0..business {
                events.push(ChainEventFactory::data_event(
                    (*stage).into(),
                    "bench.business",
                    std::num::NonZeroU32::MIN,
                    serde_json::json!({"index": index, "body": "x".repeat(payload_bytes)}),
                ));
            }
            events.extend((0..lifecycle_facts).map(|_| running_fact(*stage)));
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
            assert_eq!(written.len(), business + lifecycle_facts);
            assert_eq!(
                journal.committed_position().await.unwrap(),
                written.len() as u64
            );

            fixture.journals.push(journal);
            fixture.paths.push(path);
            fixture.records.push(written);
        }
        fixture
    }
}

pub async fn causal_record(
    incoming_journals: usize,
    payload_bytes: usize,
) -> (ChainJournalRecord, CausalFrontier) {
    let run = FlowId::new();
    let mut frontier = CausalFrontier::default();
    for _ in 0..incoming_journals {
        let stage = StageId::new();
        let journal = MemoryJournal::with_owner_in_run(JournalOwner::stage(stage), run);
        let record = journal
            .append(
                ChainEventFactory::data_event(
                    stage.into(),
                    "bench.parent",
                    std::num::NonZeroU32::MIN,
                    Default::default(),
                ),
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
        .append(running_fact(stage), Default::default())
        .await
        .unwrap();
    let record = journal
        .append(
            ChainEventFactory::data_event(
                stage.into(),
                "bench.record",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"body": "x".repeat(payload_bytes)}),
            ),
            obzenflow_core::journal::AppendOptions::new(frontier.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        record.envelope.provenance.journal.vector_clock.clocks.len(),
        incoming_journals + 1
    );
    (record, frontier)
}
