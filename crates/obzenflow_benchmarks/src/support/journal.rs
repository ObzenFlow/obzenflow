// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub use super::{runtime, DEADLINE};
use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{CausalFrontier, ChainEventFactory, ChainPayload};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_infra::testing::journal_bench::FrameCorpus;
use std::sync::Arc;

pub fn execution(stage: StageId, payload: ExecutionPayload) -> ChainEvent {
    ChainEventFactory::create_with_context(
        stage.into(),
        ChainPayload::Execution(payload),
        FlowContext::new("benchmark_child", stage),
    )
}

pub fn running(stage: StageId) -> ChainEvent {
    execution(
        stage,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: stage }),
    )
}

pub fn execution_fact(stage: StageId, payload_bytes: usize) -> ChainEvent {
    execution(
        stage,
        ExecutionPayload::SourceCleanupFailed {
            stage_id: stage,
            stage_name: "benchmark_child".into(),
            error: "x".repeat(payload_bytes),
        },
    )
}

pub fn business(stage: StageId, payload_bytes: usize) -> ChainEvent {
    ChainEventFactory::data_event(
        stage.into(),
        "bench.business",
        serde_json::json!({"body":"x".repeat(payload_bytes)}),
    )
}

#[derive(Clone, Copy)]
pub struct Dimensions {
    pub clock: usize,
    pub advanced_inputs: usize,
    pub payload: usize,
}
impl Dimensions {
    pub fn name(self) -> String {
        format!(
            "clock_{}/advanced_inputs_{}/payload_{}",
            self.clock, self.advanced_inputs, self.payload
        )
    }
    pub fn json(self) -> serde_json::Value {
        serde_json::json!({"clock_components":self.clock,"advanced_inputs":self.advanced_inputs,"payload_string_bytes":self.payload})
    }
}
pub fn dimensions() -> [Dimensions; 6] {
    [
        (1, 0, 256),
        (33, 0, 256),
        (33, 4, 256),
        (33, 32, 256),
        (1025, 1024, 256),
        (33, 32, 8192),
    ]
    .map(|(clock, advanced_inputs, payload)| Dimensions {
        clock,
        advanced_inputs,
        payload,
    })
}

pub struct RecordFixture {
    pub record: ChainJournalRecord,
    pub canonical_bytes: usize,
    pub clock_bytes: usize,
    pub corpus: Arc<FrameCorpus>,
    // DefinitionStore::for_archive keeps weak entries. Retain the actual writer
    // so warm measurements cannot silently become carrier-cache misses.
    pub _journal: DiskJournal<ChainEvent>,
    pub _directory: tempfile::TempDir,
}
impl RecordFixture {
    pub async fn build(d: Dimensions) -> Self {
        assert!(d.advanced_inputs < d.clock);
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let mut roots = Vec::new();
        let mut initial = CausalFrontier::default();
        for _ in 1..d.clock {
            let stage = StageId::new();
            let journal = MemoryJournal::with_owner_in_run(JournalOwner::stage(stage), run);
            let row = journal
                .append(business(stage, 0), Default::default())
                .await
                .unwrap();
            initial
                .merge(&CausalFrontier::from_record(&row).unwrap())
                .unwrap();
            roots.push((stage, journal));
        }
        let stage = StageId::new();
        let path = directory.path().join("record.log");
        let journal =
            DiskJournal::with_owner_in_run(path.clone(), JournalOwner::stage(stage), run).unwrap();
        let seed = journal
            .append(running(stage), AppendOptions::new(initial))
            .await
            .unwrap();
        // Vary newly incorporated inputs independently of the retained clock's
        // width. The other coordinates are inherited from the preceding append.
        let mut advanced = CausalFrontier::default();
        for (stage, root) in roots.iter().take(d.advanced_inputs) {
            let row = root
                .append(business(*stage, 0), Default::default())
                .await
                .unwrap();
            advanced
                .merge(&CausalFrontier::from_record(&row).unwrap())
                .unwrap();
        }
        let record = journal
            .append(
                execution_fact(stage, d.payload),
                AppendOptions::new(advanced),
            )
            .await
            .unwrap();
        let metadata = &record.envelope.provenance.journal;
        assert_eq!(metadata.vector_clock.clocks.len(), d.clock);
        let corpus = FrameCorpus::load(&path, &[*seed.id(), *record.id()]).unwrap();
        let canonical_bytes = serde_json::to_vec(&record).unwrap().len();
        let clock_bytes = serde_json::to_vec(&metadata.vector_clock).unwrap().len();
        Self {
            record,
            canonical_bytes,
            clock_bytes,
            corpus,
            _journal: journal,
            _directory: directory,
        }
    }
}

pub struct History {
    pub journal: Arc<dyn Journal<ChainEvent>>,
    pub rows: Vec<ChainJournalRecord>,
    pub events: Vec<ChainEvent>,
    pub corpus: Arc<FrameCorpus>,
    pub path: std::path::PathBuf,
    pub stage: StageId,
    pub run: FlowId,
    pub group: usize,
    pub _directory: tempfile::TempDir,
}
impl History {
    pub async fn build(
        count: usize,
        payload: usize,
        group: usize,
        execution_fact_every: usize,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.log");
        let stage = StageId::new();
        let run = FlowId::new();
        let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
            DiskJournal::with_owner_in_run(path.clone(), JournalOwner::stage(stage), run).unwrap(),
        );
        let events: Vec<_> = (0..count)
            .map(|i| {
                if execution_fact_every > 0 && i % execution_fact_every == 0 {
                    running(stage)
                } else {
                    business(stage, payload)
                }
            })
            .collect();
        let rows = append_events(&journal, events.clone(), group).await;
        let corpus =
            FrameCorpus::load(&path, &rows.iter().map(|r| *r.id()).collect::<Vec<_>>()).unwrap();
        Self {
            journal,
            rows,
            events,
            corpus,
            path,
            stage,
            run,
            group,
            _directory: directory,
        }
    }
}

pub async fn append_events(
    journal: &Arc<dyn Journal<ChainEvent>>,
    events: Vec<ChainEvent>,
    group: usize,
) -> Vec<ChainJournalRecord> {
    let mut rows = Vec::with_capacity(events.len());
    if group == 1 {
        for event in events {
            rows.push(journal.append(event, Default::default()).await.unwrap());
        }
    } else {
        for (index, chunk) in events.chunks(group).enumerate() {
            rows.extend(
                journal
                    .append_group(
                        &format!("group-{index}"),
                        chunk.to_vec(),
                        Default::default(),
                    )
                    .await
                    .unwrap(),
            );
        }
    }
    rows
}
