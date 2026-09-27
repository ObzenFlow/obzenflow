// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, RateLimiterFact, StageLifecycleFact,
};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{CausalFrontier, ChainEventFactory, ChainPayload, SupervisorRecord};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalId, JournalOwner, StageId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_infra::testing::journal_bench::FrameCorpus;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub enum Pattern {
    Prefix(usize),
    Business(usize),
    Reports(usize),
    Interleaved,
    Mixed(Vec<usize>),
    Eligibility,
}

#[derive(Clone)]
pub struct Case {
    pub name: String,
    pub pattern: Pattern,
    pub group: usize,
    pub payload: usize,
    pub external: usize,
    pub readers: usize,
    pub cold: bool,
}
impl Case {
    pub fn new(name: &str, pattern: Pattern) -> Self {
        Self {
            name: name.into(),
            pattern,
            group: 1,
            payload: 256,
            external: 0,
            readers: 1,
            cold: false,
        }
    }
    pub fn rows(&self) -> usize {
        match &self.pattern {
            Pattern::Prefix(n) => n + 1,
            Pattern::Business(n) | Pattern::Reports(n) => *n,
            Pattern::Interleaved => 1024,
            Pattern::Mixed(_) => 64,
            Pattern::Eligibility => 4,
        }
    }
}

pub fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for n in [0, 64, 1024, 10_000] {
        cases.push(Case::new(
            &format!("business_prefix_{n}/payload_256"),
            Pattern::Prefix(n),
        ));
    }
    let mut large = Case::new("business_prefix_1024/payload_8192", Pattern::Prefix(1024));
    large.payload = 8192;
    cases.push(large);
    cases.push(Case::new("empty_journal", Pattern::Business(0)));
    cases.push(Case::new(
        "business_only_1024/single_record_frames",
        Pattern::Business(1024),
    ));
    cases.push(Case::new(
        "reports_only_256/single_record_frames",
        Pattern::Reports(256),
    ));
    cases.push(Case::new(
        "interleaved_1024/report_every_8",
        Pattern::Interleaved,
    ));
    for (name, pattern) in [
        (
            "business_only_1024/atomic_groups_64",
            Pattern::Business(1024),
        ),
        ("reports_only_256/atomic_groups_64", Pattern::Reports(256)),
        ("mixed_group_64/report_first", Pattern::Mixed(vec![0])),
        ("mixed_group_64/report_middle", Pattern::Mixed(vec![32])),
        ("mixed_group_64/report_last", Pattern::Mixed(vec![63])),
        (
            "mixed_group_64/reports_first_middle_last",
            Pattern::Mixed(vec![0, 32, 63]),
        ),
    ] {
        let mut case = Case::new(name, pattern);
        case.group = 64;
        cases.push(case);
    }
    for (external, prefixes) in [(32, [32, 128]), (1024, [4, 16])] {
        for n in prefixes {
            let mut case = Case::new(
                &format!("business_prefix_{n}/external_coordinates_{external}"),
                Pattern::Prefix(n),
            );
            case.external = external;
            cases.push(case);
        }
    }
    for external in [0, 32] {
        let mut case = Case::new(
            &format!("cold_definitions/business_prefix_64/external_coordinates_{external}"),
            Pattern::Prefix(64),
        );
        case.external = external;
        case.cold = true;
        cases.push(case);
    }
    let mut eligibility = Case::new(
        "mixed_group_4/owner_and_execution_selection",
        Pattern::Eligibility,
    );
    eligibility.group = 4;
    cases.push(eligibility);
    let mut concurrent = Case::new(
        "concurrent_readers_8/business_prefix_128",
        Pattern::Prefix(128),
    );
    concurrent.readers = 8;
    cases.push(concurrent);
    cases
}

fn execution(stage: StageId, payload: ExecutionPayload) -> ChainEvent {
    ChainEventFactory::create_with_context(
        stage.into(),
        ChainPayload::Execution(payload),
        FlowContext::new("benchmark_child", stage),
    )
}

pub fn events(case: &Case, stage: StageId) -> Vec<(ChainEvent, bool)> {
    (0..case.rows())
        .map(|index| {
            if matches!(case.pattern, Pattern::Eligibility) {
                match index {
                    0 => {
                        return (
                            execution(
                                StageId::new(),
                                ExecutionPayload::StageLifecycle(StageLifecycleFact::Running {
                                    stage_id: stage,
                                }),
                            ),
                            false,
                        )
                    }
                    1 => {
                        return (
                            execution(
                                stage,
                                ExecutionPayload::RateLimiter(RateLimiterFact::Delayed {
                                    delay_ms: 1,
                                    current_rate: 1.0,
                                    limit_rate: 1.0,
                                }),
                            ),
                            false,
                        )
                    }
                    _ => {}
                }
            }
            let selected = match &case.pattern {
                Pattern::Prefix(n) => index == *n,
                Pattern::Business(_) => false,
                Pattern::Reports(_) => true,
                Pattern::Interleaved => index % 8 == 7,
                Pattern::Mixed(positions) => positions.contains(&index),
                Pattern::Eligibility => index == 3,
            };
            if selected {
                (
                    execution(
                        stage,
                        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running {
                            stage_id: stage,
                        }),
                    ),
                    true,
                )
            } else {
                (
                    ChainEventFactory::data_event(
                        stage.into(),
                        "bench.business",
                        serde_json::json!({"index":index,"body":"x".repeat(case.payload)}),
                    ),
                    false,
                )
            }
        })
        .collect()
}

async fn frontier(run: FlowId, width: usize) -> CausalFrontier {
    let mut input = CausalFrontier::default();
    for _ in 0..width {
        let stage = StageId::new();
        let source = MemoryJournal::with_owner_in_run(JournalOwner::stage(stage), run);
        let record = source
            .append(
                ChainEventFactory::data_event(stage.into(), "bench.cause", Default::default()),
                Default::default(),
            )
            .await
            .unwrap();
        input
            .merge(&CausalFrontier::from_record(&record).unwrap())
            .unwrap();
    }
    input
}

pub struct History {
    pub case: Case,
    pub journals: Vec<Arc<dyn Journal<ChainEvent>>>,
    pub paths: Vec<PathBuf>,
    pub rows: BTreeMap<JournalId, Vec<ChainJournalRecord>>,
    pub reports: BTreeMap<JournalId, Vec<SupervisorRecord>>,
    pub business: usize,
    pub encoded_bytes: u64,
    pub frames: usize,
    pub _directory: tempfile::TempDir,
}

impl History {
    pub async fn build(case: Case) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let input = frontier(run, case.external).await;
        let mut history = Self {
            case,
            journals: vec![],
            paths: vec![],
            rows: BTreeMap::new(),
            reports: BTreeMap::new(),
            business: 0,
            encoded_bytes: 0,
            frames: 0,
            _directory: directory,
        };
        for index in 0..history.case.readers {
            let stage = StageId::new();
            let path = history._directory.path().join(format!("child{index}.log"));
            let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
                DiskJournal::with_owner_in_run(path.clone(), JournalOwner::stage(stage), run)
                    .unwrap(),
            );
            let events = events(&history.case, stage);
            let mut rows = Vec::new();
            for (index, group) in events.chunks(history.case.group).enumerate() {
                let options = AppendOptions::new(input.clone());
                if history.case.group == 1 {
                    rows.push(journal.append(group[0].0.clone(), options).await.unwrap());
                } else {
                    rows.extend(
                        journal
                            .append_group(
                                &format!("group-{index}"),
                                group.iter().map(|(e, _)| e.clone()).collect(),
                                options,
                            )
                            .await
                            .unwrap(),
                    );
                }
                history.frames += 1;
            }
            assert_eq!(rows.len(), events.len());
            let mut expected = Vec::new();
            for (row, (_, selected)) in rows.iter().zip(&events) {
                assert_eq!(
                    row.envelope.provenance.journal.vector_clock.clocks.len(),
                    history.case.external + 1
                );
                history.business += usize::from(matches!(row.payload, ChainPayload::Fact(_)));
                if *selected {
                    expected.push(SupervisorRecord::from_chain(row.clone()).unwrap());
                }
            }
            history.encoded_bytes += std::fs::metadata(&path).unwrap().len();
            history.reports.insert(*journal.id(), expected);
            history.rows.insert(*journal.id(), rows);
            history.paths.push(path);
            history.journals.push(journal);
        }
        history
    }

    pub fn corpus(&self) -> Arc<FrameCorpus> {
        let ids: Vec<_> = self.rows[self.journals[0].id()]
            .iter()
            .map(|r| *r.id())
            .collect();
        FrameCorpus::load(&self.paths[0], &ids).unwrap()
    }

    pub fn input_description(&self) -> serde_json::Value {
        serde_json::json!({
            "journals":self.case.readers, "records":self.case.rows()*self.case.readers,
            "business_records":self.business, "reports":self.reports.values().map(Vec::len).sum::<usize>(),
            "frames":self.frames, "encoded_bytes":self.encoded_bytes,
            "body_bytes":self.case.payload, "clock_components":self.case.external+1,
            "cold_definitions":self.case.cold
        })
    }
}
