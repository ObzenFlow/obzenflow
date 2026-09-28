// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fixtures::{self, Backend, History, DEADLINE};
use criterion::{Criterion, Throughput};
use obzenflow_core::event::{SystemEvent, SystemEventFactory};
use obzenflow_core::{Journal, JournalOwner, SystemId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_runtime::testing::pipeline::ParentAdmission;
use std::cell::LazyCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut admission = c.benchmark_group("parent_admission");
    for stages in [1, 10, 100] {
        admission.throughput(Throughput::Elements(stages as u64));
        let fixture = LazyCell::new(|| {
            let (topology, ids) = ParentAdmission::topology(stages);
            let history = runtime.block_on(History::build(Backend::Memory, &ids, 0, 1, 0, 1));
            let system = SystemId::new();
            let journal = Arc::new(MemoryJournal::<SystemEvent>::with_owner_in_run(
                JournalOwner::system(system),
                history.records[0][0].envelope.provenance.journal.run_id,
            ));
            (topology, history, journal)
        });
        admission.bench_function(format!("running_reports{stages}"), |b| {
            let (topology, history, journal) = &*fixture;
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut parent = ParentAdmission::new(journal.clone(), topology.clone());
                        let rows = history.reports.clone();
                        let (measured, actions) = tokio::time::timeout(DEADLINE, async {
                            let start = Instant::now();
                            let actions = parent.admit(rows).await;
                            (start.elapsed(), actions)
                        })
                        .await
                        .expect("parent admission exceeded benchmark deadline");
                        assert_eq!(actions, 0);
                        parent.validate(&history.reports);
                        parent.settle().await;
                        elapsed += measured;
                    }
                    elapsed
                })
            });
        });
    }
    admission.finish();

    let mut publication = c.benchmark_group("parent_publication");
    publication.throughput(Throughput::Elements(16));
    for backend in [Backend::Memory, Backend::Disk] {
        for width in [1, 32, 1024] {
            let fixture = LazyCell::new(|| {
                let (record, input) = runtime.block_on(fixtures::causal_record(width, 0));
                let run = record.envelope.provenance.journal.run_id;
                let (topology, _) = ParentAdmission::topology(1);
                (run, input, topology)
            });
            publication.bench_function(format!("{}/w{width}_records16", backend.name()), |b| {
                let (run, input, topology) = &*fixture;
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let directory = tempfile::tempdir().unwrap();
                            let system = SystemId::new();
                            let journal: Arc<dyn Journal<SystemEvent>> = match backend {
                                Backend::Memory => Arc::new(MemoryJournal::with_owner_in_run(
                                    JournalOwner::system(system),
                                    *run,
                                )),
                                Backend::Disk => Arc::new(
                                    DiskJournal::with_owner_in_run(
                                        directory.path().join("pipeline.log"),
                                        JournalOwner::system(system),
                                        *run,
                                    )
                                    .unwrap(),
                                ),
                            };
                            let parent = ParentAdmission::new(journal.clone(), topology.clone());
                            parent.incorporate(input);
                            let events: Vec<_> = (0..16)
                                .map(|_| SystemEventFactory::new(system).pipeline_running())
                                .collect();
                            let ids: Vec<_> = events.iter().map(|event| event.id).collect();
                            let (measured, receipts) = tokio::time::timeout(DEADLINE, async {
                                let start = Instant::now();
                                let receipts = parent.publish(events).await;
                                (start.elapsed(), receipts)
                            })
                            .await
                            .expect("pipeline publication exceeded benchmark deadline");
                            parent.settle().await;
                            assert_eq!(
                                receipts.iter().map(|row| *row.id()).collect::<Vec<_>>(),
                                ids
                            );
                            assert_eq!(journal.committed_position().await.unwrap(), 16);
                            for (index, row) in receipts.iter().enumerate() {
                                assert_eq!(row.position(), index as u64 + 1);
                                for (coordinate, sequence) in &input.clock().clocks {
                                    assert!(
                                        row.journal().vector_clock.get(coordinate) >= *sequence
                                    );
                                }
                            }
                            elapsed += measured;
                        }
                        elapsed
                    })
                });
            });
        }
    }
    publication.finish();
}
