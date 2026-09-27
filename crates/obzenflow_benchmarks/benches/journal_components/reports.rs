// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fixtures::{self, Backend, History, DEADLINE};
use criterion::{Criterion, Throughput};
use obzenflow_core::StageId;
use obzenflow_runtime::supervised_base::report_reader::{ReportRead, ReportReaders};
use std::cell::LazyCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::poll_fn;
use std::time::{Duration, Instant};

pub fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut group = c.benchmark_group("report_discovery");
    for backend in [Backend::Memory, Backend::Disk] {
        for (readers, prefix, payload) in [
            (1, 0, 256),
            (1, 64, 256),
            (1, 1024, 256),
            (1, 10_000, 256),
            (1, 1024, 8192),
            (8, 1024, 256),
            (32, 128, 256),
        ] {
            group.throughput(Throughput::Elements((readers * (prefix + 1)) as u64));
            let case = format!(
                "{}/readers{readers}_prefix{prefix}_p{payload}",
                backend.name()
            );
            let fixture = LazyCell::new(|| {
                let stages: Vec<_> = (0..readers).map(|_| StageId::new()).collect();
                let history =
                    runtime.block_on(History::build(backend, &stages, prefix, 1, payload, 1));
                let expected: BTreeSet<_> = history.reports.iter().map(|row| *row.id()).collect();
                let boundaries: BTreeMap<_, _> = history
                    .journals
                    .iter()
                    .map(|journal| (*journal.id(), (prefix + 1) as u64))
                    .collect();
                (history, expected, boundaries)
            });
            group.bench_function(case, |b| {
                let (history, expected, boundaries) = &*fixture;
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let mut reports = ReportReaders::default();
                            let mut found = BTreeSet::new();
                            let mut coverage = BTreeMap::new();
                            // Start before registering tasks: no unmeasured prefetch.
                            let measured = tokio::time::timeout(DEADLINE, async {
                                let start = Instant::now();
                                for journal in &history.journals {
                                    reports.stage(journal.clone());
                                }
                                while coverage != *boundaries {
                                    match poll_fn(|cx| reports.poll_next(cx)).await.unwrap() {
                                        ReportRead::Record(row) => {
                                            assert!(found.insert(*row.id()), "duplicate report");
                                        }
                                        ReportRead::Coverage { journal, through } => {
                                            coverage.insert(journal, through);
                                        }
                                    }
                                }
                                start.elapsed()
                            })
                            .await
                            .expect("report discovery exceeded benchmark deadline");
                            assert_eq!(&found, expected);
                            assert!(reports.initial_prefix_complete());
                            let diagnostics = reports.diagnostics();
                            assert_eq!(
                                diagnostics.iter().map(|d| d.scanned_records).sum::<u64>(),
                                history.total_records as u64
                            );
                            assert_eq!(
                                diagnostics.iter().map(|d| d.selected_records).sum::<u64>(),
                                readers as u64
                            );
                            reports.shutdown_for_benchmark().await;
                            elapsed += measured;
                        }
                        elapsed
                    })
                });
            });
        }
    }
    group.finish();

    let mut ready = c.benchmark_group("ready_report_handoff");
    for count in [1, 10, 100] {
        ready.throughput(Throughput::Elements(count as u64));
        let fixture = LazyCell::new(|| {
            let stages: Vec<_> = (0..count).map(|_| StageId::new()).collect();
            let history = runtime.block_on(History::build(Backend::Memory, &stages, 0, 1, 0, 1));
            let expected: BTreeSet<_> = history.reports.iter().map(|row| *row.id()).collect();
            (history, expected)
        });
        ready.bench_function(format!("journals{count}"), |b| {
            let (history, expected) = &*fixture;
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut readers = ReportReaders::default();
                        for journal in &history.journals {
                            readers.stage(journal.clone());
                        }
                        tokio::time::timeout(DEADLINE, async {
                            while !readers.diagnostics().iter().all(|d| d.ready) {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .expect("report readers never became ready");
                        let mut ids = Vec::with_capacity(count);
                        elapsed += tokio::time::timeout(DEADLINE, async {
                            let start = Instant::now();
                            for _ in 0..count {
                                let row = poll_fn(|cx| readers.poll_next(cx)).await.unwrap();
                                let ReportRead::Record(row) = row else {
                                    panic!("one ready report per journal must precede coverage");
                                };
                                ids.push(*row.id());
                            }
                            start.elapsed()
                        })
                        .await
                        .expect("ready handoff exceeded benchmark deadline");
                        assert_eq!(&ids.into_iter().collect::<BTreeSet<_>>(), expected);
                        readers.shutdown_for_benchmark().await;
                    }
                    elapsed
                })
            });
        });
    }
    ready.finish();
}
