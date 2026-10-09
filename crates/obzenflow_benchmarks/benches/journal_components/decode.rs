// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fixtures::{self, History, DEADLINE};
use criterion::{BenchmarkId, Criterion, Throughput};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_core::StageId;
use std::cell::LazyCell;
use std::hint::black_box;
use std::time::{Duration, Instant};

pub fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut group = c.benchmark_group("disk_components");
    group.throughput(Throughput::Elements(64));
    for (payload, physical_group) in [(256, 1), (8192, 1), (256, 64)] {
        let history = LazyCell::new(|| {
            runtime.block_on(History::build(
                &[StageId::new()],
                64,
                0,
                payload,
                physical_group,
            ))
        });
        let case = format!("p{payload}_g{physical_group}");
        declare(
            &format!("disk_components/reader_next/{case}"),
            Category::Read,
            "Read 64 records; reader opening excluded",
        );
        group.bench_function(BenchmarkId::new("reader_next", &case), |b| {
            let expected: Vec<_> = history.records[0].iter().map(|row| *row.id()).collect();
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut reader = history.journals[0].reader().await.unwrap();
                        let mut found = Vec::with_capacity(64);
                        let measured = tokio::time::timeout(DEADLINE, async {
                            let start = Instant::now();
                            for _ in 0..64 {
                                let row = reader
                                    .next()
                                    .await
                                    .unwrap()
                                    .expect("complete benchmark history");
                                found.push(*row.id());
                                black_box(row);
                            }
                            start.elapsed()
                        })
                        .await
                        .expect("journal scan exceeded benchmark deadline");
                        assert_eq!(found, expected);
                        assert_eq!(reader.position(), 64);
                        assert!(reader.next().await.unwrap().is_none());
                        elapsed += measured;
                    }
                    elapsed
                })
            });
        });
    }
    group.finish();
}
