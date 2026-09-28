// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fixtures::{self, History, DEADLINE};
use criterion::{BenchmarkId, Criterion, Throughput};
use obzenflow_core::{EventId, StageId};
use obzenflow_infra::testing::journal_bench::{DecodeCursor, FrameCorpus};
use std::cell::LazyCell;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn validate(cursor: &DecodeCursor, frames: usize, records: usize) {
    assert!(cursor.finished());
    assert_eq!(cursor.work().frames, frames);
    assert_eq!(cursor.work().records, records);
    assert_eq!(
        cursor.work().sequence_sum,
        (records * (records + 1) / 2) as u64
    );
}

pub fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut group = c.benchmark_group("disk_components");
    group.throughput(Throughput::Elements(64));
    for (payload, physical_group) in [(256, 1), (8192, 1), (256, 64)] {
        let fixture = LazyCell::new(|| prepare(&runtime, payload, physical_group));
        let case = format!("p{payload}_g{physical_group}");
        for admit in [false, true] {
            let name = if admit {
                "decode_and_continuity"
            } else {
                "decode"
            };
            group.bench_function(BenchmarkId::new(name, &case), |b| {
                let (_, _, corpus) = &*fixture;
                b.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut cursor = corpus.cursor();
                        let start = Instant::now();
                        cursor.decode(corpus.frames(), admit).unwrap();
                        elapsed += start.elapsed();
                        validate(&cursor, corpus.frames(), 64);
                        black_box(cursor);
                    }
                    elapsed
                });
            });
        }
        group.bench_function(BenchmarkId::new("reader_next", &case), |b| {
            let (history, expected, _) = &*fixture;
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
                        assert_eq!(&found, expected);
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

    let fixture = LazyCell::new(|| prepare(&runtime, 256, 1));
    let mut dispatch = c.benchmark_group("decode_dispatch");
    for readers in [1, 8] {
        dispatch.throughput(Throughput::Elements(64 * readers as u64));
        for quantum in [1, 64] {
            let case = format!("readers{readers}_frames_per_job{quantum}");
            dispatch.bench_function(case, |b| {
                let (_, _, corpus) = &*fixture;
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let mut elapsed = Duration::ZERO;
                        for _ in 0..iterations {
                            let cursors: Vec<_> = (0..readers).map(|_| corpus.cursor()).collect();
                            let (measured, completed) = tokio::time::timeout(DEADLINE, async {
                                let start = Instant::now();
                                let mut jobs = tokio::task::JoinSet::new();
                                for mut cursor in cursors {
                                    jobs.spawn(async move {
                                        while !cursor.finished() {
                                            cursor = tokio::task::spawn_blocking(move || {
                                                cursor.decode(quantum, true).unwrap();
                                                cursor
                                            })
                                            .await
                                            .unwrap();
                                        }
                                        cursor
                                    });
                                }
                                let mut completed = Vec::new();
                                while let Some(result) = jobs.join_next().await {
                                    completed.push(result.unwrap());
                                }
                                (start.elapsed(), completed)
                            })
                            .await
                            .expect("decode dispatch exceeded benchmark deadline");
                            assert_eq!(completed.len(), readers);
                            for cursor in &completed {
                                validate(cursor, 64, 64);
                            }
                            elapsed += measured;
                        }
                        elapsed
                    })
                });
            });
        }
    }
    dispatch.finish();
}

fn prepare(
    runtime: &tokio::runtime::Runtime,
    payload: usize,
    physical_group: usize,
) -> (History, Vec<EventId>, Arc<FrameCorpus>) {
    let history = runtime.block_on(History::build(
        &[StageId::new()],
        64,
        0,
        payload,
        physical_group,
    ));
    let expected: Vec<_> = history.records[0].iter().map(|row| *row.id()).collect();
    let corpus = FrameCorpus::load(&history.paths[0], &expected).unwrap();
    assert_eq!(corpus.frames(), 64 / physical_group);
    eprintln!("disk fixture: payload={payload}, physical_group={physical_group}, records=64, frames={}, encoded_bytes={}", corpus.frames(), corpus.encoded_bytes());
    (history, expected, corpus)
}
