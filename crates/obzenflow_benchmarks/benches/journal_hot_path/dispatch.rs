// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{fixtures, measure, Census, Meter, Sample};
use criterion::{Criterion, Throughput};
use obzenflow_core::benchmark::{add, Counter};
use std::cell::LazyCell;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

fn controlled(
    runtime: &Runtime,
    history: &fixtures::History,
    readers: usize,
    quantum: usize,
) -> Sample {
    let cursors: Vec<_> = (0..readers).map(|_| history.corpus.cursor()).collect();
    let meter = Meter::start();
    let start = Instant::now();
    let output = runtime.block_on(async {
        tokio::time::timeout(fixtures::DEADLINE, async {
            let mut tasks = tokio::task::JoinSet::new();
            for mut cursor in cursors {
                tasks.spawn(async move {
                    let mut first = None;
                    while !cursor.finished() {
                        if quantum == 0 {
                            cursor.decode(64, true).unwrap();
                        } else {
                            add(Counter::DecodeBlockingJobs, 1);
                            cursor = tokio::task::spawn_blocking(move || {
                                cursor.decode(quantum, true).unwrap();
                                cursor
                            })
                            .await
                            .unwrap();
                        }
                        first.get_or_insert_with(|| start.elapsed());
                    }
                    (cursor, first.unwrap())
                });
            }
            let mut output = Vec::new();
            while let Some(task) = tasks.join_next().await {
                output.push(task.unwrap());
            }
            output
        })
        .await
        .expect("dispatch workload exceeded deadline")
    });
    let elapsed = meter.elapsed();
    let mut sample = meter.finish(elapsed);
    for (cursor, _) in &output {
        assert_eq!(cursor.work().records, 64);
        assert_eq!(cursor.work().frames, 64);
        assert_eq!(cursor.work().sequence_sum, 64 * 65 / 2);
    }
    sample.expect_work("verified_frames", (readers * 64) as u64);
    sample.expect_work("payload_json_decodes", (readers * 64) as u64);
    sample.expect_work(
        "decode_blocking_jobs",
        if quantum == 0 {
            0
        } else {
            (readers * 64 / quantum) as u64
        },
    );
    sample.observations = serde_json::json!({
        "first_completed_job_ns":output.iter().map(|(_,t)|t.as_nanos() as u64).min(),
        "last_reader_first_job_ns":output.iter().map(|(_,t)|t.as_nanos() as u64).max(),
    });
    sample
}

fn actual(runtime: &Runtime, history: &fixtures::History, readers: usize) -> Sample {
    let full = runtime.block_on(async {
        let mut opened = Vec::new();
        for _ in 0..readers {
            opened.push(history.journal.reader().await.unwrap());
        }
        opened
    });
    let meter = Meter::start();
    let start = Instant::now();
    let results = runtime.block_on(async {
        tokio::time::timeout(fixtures::DEADLINE, async {
            let mut tasks = tokio::task::JoinSet::new();
            for mut reader in full {
                tasks.spawn(async move {
                    let mut ids = Vec::with_capacity(64);
                    let mut first = Duration::ZERO;
                    while let Some(row) = reader.next().await.unwrap() {
                        if ids.is_empty() {
                            first = start.elapsed();
                        }
                        ids.push(*row.id());
                    }
                    (ids, first)
                });
            }
            let mut results = Vec::new();
            while let Some(task) = tasks.join_next().await {
                results.push(task.unwrap());
            }
            results
        })
        .await
        .expect("actual reader workload exceeded deadline")
    });
    let elapsed = meter.elapsed();
    let mut sample = meter.finish(elapsed);
    let expected: Vec<_> = history.rows.iter().map(|r| *r.id()).collect();
    for (ids, _) in &results {
        assert_eq!(ids, &expected);
    }
    assert_eq!(results.len(), readers);
    sample.expect_work("payload_json_decodes", (readers * 64) as u64);
    sample.expect_work(
        "primary_frame_bytes",
        (readers * history.corpus.encoded_bytes()) as u64,
    );
    sample.observations = serde_json::json!({
        "first_record_ns":results.iter().map(|(_,t)|t.as_nanos() as u64).min(),
        "last_reader_first_record_ns":results.iter().map(|(_,t)|t.as_nanos() as u64).max(),
    });
    sample
}

pub fn bench(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    let history = LazyCell::new(|| runtime.block_on(fixtures::History::build(64, 256, 1, 1)));
    let mut group = c.benchmark_group("reader_dispatch");
    for readers in [1, 8, 32] {
        group.throughput(Throughput::Elements((64 * readers) as u64));
        {
            let kind = "full";
            for quantum in [0, 1, 8, 64, usize::MAX] {
                let boundary = match quantum {
                    0 => "inline".into(),
                    usize::MAX => "actual_reader".into(),
                    q => format!("frames_per_job_{q}"),
                };
                let case = format!("{kind}/{boundary}/readers_{readers}");
                let input = serde_json::json!({"readers":readers,"records_per_reader":64,"physical_group_size":1,"encoded_corpus_in_memory":quantum != usize::MAX});
                let mut taken = false;
                group.bench_function(&case, |b| {
                    measure(
                        b,
                        censuses,
                        &mut taken,
                        &format!("reader_dispatch/{case}"),
                        &input,
                        || {
                            if quantum == usize::MAX {
                                actual(runtime, &history, readers)
                            } else {
                                controlled(runtime, &history, readers, quantum)
                            }
                        },
                    )
                });
            }
        }
    }
    group.finish();
}
