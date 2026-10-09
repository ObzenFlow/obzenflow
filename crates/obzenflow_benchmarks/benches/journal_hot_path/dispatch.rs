// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{declare, fixtures, measure, Category, Census, Meter, Sample};
use criterion::{Criterion, Throughput};
use std::cell::LazyCell;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

fn actual(runtime: &Runtime, history: &fixtures::History, readers: usize) -> Sample {
    let control = super::control::selected();
    let full = runtime.block_on(async {
        let mut opened = Vec::new();
        for _ in 0..readers {
            opened.push(history.journal.reader().await.unwrap());
        }
        opened
    });
    let meter = Meter::start();
    let start = Instant::now();
    let mut results = runtime.block_on(async {
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
    if control == super::control::Control::SlowReader {
        // Delay belongs inside the measured complete operation. The raw output
        // and work oracle are identical; only completion is deliberately slow.
        std::thread::sleep(meter.elapsed() * 2);
    }
    let elapsed = meter.elapsed();
    let mut sample = meter.finish(elapsed);
    if control == super::control::Control::MissingReaderOutput {
        results[0].0.pop();
    }
    let expected: Vec<_> = history.rows.iter().map(|r| *r.id()).collect();
    for (ids, _) in &results {
        assert_eq!(
            ids, &expected,
            "actual reader output completeness and order"
        );
    }
    assert_eq!(results.len(), readers);
    sample.completed("complete_readers", results.len() as u64);
    sample.completed(
        "complete_records",
        results.iter().map(|(ids, _)| ids.len() as u64).sum(),
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
        let case = format!("full/actual_reader/readers_{readers}");
        let full = format!("reader_dispatch/{case}");
        declare(
            &full,
            Category::Read,
            &format!(
                "Concurrent readers: {readers}; spawn tasks and read 64 records each; opening excluded"
            ),
        );
        let input = serde_json::json!({"readers":readers,"records_per_reader":64,"physical_group_size":1,"encoded_corpus_in_memory":false});
        let mut taken = false;
        group.bench_function(&case, |b| {
            measure(b, censuses, &mut taken, &full, &input, || {
                actual(runtime, &history, readers)
            })
        });
    }
    group.finish();
}
