// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::fixtures;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use obzenflow_core::event::{CausalCoordinate, CausalFrontier, JournalClock};
use obzenflow_core::journal::limits::record_bytes;
use obzenflow_core::EventId;
use std::cell::LazyCell;
use std::hint::black_box;

pub fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut group = c.benchmark_group("causal_components");
    group.throughput(Throughput::Elements(1));
    for (width, payload) in [(1, 256), (32, 256), (1024, 256), (32, 8192)] {
        let fixture = LazyCell::new(|| {
            let (record, input) = runtime.block_on(fixtures::causal_record(width, payload));
            let previous = JournalClock::from_record(&record).unwrap();
            let overlapping = CausalFrontier::from_record(&record).unwrap();
            (record, input, previous, overlapping)
        });
        let id = format!("w{width}_p{payload}");
        group.bench_function(BenchmarkId::new("journal_clock_restore", &id), |b| {
            let (record, _, _, _) = &*fixture;
            b.iter(|| black_box(JournalClock::from_record(black_box(record)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("byte_accounting", &id), |b| {
            let (record, _, _, _) = &*fixture;
            b.iter(|| black_box(record_bytes(black_box(record)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("frontier_from_record", &id), |b| {
            let (record, _, _, _) = &*fixture;
            b.iter(|| black_box(CausalFrontier::from_record(black_box(record)).unwrap()));
        });
        group.bench_function(BenchmarkId::new("merge_overlapping", &id), |b| {
            let (_, input, _, overlapping) = &*fixture;
            b.iter_batched_ref(
                || overlapping.clone(),
                |target| {
                    target.merge(black_box(input)).unwrap();
                    black_box(&*target);
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function(BenchmarkId::new("prepare_append", &id), |b| {
            let (_, input, previous, _) = &*fixture;
            let event = EventId::new();
            let prepare = || {
                JournalClock::prepare(
                    previous.reference.run_id,
                    CausalCoordinate::new(previous.reference.journal_writer_id),
                    event,
                    Some(previous),
                    input,
                )
                .unwrap()
            };
            let (check, _) = prepare();
            assert_eq!(check.reference.sequence, previous.reference.sequence + 1);
            assert_eq!(check.clock.clocks.len(), width + 1);
            b.iter(|| black_box(prepare()));
        });
    }
    group.finish();
}
