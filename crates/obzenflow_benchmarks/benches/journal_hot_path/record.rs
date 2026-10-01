// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{fixtures, measure, timed, Census, Sample};
use criterion::{Criterion, Throughput};
use obzenflow_core::event::JournalClock;
use obzenflow_core::journal::limits::record_bytes;
use obzenflow_core::Journal;
use std::cell::LazyCell;
use tokio::runtime::Runtime;

#[derive(Clone, Copy)]
enum Operation {
    CanonicalBytes,
    RestoreClock,
    CloneClock,
    ClockBytes,
    ReadRecords,
}

fn operation(f: &fixtures::RecordFixture, op: Operation, runtime: &Runtime) -> Sample {
    let record = &f.record;
    let journal = &record.envelope.provenance.journal;
    match op {
        Operation::CanonicalBytes => {
            let (bytes, mut sample) = timed(|| record_bytes(record).unwrap());
            assert_eq!(bytes, f.canonical_bytes);
            sample.completed("canonical_bytes", bytes as u64);
            sample
        }
        Operation::RestoreClock => {
            let (clock, mut sample) = timed(|| JournalClock::from_record(record).unwrap());
            assert_eq!(clock.reference.event_id, *record.id());
            assert_eq!(clock.clock, journal.vector_clock);
            sample.completed("restored_clock_components", clock.clock.clocks.len() as u64);
            sample
        }
        Operation::CloneClock => {
            let (clock, mut sample) = timed(|| journal.vector_clock.clone());
            assert_eq!(clock, journal.vector_clock);
            sample.completed("cloned_clock_components", clock.clocks.len() as u64);
            sample
        }
        Operation::ClockBytes => {
            let (bytes, mut sample) = timed(|| record_bytes(&journal.vector_clock).unwrap());
            assert_eq!(bytes, f.clock_bytes);
            sample.completed("clock_bytes", bytes as u64);
            sample
        }
        Operation::ReadRecords => {
            // Public reader creation and full consumption, including admission.
            // No private decoder, definition-cache control or copied codec loop.
            let (rows, mut sample) = timed(|| {
                runtime.block_on(async {
                    tokio::time::timeout(fixtures::DEADLINE, async {
                        let mut reader = f.journal.reader().await.unwrap();
                        let mut rows = Vec::new();
                        while let Some(row) = reader.next().await.unwrap() {
                            rows.push(row);
                        }
                        assert_eq!(reader.position(), f.rows.len() as u64);
                        rows
                    })
                    .await
                    .expect("record read watchdog")
                })
            });
            assert_eq!(
                serde_json::to_value(&rows).unwrap(),
                serde_json::to_value(&f.rows).unwrap()
            );
            for row in &rows {
                JournalClock::from_record(row).expect("reader returns admitted causal records");
            }
            sample.completed("complete_records", rows.len() as u64);
            sample
        }
    }
}

pub fn bench(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    let fixtures: Vec<_> = fixtures::dimensions()
        .into_iter()
        .map(|d| {
            (
                d,
                LazyCell::new(move || runtime.block_on(fixtures::RecordFixture::build(d))),
            )
        })
        .collect();
    for (name, operations) in [
        (
            "record_accounting",
            &[("canonical_bytes", Operation::CanonicalBytes)][..],
        ),
        (
            "causal_record_work",
            &[
                ("journal_clock_restore", Operation::RestoreClock),
                ("clock_clone", Operation::CloneClock),
                ("clock_json_bytes", Operation::ClockBytes),
            ][..],
        ),
        (
            "journal_record_read",
            &[("open_and_read", Operation::ReadRecords)][..],
        ),
    ] {
        let mut group = c.benchmark_group(name);
        group.throughput(Throughput::Elements(if name == "journal_record_read" {
            2
        } else {
            1
        }));
        for (dimensions, fixture) in &fixtures {
            for (label, op) in operations {
                let case = format!("{label}/{}", dimensions.name());
                let mut taken = false;
                group.bench_function(&case, |b| {
                    measure(
                        b,
                        censuses,
                        &mut taken,
                        &format!("{name}/{case}"),
                        &dimensions.json(),
                        || operation(fixture, *op, runtime),
                    )
                });
            }
        }
        group.finish();
    }
}
