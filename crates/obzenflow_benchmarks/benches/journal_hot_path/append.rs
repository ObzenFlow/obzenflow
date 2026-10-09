// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{declare, fixtures, measure, timed, Category, Census, Sample};
use criterion::{Criterion, Throughput};
use obzenflow_core::{ChainEvent, Journal, JournalOwner};
use obzenflow_infra::journal::DiskJournal;
use std::cell::LazyCell;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn run(runtime: &Runtime, history: &fixtures::History) -> Sample {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("append.log");
    let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner_in_run(
            path.clone(),
            JournalOwner::stage(history.stage),
            history.run,
        )
        .unwrap(),
    );
    let events = history.events.clone();
    let (rows, mut sample) = timed(|| {
        runtime.block_on(async {
            tokio::time::timeout(
                fixtures::DEADLINE,
                fixtures::append_events(&journal, events, history.group),
            )
            .await
            .expect("complete append watchdog")
        })
    });
    assert_eq!(
        rows.iter().map(|r| *r.id()).collect::<Vec<_>>(),
        history.rows.iter().map(|r| *r.id()).collect::<Vec<_>>()
    );
    // Validate committed results through an ordinary reader. Byte-format and
    // atomicity proofs remain private tests at the codec/storage owner.
    runtime.block_on(async {
        tokio::time::timeout(fixtures::DEADLINE, async {
            assert_eq!(
                journal.committed_position().await.unwrap(),
                rows.len() as u64
            );
            let mut reader = journal.reader().await.unwrap();
            for (index, expected) in rows.iter().enumerate() {
                let actual = reader
                    .next()
                    .await
                    .unwrap()
                    .expect("committed append readable");
                assert_eq!(actual.local_sequence(), index as u64 + 1);
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(reader.position(), rows.len() as u64);
        })
        .await
        .expect("append readback watchdog");
    });
    sample.completed("committed_records", rows.len() as u64);
    sample.observations =
        serde_json::json!({"journal_file_bytes":std::fs::metadata(path).unwrap().len()});
    sample
}

pub fn bench(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("journal_append_cost");
    group.throughput(Throughput::Elements(64));
    for (name, payload, group_size, every) in [
        ("ordinary_business_64", 256, 1, 0),
        ("ordinary_business_group_64", 256, 64, 0),
        ("large_business_64", 8192, 1, 0),
        ("mixed_group_64", 256, 64, 8),
        ("execution_fact_group_64", 256, 64, 1),
    ] {
        let history = LazyCell::new(|| {
            runtime.block_on(fixtures::History::build(64, payload, group_size, every))
        });
        let case = format!("complete_append/{name}");
        let full = format!("journal_append_cost/{case}");
        declare(
            &full,
            Category::Append,
            "Append/group-append 64 records; destination setup and readback excluded",
        );
        let mut taken = false;
        let input = serde_json::json!({"records":64,"business_payload_bytes":payload,"group_size":group_size,"execution_fact_every":every});
        group.bench_function(&case, |b| {
            measure(b, censuses, &mut taken, &full, &input, || {
                run(runtime, &history)
            })
        });
    }
    group.finish();
}
