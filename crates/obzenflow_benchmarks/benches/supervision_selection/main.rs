// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Unchanged-runtime references for early binary supervision selection.
mod allocations;
mod fixtures;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use fixtures::{Case, History, Pattern};
use obzenflow_core::benchmark::WorkScope;
use obzenflow_core::event::{StageLifecycleEvent, SystemPayload};
use obzenflow_core::{ChainEvent, EventId, FlowId, Journal, JournalId, JournalOwner, StageId};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_infra::testing::journal_bench::clear_definition_cache;
use obzenflow_runtime::supervised_base::report_reader::{ReportRead, ReportReaders};
use std::cell::LazyCell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};

#[global_allocator]
static ALLOCATOR: allocations::Allocator = allocations::Allocator;
const DEADLINE: Duration = Duration::from_secs(30);

#[derive(serde::Serialize)]
struct Census {
    case: String,
    input: serde_json::Value,
    work: BTreeMap<String, u64>,
    allocations: allocations::Work,
    report_retained_high_water_sum: usize,
}

enum Observed {
    Report(JournalId, u64, EventId, bool),
    Coverage(JournalId, u64),
}

fn validate(history: &History, observed: &[Observed]) {
    let mut next: BTreeMap<_, usize> = history.reports.keys().map(|id| (*id, 0)).collect();
    let mut covered = BTreeMap::new();
    for event in observed {
        match event {
            Observed::Report(journal, position, id, metadata_matches) => {
                let index = next.get_mut(journal).expect("unexpected report journal");
                let expected = history.reports[journal].get(*index).expect("extra report");
                assert_eq!(
                    (*position, *id),
                    (expected.position(), *expected.id()),
                    "report identity/order"
                );
                assert!(
                    *metadata_matches,
                    "report payload or inherited provenance changed"
                );
                *index += 1;
            }
            Observed::Coverage(journal, through) => {
                assert!(*through <= history.case.rows() as u64);
                assert!(*through >= covered.get(journal).copied().unwrap_or(0));
                // Coverage cannot overtake even one expected undelivered report.
                assert!(history.reports[journal]
                    .get(next[journal])
                    .is_none_or(|r| r.position() > *through));
                covered.insert(*journal, *through);
            }
        }
    }
    for (journal, reports) in &history.reports {
        assert_eq!(next[journal], reports.len());
        assert_eq!(covered.get(journal), Some(&(history.case.rows() as u64)));
    }
}

fn discover(runtime: &Runtime, history: &History) -> (Duration, Census) {
    if history.case.cold {
        clear_definition_cache(&history.paths[0]);
    }
    // Consumer observation storage is reserved before measurement. Report bodies
    // are checked and dropped immediately, not retained as an artificial backlog.
    let mut observed =
        Vec::with_capacity(history.case.rows() * history.case.readers * 2 + history.case.readers);
    let mut coverage: BTreeMap<_, u64> = history.journals.iter().map(|j| (*j.id(), 0)).collect();
    let mut next: BTreeMap<_, usize> = history.journals.iter().map(|j| (*j.id(), 0)).collect();
    let memory = allocations::Start::new();
    let scope = WorkScope::start();
    let (duration, diagnostics) = runtime.block_on(async {
        let mut readers = ReportReaders::default();
        let duration = tokio::time::timeout(DEADLINE, async {
            let start = Instant::now();
            for journal in &history.journals { readers.stage(journal.clone()); }
            loop {
                match poll_fn(|cx| readers.poll_next(cx)).await.unwrap() {
                    ReportRead::Record(row) => {
                        let journal = row.journal_id();
                        let index = next.get_mut(&journal).expect("unexpected journal");
                        let expected = history.reports[&journal].get(*index);
                        let matches = expected.is_some_and(|e| {
                            row.journal().vector_clock == e.journal().vector_clock
                                && row.journal().causal == e.journal().causal
                                && matches!((&row.payload, &e.payload),
                                    (SystemPayload::StageLifecycle { stage_id: a, event: StageLifecycleEvent::Running },
                                     SystemPayload::StageLifecycle { stage_id: b, event: StageLifecycleEvent::Running }) if a == b)
                        });
                        observed.push(Observed::Report(journal, row.position(), *row.id(), matches));
                        *index += 1;
                    }
                    ReportRead::Coverage { journal, through } => {
                        *coverage.get_mut(&journal).expect("unexpected coverage") = through;
                        observed.push(Observed::Coverage(journal, through));
                    }
                }
                if readers.initial_prefix_complete() && coverage.values().all(|p| *p == history.case.rows() as u64) { break; }
            }
            start.elapsed()
        }).await.expect("supervision discovery exceeded deadline; partial work is not a sample");
        let diagnostics = readers.diagnostics();
        readers.shutdown_for_benchmark().await;
        (duration, diagnostics)
    });
    let work = scope.finish();
    let allocations = memory.finish();
    validate(history, &observed);
    assert_eq!(
        diagnostics.iter().map(|d| d.scanned_records).sum::<u64>(),
        (history.case.rows() * history.case.readers) as u64
    );
    assert_eq!(
        diagnostics.iter().map(|d| d.selected_records).sum::<u64>(),
        history.reports.values().map(Vec::len).sum::<usize>() as u64
    );
    // Framing remains a real obligation in both the full and selective paths.
    assert_eq!(work["primary_frame_reads"], history.frames as u64);
    assert_eq!(work["primary_frame_bytes"], history.encoded_bytes);
    assert_eq!(
        work["verified_frames"],
        history.frames as u64 + work["definition_carrier_reads"]
    );
    assert_eq!(
        work["verified_frame_bytes"],
        history.encoded_bytes + work["definition_carrier_bytes"]
    );
    if std::env::var("OBZENFLOW_EXPECT_SELECTIVE_READS").as_deref() == Ok("1") {
        for name in [
            "business_payload_decodes",
            "business_records_constructed",
            "business_record_accounting_serializations",
        ] {
            assert_eq!(
                work[name], 0,
                "selective-reader acceptance: {} still performs {name}",
                history.case.name
            );
        }
    }
    (
        duration,
        Census {
            case: format!("supervision_discovery/{}", history.case.name),
            input: history.input_description(),
            work,
            allocations,
            report_retained_high_water_sum: diagnostics
                .iter()
                .map(|d| d.high_water_record_bytes)
                .sum(),
        },
    )
}

fn full_reader(runtime: &Runtime, history: &History) -> (Duration, Census) {
    let mut ids = Vec::with_capacity(history.case.rows());
    let memory = allocations::Start::new();
    let scope = WorkScope::start();
    let elapsed = runtime.block_on(async {
        tokio::time::timeout(DEADLINE, async {
            let start = Instant::now();
            let mut reader = history.journals[0].reader().await.unwrap();
            while let Some(row) = reader.next().await.unwrap() {
                ids.push(*row.id());
            }
            start.elapsed()
        })
        .await
        .expect("full-reader control exceeded deadline")
    });
    let work = scope.finish();
    let allocations = memory.finish();
    let expected: Vec<_> = history.rows[history.journals[0].id()]
        .iter()
        .map(|r| *r.id())
        .collect();
    assert_eq!(ids, expected);
    // Positive control: zero acceptance counts cannot pass because instrumentation
    // has accidentally been disabled or removed from the underlying operations.
    assert_eq!(work["business_payload_decodes"], history.business as u64);
    assert_eq!(
        work["business_records_constructed"],
        history.business as u64
    );
    assert_eq!(work["primary_frame_bytes"], history.encoded_bytes);
    (
        elapsed,
        Census {
            case: "journal_read_controls/full_record_scan".into(),
            input: history.input_description(),
            work,
            allocations,
            report_retained_high_water_sum: 0,
        },
    )
}

fn bench(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut censuses = Vec::new();
    let mut discovery = c.benchmark_group("supervision_discovery");
    for case in fixtures::cases() {
        discovery.throughput(Throughput::Elements(
            (case.rows() * case.readers).max(1) as u64
        ));
        let fixture = LazyCell::new(|| runtime.block_on(History::build(case.clone())));
        let mut census_taken = false;
        discovery.bench_function(&case.name, |b| {
            let history = &*fixture;
            if !census_taken {
                if !case.cold {
                    // Writer-populated definitions have not necessarily had their
                    // carrier stamps checked by a reader. Prime that state before
                    // both the saved census and Criterion's measurement sequence.
                    let _ = discover(&runtime, history);
                }
                censuses.push(discover(&runtime, history).1);
                census_taken = true;
            }
            b.iter_custom(|iterations| {
                (0..iterations).map(|_| discover(&runtime, history).0).sum()
            });
        });
    }
    discovery.finish();

    let fixture = LazyCell::new(|| {
        runtime.block_on(History::build(Case::new("control", Pattern::Prefix(64))))
    });
    let mut full = c.benchmark_group("journal_read_controls");
    full.throughput(Throughput::Elements(65));
    let mut census_taken = false;
    full.bench_function("full_record_scan", |b| {
        let history = &*fixture;
        if !census_taken {
            let _ = full_reader(&runtime, history);
            censuses.push(full_reader(&runtime, history).1);
            census_taken = true;
        }
        b.iter_custom(|iterations| {
            (0..iterations)
                .map(|_| full_reader(&runtime, history).0)
                .sum()
        });
    });
    full.throughput(Throughput::Elements(1));
    let mut census_taken = false;
    full.bench_function("business_record_serialization", |b| {
        let history = &*fixture;
        let row = &history.rows[history.journals[0].id()][0];
        let operation = || {
            let memory = allocations::Start::new();
            let scope = WorkScope::start();
            let start = Instant::now();
            let encoded = serde_json::to_vec(std::hint::black_box(row)).unwrap();
            std::hint::black_box(&encoded);
            let elapsed = start.elapsed();
            let work = scope.finish();
            let allocations = memory.finish();
            assert_eq!(work["record_accounting_serializations"], 1);
            assert_eq!(work["business_record_accounting_serializations"], 1);
            (
                elapsed,
                Census {
                    case: "journal_read_controls/business_record_serialization".into(),
                    input: serde_json::json!({"records":1,"encoded_json_bytes":encoded.len()}),
                    work,
                    allocations,
                    report_retained_high_water_sum: 0,
                },
            )
        };
        if !census_taken {
            censuses.push(operation().1);
            census_taken = true;
        }
        b.iter_custom(|iterations| (0..iterations).map(|_| operation().0).sum());
    });
    full.finish();

    let fixture = LazyCell::new(|| {
        let history = runtime.block_on(History::build(Case::new("dependency", Pattern::Prefix(1))));
        let corpus = history.corpus();
        (history, corpus)
    });
    let mut dependencies = c.benchmark_group("report_definition_resolution");
    dependencies.throughput(Throughput::Elements(1));
    for cold in [false, true] {
        let name = if cold {
            "cold_definitions"
        } else {
            "warm_definitions"
        };
        let mut census_taken = false;
        dependencies.bench_function(name, |b| {
            let (history, corpus) = &*fixture;
            let operation = || {
                let memory = allocations::Start::new();
                let scope = WorkScope::start();
                let start = Instant::now();
                let rows = corpus.decode_frame(1, cold).unwrap();
                let elapsed = start.elapsed();
                let work = scope.finish();
                let allocations = memory.finish();
                assert_eq!(rows.len(), 1);
                let expected = &history.rows[history.journals[0].id()][1];
                assert_eq!(rows[0].id(), expected.id());
                assert_eq!(
                    rows[0].envelope.provenance.journal.vector_clock,
                    expected.envelope.provenance.journal.vector_clock
                );
                assert_eq!(work["business_payload_decodes"], 0);
                if cold {
                    assert!(
                        work["definition_carrier_bytes"] > 0,
                        "fixture did not exercise a skipped carrier"
                    );
                } else {
                    assert_eq!(work["definition_carrier_bytes"], 0);
                }
                (
                    elapsed,
                    Census {
                        case: format!("report_definition_resolution/{name}"),
                        input: serde_json::json!({
                            "fixture": history.input_description(),
                            "decoded_records": 1,
                            "cold_definitions": cold,
                            "primary_frame_preloaded": true,
                        }),
                        work,
                        allocations,
                        report_retained_high_water_sum: 0,
                    },
                )
            };
            if !census_taken {
                censuses.push(operation().1);
                census_taken = true;
            }
            b.iter_custom(|iterations| (0..iterations).map(|_| operation().0).sum());
        });
    }
    dependencies.finish();

    append(c, &runtime, &mut censuses);
    if let Ok(path) = std::env::var("OBZENFLOW_SUPERVISION_WORK_OUTPUT") {
        let path = std::path::PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(path)
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let output = serde_json::json!({"contract":"supervision-selection-v1", "cases":censuses});
        std::fs::write(path, serde_json::to_vec_pretty(&output).unwrap()).unwrap();
    }
}

fn append(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("journal_append_costs");
    group.throughput(Throughput::Elements(64));
    for (name, pattern, size) in [
        ("business_records_64", Pattern::Business(64), 1),
        ("mixed_atomic_group_64", Pattern::Mixed(vec![0, 32, 63]), 64),
        ("report_only_atomic_group_64", Pattern::Reports(64), 64),
    ] {
        let mut census_taken = false;
        group.bench_function(name, |b| {
            let operation = || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("append.log");
                let stage = StageId::new();
                let journal = DiskJournal::<ChainEvent>::with_owner_in_run(path.clone(), JournalOwner::stage(stage), FlowId::new()).unwrap();
                let case = Case::new(name, pattern.clone());
                let events: Vec<_> = fixtures::events(&case, stage).into_iter().map(|(e, _)| e).collect();
                let chunks: Vec<_> = events.chunks(size).enumerate().map(|(i, group)| (format!("group-{i}"), group.to_vec())).collect();
                let mut receipts = Vec::with_capacity(64);
                let memory = allocations::Start::new();
                let scope = WorkScope::start();
                let elapsed = runtime.block_on(async {
                    tokio::time::timeout(DEADLINE, async {
                        let start = Instant::now();
                        for (group, mut events) in chunks {
                            if size == 1 { receipts.push(journal.append(events.pop().unwrap(), Default::default()).await.unwrap()); }
                            else { receipts.extend(journal.append_group(&group, events, Default::default()).await.unwrap()); }
                        }
                        start.elapsed()
                    }).await.expect("append benchmark exceeded deadline")
                });
                let work = scope.finish();
                let allocations = memory.finish();
                assert_eq!(receipts.len(), 64);
                for (i, row) in receipts.iter().enumerate() { assert_eq!(row.local_sequence(), i as u64 + 1); }
                assert_eq!(runtime.block_on(journal.committed_position()).unwrap(), 64);
                let bytes = std::fs::metadata(path).unwrap().len();
                (elapsed, Census { case: format!("journal_append_costs/{name}"), input: serde_json::json!({"records":64,"physical_group_size":size,"encoded_bytes":bytes}), work, allocations, report_retained_high_water_sum: 0 })
            };
            if !census_taken {
                censuses.push(operation().1);
                census_taken = true;
            }
            b.iter_custom(|iterations| (0..iterations).map(|_| operation().0).sum());
        });
    }
    group.finish();
}

criterion_group! {
    name = supervision_selection;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(supervision_selection);
