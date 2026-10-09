// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Measure selected operations directly, keeping full application correctness
//! in integration tests. Every iteration validates completed work after timing.

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_benchmarks::support::{
    runtime,
    validation::{Archive, ObserverJournals},
};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use serde_json::{json, Value};
use std::cell::LazyCell;
use std::time::{Duration, Instant};

fn case(
    c: &mut Criterion,
    census: &mut Vec<Value>,
    name: &str,
    (category, timed): (Category, &str),
    input: Value,
    mut operation: impl FnMut() -> (Duration, Value),
) {
    declare(name, category, timed);
    let mut captured = false;
    c.bench_function(name, |b| {
        b.iter_custom(|iterations| {
            let mut total = Duration::ZERO;
            for _ in 0..iterations {
                let (elapsed, work) = operation();
                total += elapsed;
                if !captured {
                    census.push(json!({"case":name,"input":input,"work":work}));
                    captured = true;
                }
            }
            total
        });
    });
}
fn sorted(rows: &[Value]) -> Vec<String> {
    let mut rows: Vec<_> = rows
        .iter()
        .map(|row| serde_json::to_string(row).unwrap())
        .collect();
    rows.sort();
    rows
}

fn bench(c: &mut Criterion) {
    let runtime = runtime();
    let archive = LazyCell::new(|| Archive::build(&runtime, 1000, false));
    let replay = LazyCell::new(|| Archive::build(&runtime, 10_000, true));
    let observers = LazyCell::new(|| runtime.block_on(ObserverJournals::build(64)));
    let studio = LazyCell::new(|| {
        runtime.block_on(obzenflow_benchmarks::support::studio::ProjectionFixture::read(&observers))
    });
    let mut census = Vec::new();
    for operation in ["export", "admit_and_read"] {
        let name = format!("archive_validation/{operation}/inputs_1000");
        case(
            c,
            &mut census,
            &name,
            if operation == "export" {
                (
                    Category::Archive,
                    "JSONL export of an existing 1,000-input archive",
                )
            } else {
                (
                    Category::Archive,
                    "Open, admit and fully read a 1,000-input archive",
                )
            },
            json!({"inputs":1000,"payload_bytes":256,"metrics":false,"storage":"disk","filesystem_cache":"warm"}),
            || {
                let archive = &*archive;
                let output = archive.directory.path().join("export.jsonl");
                let expected = sorted(&archive.records);
                let started = Instant::now();
                let (elapsed, records) = match operation {
                    "export" => {
                        obzenflow_infra::journal::disk::inspect::export_jsonl(
                            &archive.baseline,
                            Some(&output),
                        )
                        .unwrap();
                        let elapsed = started.elapsed();
                        let rows: Vec<Value> = std::fs::read_to_string(&output)
                            .unwrap()
                            .lines()
                            .map(|line| serde_json::from_str(line).unwrap())
                            .collect();
                        assert_eq!(
                            sorted(&rows),
                            expected,
                            "export preserves every admitted record exactly"
                        );
                        (elapsed, rows.len())
                    }
                    "admit_and_read" => {
                        let rows = runtime.block_on(Archive::read(&archive.baseline));
                        let elapsed = started.elapsed();
                        let rows: Vec<Value> = rows
                            .into_iter()
                            .map(|row| serde_json::to_value(row.record).unwrap())
                            .collect();
                        assert_eq!(
                            sorted(&rows),
                            expected,
                            "cold admission/read preserves every record exactly"
                        );
                        (elapsed, rows.len())
                    }
                    _ => unreachable!(),
                };
                (
                    elapsed,
                    json!({"complete_records":records,"inputs":archive.inputs}),
                )
            },
        );
    }
    case(
        c,
        &mut census,
        "replay_validation/streaming_comparison/inputs_10000",
        (
            Category::Archive,
            "Compare existing live/replay 10,000-input archives; execution and report writing excluded",
        ),
        json!({"inputs":10_000,"payload_bytes":256,"stages":2,"write_report":false,"storage":"disk","filesystem_cache":"warm"}),
        || {
            let archive = &*replay;
            let options = VerifyOptions {
                write_report: false,
                ..Default::default()
            };
            let started = Instant::now();
            let result = verify_run_dirs(
                &archive.baseline,
                archive.replay.as_ref().unwrap(),
                &options,
            )
            .unwrap();
            let elapsed = started.elapsed();
            assert_eq!(
                result.exit_code(),
                0,
                "{}",
                obzenflow_infra::verify::render_verdict(&result)
            );
            let VerifyOutcome::Completed { report, .. } = result else {
                panic!("incomplete comparison");
            };
            assert_eq!(
                report.stages["ticks"].positional_rows_baseline,
                archive.inputs
            );
            assert_eq!(
                report.stages["ticks"].positional_rows_candidate,
                archive.inputs
            );
            (
                elapsed,
                json!({"baseline_source_rows":archive.inputs,"candidate_source_rows":archive.inputs,"stages":report.stages.len()}),
            )
        },
    );
    case(
        c,
        &mut census,
        "metrics_validation/tail_refresh/data_and_error_64",
        (
            Category::Observe,
            "One stage metrics snapshot from 66 data and one error record",
        ),
        json!({"data_records":66,"error_records":1,"stage_count":1,"readers":{"data":1,"error":1},"storage":"disk"}),
        || {
            let fixture = &*observers;
            let started = Instant::now();
            let snapshot = runtime
                .block_on(
                    obzenflow_runtime::metrics::tail_read::read_stage_metrics_from_tail(
                        &fixture.data,
                        Some(&fixture.error),
                        fixture.stage,
                    ),
                )
                .expect("metrics snapshot");
            let elapsed = started.elapsed();
            assert_eq!(snapshot.events_processed_total, fixture.inputs as u64);
            assert_eq!(snapshot.events_emitted_total, fixture.inputs as u64);
            assert_eq!(snapshot.errors_total, 4);
            (
                elapsed,
                json!({"snapshots":1,"processed":snapshot.events_processed_total,"errors":snapshot.errors_total}),
            )
        },
    );
    case(
        c,
        &mut census,
        "studio_validation/project_and_snapshot/inputs_64",
        (
            Category::Observe,
            "Project 69 preloaded records to frames, measurements and snapshots; journal reads excluded",
        ),
        json!({"inputs":64,"payload_bytes":256,"data_records":66,"error_records":1,"system_records":2,"stages":1,"preloaded_records":true}),
        || studio.measure(),
    );
    if let Ok(path) = std::env::var("OBZENFLOW_WORK_CENSUS") {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let report = serde_json::json!({
            "measurement_contract": obzenflow_benchmarks::support::MEASUREMENT_CONTRACT,
            "compiled_manifest_dir": env!("CARGO_MANIFEST_DIR"),
            "cases": census,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
