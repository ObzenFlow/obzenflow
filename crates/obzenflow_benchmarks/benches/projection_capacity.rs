// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Finite arrival windows through ordinary readers and real consumer projections.
//! Diagnostic census and unprobed timing runs share the same completion oracles.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use obzenflow_benchmarks::support::{
    self,
    capacity::projection::{operation, Workload},
    measure,
};
use serde_json::json;
use std::time::Duration;

#[global_allocator]
static ALLOCATOR: support::allocations::Allocator = support::allocations::Allocator;

fn bench(c: &mut Criterion) {
    let runtime = support::runtime();
    let base = Workload {
        journals: 8,
        records: 256,
        offered_per_second: 0,
        payload: 256,
        group: 1,
        execution_every: 16,
        studio: 0,
        metrics: false,
        slow_resume: false,
        uneven: false,
    };
    let mut cases = Vec::new();
    for journals in [1, 3, 8, 32, 50, 75, 100] {
        let regime = if journals >= 50 {
            "stress"
        } else if journals == 32 {
            "scaling"
        } else {
            "baseline"
        };
        cases.push((
            format!("burst/{regime}_journals_{journals}"),
            Workload { journals, ..base },
            false,
        ));
    }
    for rate in [100, 1000, 10_000] {
        cases.push((
            format!("live/rate_{rate}"),
            Workload {
                offered_per_second: rate,
                ..base
            },
            false,
        ));
    }
    for (name, studio, metrics, slow_resume, payload, group, uneven) in [
        ("one_studio", 1, false, false, 256, 1, false),
        ("three_studio", 3, false, false, 256, 1, false),
        ("metrics", 0, true, false, 256, 1, false),
        ("studio_and_metrics", 3, true, false, 256, 1, false),
        ("slow_resume", 3, true, true, 256, 1, false),
        ("large_mixed_groups", 3, true, false, 8192, 8, false),
        ("uneven_traffic", 3, true, false, 256, 8, true),
    ] {
        cases.push((
            format!("live/{name}"),
            Workload {
                offered_per_second: 1000,
                studio,
                metrics,
                slow_resume,
                payload,
                group,
                uneven,
                ..base
            },
            false,
        ));
    }
    // Matched workload with probe enabled inside timing estimates diagnostic
    // overhead. It is not silently compared to the ordinary timing reference.
    cases.push((
        "diagnostic_overhead/studio_and_metrics".into(),
        Workload {
            offered_per_second: 1000,
            studio: 3,
            metrics: true,
            ..base
        },
        true,
    ));
    let mut census = Vec::new();
    support::capacity::save_census(&census, false);
    let mut group = c.benchmark_group("journal_projection_capacity");
    for (name, workload, diagnostic_timing) in cases {
        let mut taken = false;
        let mut first = true;
        let mut input = workload.input(diagnostic_timing);
        input["first_census_has_detailed_probe"] = json!(true);
        group.throughput(Throughput::Elements(workload.records as u64));
        group.bench_function(&name, |b| {
            measure(
                b,
                &mut census,
                &mut taken,
                &format!("journal_projection_capacity/{name}"),
                &input,
                || {
                    if first {
                        eprintln!(
                            "capacity case={name} resources={}",
                            support::capacity::process_usage()
                        );
                    }
                    let detailed = first || diagnostic_timing;
                    first = false;
                    operation(&runtime, workload, detailed)
                },
            )
        });
        support::capacity::save_census(&census, false);
    }
    group.finish();
    support::capacity::save_census(&census, true);
}

criterion_group! { name=benches;config=Criterion::default().sample_size(40).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));targets=bench }
criterion_main!(benches);
