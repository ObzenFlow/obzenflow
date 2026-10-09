// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-run median event latency through fixed-depth pipelines. Each sample runs
//! a fresh flow to `Drained`; timeout, failure or any missing or duplicate input
//! fails the sample instead of reporting a value.

mod flows;
mod harness;

use criterion::{criterion_group, criterion_main, Criterion};
use harness::{Delivery, Workload};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_infra::journal::{disk_journals, memory_journals};
use std::time::Duration;
use tokio::runtime::Runtime;

/// One preserved case: Criterion group, depth (source plus transforms),
/// journal backend, workload and sampling.
struct Case {
    group: &'static str,
    depth: usize,
    memory: bool,
    workload: Workload,
    sample_size: usize,
    measurement: Duration,
}

const fn case(
    group: &'static str,
    depth: usize,
    memory: bool,
    (inputs, warm_up, deadline_secs): (u64, u64, u64),
    (sample_size, measurement_secs): (usize, u64),
) -> Case {
    Case {
        group,
        depth,
        memory,
        workload: Workload {
            inputs,
            warm_up,
            deadline: Duration::from_secs(deadline_secs),
        },
        sample_size,
        measurement: Duration::from_secs(measurement_secs),
    }
}

const CASES: [Case; 8] = [
    case("1_stage_latency", 1, false, (110, 10, 30), (20, 30)),
    case("2_stage_latency", 2, false, (110, 10, 30), (20, 30)),
    case("3_stage_latency", 3, false, (110, 10, 30), (20, 30)),
    case("4_stage_latency", 4, false, (110, 10, 30), (20, 30)),
    case("5_stage_latency", 5, false, (110, 10, 60), (10, 45)),
    case("20_stage_latency", 20, false, (110, 10, 90), (10, 60)),
    case("100_stage_latency", 100, false, (22, 2, 180), (10, 30)),
    case(
        "100_stage_latency_memory",
        100,
        true,
        (110, 10, 300),
        (10, 180),
    ),
];

async fn median_latency(case: &Case) -> anyhow::Result<Duration> {
    let directory = tempfile::tempdir()?;
    let delivery = Delivery::new(case.workload);
    let handle = if case.memory {
        flows::build(
            memory_journals(),
            case.depth,
            delivery.source(),
            delivery.sink(),
        )
        .await?
    } else {
        flows::build(
            disk_journals(directory.path().to_owned()),
            case.depth,
            delivery.source(),
            delivery.sink(),
        )
        .await?
    };
    delivery.median_latency(handle).await
}

fn bench(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    for case in &CASES {
        let transforms = case.depth - 1;
        declare(
            &format!("{}/median_latency", case.group),
            Category::Flow,
            &format!(
                "Per-run median latency of {} post-warm-up inputs through {transforms} {} on {} journals",
                case.workload.inputs - case.workload.warm_up,
                if transforms == 1 { "transform" } else { "transforms" },
                if case.memory { "memory" } else { "disk" }
            ),
        );
        let rt = Runtime::new().unwrap();
        let mut group = c.benchmark_group(case.group);
        group.sample_size(case.sample_size);
        group.measurement_time(case.measurement);
        group.bench_function("median_latency", |b| {
            b.to_async(&rt).iter_custom(|iterations| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iterations {
                    total = total.saturating_add(median_latency(case).await.unwrap());
                }
                total
            });
        });
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
