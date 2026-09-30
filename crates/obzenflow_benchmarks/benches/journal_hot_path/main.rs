// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Component baselines for ordinary journal operations.
mod append;
mod control;
mod dispatch;
mod record;

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

use obzenflow_benchmarks::support;
pub(crate) use support::{journal as fixtures, measure, timed, Census, Meter, Sample};

#[global_allocator]
static ALLOCATOR: support::allocations::Allocator = support::allocations::Allocator;

fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut censuses = Vec::new();
    record::bench(c, &runtime, &mut censuses);
    dispatch::bench(c, &runtime, &mut censuses);
    append::bench(c, &runtime, &mut censuses);
    if let Ok(path) = std::env::var("OBZENFLOW_WORK_CENSUS") {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let report = serde_json::json!({
            "measurement_contract": support::MEASUREMENT_CONTRACT,
            "compiled_manifest_dir": env!("CARGO_MANIFEST_DIR"),
            "cases": censuses,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20)
        .warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
