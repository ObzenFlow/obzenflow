// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-report delivery and fixed-report-count business-traffic sensitivity.
// Shared fixtures retain their other cases for the original hot-path executable.
#[allow(dead_code)]
#[path = "../journal_hot_path/fan_in.rs"]
mod fan_in;
#[allow(dead_code)]
#[path = "../journal_hot_path/fixtures.rs"]
mod fixtures;
#[allow(dead_code)]
#[path = "../journal_hot_path/support.rs"]
mod support;

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;
pub(crate) use support::{measure, Census, Meter, Sample};

fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut censuses = Vec::new();
    fan_in::bench_delivery(c, &runtime, &mut censuses);
    if let Ok(path) = std::env::var("OBZENFLOW_WORK_CENSUS") {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(&censuses).unwrap()).unwrap();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20)
        .warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
