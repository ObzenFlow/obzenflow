// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-145h component baselines. No flow builder or business execution.

mod causal;
mod decode;
mod fixtures;

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;

criterion_group! {
    name = components;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1));
    targets = causal::bench, decode::bench
}
criterion_main!(components);
