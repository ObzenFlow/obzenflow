// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Component baselines for the remaining journal and parent hot paths.
#[path = "../supervision_selection/allocations.rs"]
mod allocations;
mod append;
mod dispatch;
mod fan_in;
mod fixtures;
mod record;

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_core::benchmark::WorkScope;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOCATOR: allocations::Allocator = allocations::Allocator;

#[derive(serde::Serialize)]
pub struct Census {
    case: String,
    input: Value,
    work: BTreeMap<String, u64>,
    allocations: allocations::Work,
    observations: Value,
}

pub struct Meter {
    census: Option<(allocations::Start, WorkScope)>,
    start: Instant,
}

impl Meter {
    pub fn start() -> Self {
        let census = (!obzenflow_core::benchmark::active())
            .then(|| (allocations::Start::new(), WorkScope::start()));
        Self {
            census,
            start: Instant::now(),
        }
    }
    pub fn finish(self, elapsed: Duration) -> Sample {
        let (work, allocations) = match self.census {
            Some((memory, scope)) => (scope.finish(), Some(memory.finish())),
            None => (BTreeMap::new(), None),
        };
        Sample {
            elapsed,
            work,
            allocations,
            observations: Value::Null,
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

pub struct Sample {
    elapsed: Duration,
    work: BTreeMap<String, u64>,
    allocations: Option<allocations::Work>,
    observations: Value,
}

impl Sample {
    fn expect_work(&self, name: &str, expected: u64) {
        if self.allocations.is_some() {
            assert_eq!(self.work[name], expected, "{name}");
        }
    }
    fn is_census(&self) -> bool {
        self.allocations.is_some()
    }
}

pub fn timed<T>(operation: impl FnOnce() -> T) -> (T, Sample) {
    let meter = Meter::start();
    let value = std::hint::black_box(operation());
    let elapsed = meter.elapsed();
    (value, meter.finish(elapsed))
}

// Store a single complete-work census per case, alongside Criterion's repeated
// timing samples. Fixture construction, checking and census JSON are untimed.
pub fn measure(
    b: &mut criterion::Bencher<'_>,
    censuses: &mut Vec<Census>,
    taken: &mut bool,
    name: &str,
    input: &Value,
    mut operation: impl FnMut() -> Sample,
) {
    if !*taken {
        let sample = operation();
        censuses.push(Census {
            case: name.to_owned(),
            input: input.clone(),
            work: sample.work,
            allocations: sample.allocations.expect("exclusive per-operation census"),
            observations: sample.observations,
        });
        *taken = true;
    }
    b.iter_custom(|iterations| {
        // Keep identical production instrumentation enabled, but collect/reset
        // counters once per Criterion sample. Per-operation map/JSON allocation
        // would otherwise dominate the *untimed* harness for sub-microsecond work.
        let scope = WorkScope::start();
        let mut elapsed = Duration::ZERO;
        for _ in 0..iterations {
            let sample = operation();
            elapsed += sample.elapsed;
        }
        drop(scope);
        elapsed
    });
}

fn bench(c: &mut Criterion) {
    let runtime = fixtures::runtime();
    let mut censuses = Vec::new();
    record::bench(c, &runtime, &mut censuses);
    dispatch::bench(c, &runtime, &mut censuses);
    fan_in::bench(c, &runtime, &mut censuses);
    append::bench(c, &runtime, &mut censuses);
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
