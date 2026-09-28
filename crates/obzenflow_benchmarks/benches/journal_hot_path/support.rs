// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared measurement and work-census harness; no production behaviour.

use obzenflow_core::benchmark::WorkScope;
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[path = "allocations.rs"]
mod allocations;

#[global_allocator]
static ALLOCATOR: allocations::Allocator = allocations::Allocator;

#[derive(serde::Serialize)]
pub struct Census {
    pub(crate) case: String,
    pub(crate) input: Value,
    pub(crate) work: BTreeMap<String, u64>,
    pub(crate) allocations: allocations::Work,
    pub(crate) observations: Value,
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
    pub(crate) elapsed: Duration,
    pub(crate) work: BTreeMap<String, u64>,
    pub(crate) allocations: Option<allocations::Work>,
    pub(crate) observations: Value,
}

impl Sample {
    pub(crate) fn expect_work(&self, name: &str, expected: u64) {
        if self.allocations.is_some() {
            assert_eq!(self.work[name], expected, "{name}");
        }
    }
    pub(crate) fn is_census(&self) -> bool {
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
