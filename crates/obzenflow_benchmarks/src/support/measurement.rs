// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared measurement and work-census harness; no production behaviour.

use super::{allocations, work::WorkScope};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(serde::Serialize)]
pub struct Census {
    pub case: String,
    pub input: Value,
    pub work: BTreeMap<String, u64>,
    pub allocations: allocations::Work,
    pub observations: Value,
}

pub struct Meter {
    census: Option<(allocations::Start, WorkScope)>,
    start: Instant,
}

impl Meter {
    pub fn start() -> Self {
        let census = (cfg!(feature = "allocation-census") && !super::work::active())
            .then(|| (allocations::Start::new(), WorkScope::start()));
        Self {
            census,
            start: Instant::now(),
        }
    }
    pub fn finish(self, elapsed: Duration) -> Sample {
        let allocations = match self.census {
            Some((memory, scope)) => {
                scope.finish();
                Some(memory.finish())
            }
            None => None,
        };
        Sample {
            elapsed,
            work: BTreeMap::new(),
            allocations,
            observations: Value::Null,
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Sample {
    pub elapsed: Duration,
    pub work: BTreeMap<String, u64>,
    pub allocations: Option<allocations::Work>,
    pub observations: Value,
}

impl Sample {
    /// Record validated, consumer-visible output, never an internal work estimate.
    pub fn completed(&mut self, name: &str, count: u64) {
        if self.allocations.is_some() {
            self.work.insert(name.to_owned(), count);
        }
    }
    pub fn is_census(&self) -> bool {
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
    if cfg!(feature = "allocation-census") && !*taken {
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
        let mut elapsed = Duration::ZERO;
        for _ in 0..iterations {
            let sample = operation();
            elapsed += sample.elapsed;
        }
        elapsed
    });
}
