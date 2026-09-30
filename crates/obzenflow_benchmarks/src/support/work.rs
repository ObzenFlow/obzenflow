// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One measurement session spans the component probes and allocation meter.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

static ACTIVE: AtomicBool = AtomicBool::new(false);
static EXCLUSIVE: Mutex<()> = Mutex::new(());

pub(super) fn active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

pub(super) struct WorkScope {
    _exclusive: MutexGuard<'static, ()>,
}

impl WorkScope {
    pub(super) fn start() -> Self {
        let exclusive = EXCLUSIVE
            .try_lock()
            .expect("overlapping benchmark work scopes");
        obzenflow_core::benchmark::reset();
        obzenflow_infra::benchmark::reset();
        obzenflow_core::benchmark::set_enabled(true);
        obzenflow_infra::benchmark::set_enabled(true);
        ACTIVE.store(true, Ordering::SeqCst);
        Self {
            _exclusive: exclusive,
        }
    }

    /// All contributing tasks must already have completed or been joined.
    pub(super) fn finish(self) -> BTreeMap<String, u64> {
        stop();
        let mut counts = BTreeMap::new();
        for (name, count) in
            obzenflow_core::benchmark::snapshot().chain(obzenflow_infra::benchmark::snapshot())
        {
            assert!(
                counts.insert(name.to_owned(), count).is_none(),
                "duplicate probe name: {name}"
            );
        }
        counts
    }
}

fn stop() {
    ACTIVE.store(false, Ordering::SeqCst);
    obzenflow_core::benchmark::set_enabled(false);
    obzenflow_infra::benchmark::set_enabled(false);
}

impl Drop for WorkScope {
    fn drop(&mut self) {
        stop();
    }
}
