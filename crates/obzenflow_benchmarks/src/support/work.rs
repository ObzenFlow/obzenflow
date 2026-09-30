// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Exclusive allocation census owned entirely by the benchmark process.

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
        ACTIVE.store(true, Ordering::SeqCst);
        Self {
            _exclusive: exclusive,
        }
    }

    /// All contributing tasks must already have completed or been joined.
    pub(super) fn finish(self) {
        stop();
    }
}

fn stop() {
    ACTIVE.store(false, Ordering::SeqCst);
}

impl Drop for WorkScope {
    fn drop(&mut self) {
        stop();
    }
}
