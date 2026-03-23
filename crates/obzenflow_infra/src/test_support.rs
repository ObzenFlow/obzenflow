// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::ffi::OsString;
use std::sync::{Mutex, MutexGuard, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(crate) struct EnvGuard {
    saved: Vec<(String, Option<OsString>)>,
}

impl EnvGuard {
    pub(crate) fn new(names: &[&str]) -> Self {
        let mut saved = Vec::with_capacity(names.len());
        for name in names {
            saved.push(((*name).to_string(), std::env::var_os(name)));
        }
        Self { saved }
    }

    pub(crate) fn set(&self, name: &str, value: &str) {
        std::env::set_var(name, value);
    }

    pub(crate) fn set_os(&self, name: &str, value: OsString) {
        std::env::set_var(name, value);
    }

    pub(crate) fn remove(&self, name: &str) {
        std::env::remove_var(name);
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.saved.drain(..) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("poisoned")
}
