// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static WRITES: AtomicU64 = AtomicU64::new(0);

pub fn unsynced_config() -> PathBuf {
    let directory = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let path = directory.join("unsynced-journal.toml");
    let pending = directory.join(format!(
        "unsynced-journal.{}.{}.toml",
        std::process::id(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(directory).expect("target temporary directory is created");
    std::fs::write(&pending, "[journal]\nsync = \"off\"\n").expect("configuration is written");
    std::fs::rename(&pending, &path).expect("configuration is published");
    path
}
