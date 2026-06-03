// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fs;
use std::path::{Path, PathBuf};

const SURFACE_MARKERS: &[&str] = &[
    "impl EffectfulTransformHandler",
    "impl EffectfulSinkHandler",
    "impl EffectfulStatefulHandler",
    "effectful_transform!",
    "effectful_stateful!",
    "effectful_sink!",
];

const FORBIDDEN: &[(&str, &str)] = &[
    ("SystemTime::now", "EffectContext::now or Effects::capture"),
    ("Utc::now", "EffectContext::now or Effects::capture"),
    ("Local::now", "EffectContext::now or Effects::capture"),
    ("Instant::now", "EffectContext::now or Effects::capture"),
    ("Uuid::new_v4", "EffectContext::deterministic_id"),
    ("Ulid::new", "EffectContext::deterministic_id"),
    ("rand::thread_rng", "EffectContext::rng"),
    ("rand::rng", "EffectContext::rng"),
    ("fastrand::", "EffectContext::rng"),
    ("std::env::var", "Effects::capture"),
    ("std::env::var_os", "Effects::capture"),
    ("std::env::current_dir", "Effects::capture"),
    ("UNIX_EPOCH", "EffectContext::now or Effects::capture"),
];

#[test]
fn replay_safe_user_surfaces_do_not_use_ambient_nondeterminism() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&root.join("src"), &mut files);
    collect_rust_files(&root.join("examples"), &mut files);

    let mut failures = Vec::new();
    for file in files {
        let body = fs::read_to_string(&file).expect("rust source should be readable");
        if !SURFACE_MARKERS.iter().any(|marker| body.contains(marker)) {
            continue;
        }

        for (line_idx, line) in body.lines().enumerate() {
            if line.contains("allow-replay-ambient:") {
                let reason = line
                    .split_once("allow-replay-ambient:")
                    .map(|(_, reason)| reason.trim())
                    .unwrap_or_default();
                assert!(
                    !reason.is_empty(),
                    "{}:{} uses allow-replay-ambient without a reason",
                    file.display(),
                    line_idx + 1
                );
                continue;
            }

            for (token, replacement) in FORBIDDEN {
                if line.contains(token) {
                    failures.push(format!(
                        "{}:{}: matched `{}`; use {} or add `// allow-replay-ambient: <reason>`",
                        file.display(),
                        line_idx + 1,
                        token,
                        replacement
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "replay-safe deterministic-context lint failed:\n{}",
        failures.join("\n")
    );
}

fn collect_rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }

    for entry in fs::read_dir(dir).expect("source directory should be readable") {
        let path = entry.expect("source entry should be readable").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}
