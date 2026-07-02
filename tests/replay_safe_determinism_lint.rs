// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fs;
use std::path::{Path, PathBuf};

const SURFACE_MARKERS: &[&str] = &[
    "impl EffectfulTransformHandler",
    "impl EffectfulStatefulHandler",
    "effectful_transform!",
    "effectful_stateful!",
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

// FLOWIP-120v: sinks are delivery-only. Direct external I/O in a sink body
// belongs behind the effect boundary (an effectful transform authoring named
// outcome facts, consumed by a plain sink) or in a declared idempotent
// projection. File-granular like the ambient table above, and kept separate
// from it: ambient tokens are not applied to sink files, sink tokens are not
// applied to effectful files. Escape with `// allow-sink-io: <reason>`.
const SINK_MARKERS: &[&str] = &["impl SinkHandler", "impl Delivery", "sink!"];

const SINK_FORBIDDEN: &[(&str, &str)] = &[
    ("reqwest::", "an effectful transform plus a plain sink"),
    ("hyper::", "an effectful transform plus a plain sink"),
    ("ureq::", "an effectful transform plus a plain sink"),
    ("sqlx::", "an effectful transform plus a plain sink"),
    (
        "tokio_postgres::",
        "an effectful transform plus a plain sink",
    ),
    ("redis::", "an effectful transform plus a plain sink"),
    ("tokio::net", "an effectful transform plus a plain sink"),
    ("std::net::", "an effectful transform plus a plain sink"),
    (
        "std::fs::write",
        "a declared idempotent projection (`delivery: idempotent`) or the effect boundary",
    ),
];

#[test]
fn sink_bodies_do_not_perform_direct_external_io() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&root.join("src"), &mut files);
    collect_rust_files(&root.join("examples"), &mut files);

    let mut failures = Vec::new();
    for file in files {
        let body = fs::read_to_string(&file).expect("rust source should be readable");
        if !SINK_MARKERS.iter().any(|marker| body.contains(marker)) {
            continue;
        }

        for (line_idx, line) in body.lines().enumerate() {
            if line.contains("allow-sink-io:") {
                let reason = line
                    .split_once("allow-sink-io:")
                    .map(|(_, reason)| reason.trim())
                    .unwrap_or_default();
                assert!(
                    !reason.is_empty(),
                    "{}:{} uses allow-sink-io without a reason",
                    file.display(),
                    line_idx + 1
                );
                continue;
            }

            for (token, replacement) in SINK_FORBIDDEN {
                if line.contains(token) {
                    failures.push(format!(
                        "{}:{}: matched `{}` in a sink-bearing file; a non-idempotent \
                         external write belongs behind the effect boundary. Use {} or \
                         add `// allow-sink-io: <reason>`",
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
        "sink delivery-only lint failed:\n{}",
        failures.join("\n")
    );
}

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
