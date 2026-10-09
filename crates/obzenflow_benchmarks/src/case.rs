// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Case declarations collected while the native lane lists benchmark cases.
//! Std and serde only: the reference build compiles this module too.

use std::io::Write;

/// Names the file the native lane collects declarations from while listing.
pub const DECLARATIONS: &str = "OBZENFLOW_CASE_DECLARATIONS";

/// Operation category that groups a case in performance reports.
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    Read,
    Append,
    ReadWrite,
    Causal,
    Observe,
    Runtime,
    Flow,
    Archive,
}

#[derive(serde::Serialize)]
struct Declaration<'a> {
    case: &'a str,
    category: Category,
    timed: &'a str,
}

/// Declares a case's full Criterion ID, category and timed boundary next to
/// its registration. A no-op unless the native lane is listing cases.
pub fn declare(case: &str, category: Category, timed: &str) {
    let Some(path) = std::env::var_os(DECLARATIONS) else {
        return;
    };
    let line = serde_json::to_string(&Declaration {
        case,
        category,
        timed,
    })
    .expect("serialisable case declaration");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("case declarations are writable");
    writeln!(file, "{line}").expect("case declarations are writable");
}
