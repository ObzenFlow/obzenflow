// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Report persistence (FLOWIP-095j). The verifier never writes into either
//! run's journals: a verification is a fact about two runs, owned by neither
//! run's single writer. The sidecar report is the receipt, written under a
//! `verification/` subdirectory inside the candidate run directory, keyed by
//! the baseline's flow id so verifications against multiple baselines
//! coexist.

use std::path::{Path, PathBuf};

use super::error::VerifyError;
use super::report::VerificationReport;

pub fn default_report_path(candidate_run_dir: &Path, baseline_flow_id: &str) -> PathBuf {
    candidate_run_dir
        .join("verification")
        .join(format!("{baseline_flow_id}.json"))
}

pub fn write_report(path: &Path, report: &VerificationReport) -> Result<(), VerifyError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| VerifyError::ReportWrite {
            path: path.to_path_buf(),
            source,
        })?;
    }
    let body = serde_json::to_string_pretty(report).map_err(|e| VerifyError::ReportWrite {
        path: path.to_path_buf(),
        source: std::io::Error::other(e.to_string()),
    })?;
    std::fs::write(path, body).map_err(|source| VerifyError::ReportWrite {
        path: path.to_path_buf(),
        source,
    })
}
