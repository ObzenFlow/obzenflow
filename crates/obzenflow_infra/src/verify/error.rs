// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Errors and refusal reasons for replay output verification (FLOWIP-095j).
//!
//! A [`RefusalReason`] is a verdict: the comparison was declined for a stated
//! reason and the process exits with code 3. A [`VerifyError`] is an
//! operational failure (I/O, corruption) that is neither a verdict nor a
//! refusal.

use std::fmt;
use std::path::PathBuf;

use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VerifyError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse {path}: {message}")]
    Parse { path: PathBuf, message: String },

    #[error("corrupt journal record in {journal} at line {line}: {message}")]
    CorruptRecord {
        journal: String,
        line: u64,
        message: String,
    },

    #[error("failed to write verification report to {path}: {source}")]
    ReportWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Why verification refused to compare (exit code 3). Refusals are part of
/// the product contract: each carries enough context for the operator to act.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum RefusalReason {
    /// A run directory or its manifest is missing.
    ArchiveUnavailable { path: PathBuf },

    /// The manifest was written under a different (older or newer) schema
    /// version. Pre-release there is no migration path: re-record the run.
    ManifestVersion {
        path: PathBuf,
        found: String,
        supported: String,
    },

    /// The two runs are not runs of the same flow.
    FlowNameMismatch { baseline: String, candidate: String },

    /// The stage sets differ; the supported diff workflow is same-topology,
    /// changed-handler.
    StageSetMismatch {
        added: Vec<String>,
        removed: Vec<String>,
    },

    /// The recorded inbound edges differ for a stage, so the topologies are
    /// not the same shape even though the stage names match.
    InboundMismatch { stage: String },

    /// The candidate ran in `ResumeIncomplete` mode and legitimately extends
    /// history past the baseline; whole-run equality is the wrong instrument.
    ResumeIncompleteCandidate,

    /// A run did not reach a comparable terminal status
    /// (`Completed | Cancelled`, the same gate replay itself applies).
    StatusGate { run: PathBuf, status: String },

    /// Effect-lane namespaces differ, so the runs do not share a replay
    /// lineage and identity comparison is invalid.
    LineageMismatch {
        stage: String,
        baseline_namespace: String,
        candidate_namespace: String,
    },
}

impl fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArchiveUnavailable { path } => {
                write!(f, "run directory unavailable: {}", path.display())
            }
            Self::ManifestVersion {
                path,
                found,
                supported,
            } => write!(
                f,
                "run manifest version '{found}' at {} is unsupported (supported: {supported}); re-record the run with this build of ObzenFlow",
                path.display()
            ),
            Self::FlowNameMismatch {
                baseline,
                candidate,
            } => write!(
                f,
                "runs belong to different flows: baseline '{baseline}', candidate '{candidate}'"
            ),
            Self::StageSetMismatch { added, removed } => write!(
                f,
                "stage sets differ (added in candidate: [{}], removed from candidate: [{}]); verification compares same-topology runs only",
                added.join(", "),
                removed.join(", ")
            ),
            Self::InboundMismatch { stage } => write!(
                f,
                "stage '{stage}' has different inbound edges in the two runs; verification compares same-topology runs only"
            ),
            Self::ResumeIncompleteCandidate => write!(
                f,
                "candidate ran with --allow-incomplete-archive (resume mode) and legitimately extends history past the baseline; whole-run verification does not apply"
            ),
            Self::StatusGate { run, status } => write!(
                f,
                "run at {} has status '{status}'; verification requires a completed or cancelled run",
                run.display()
            ),
            Self::LineageMismatch {
                stage,
                baseline_namespace,
                candidate_namespace,
            } => write!(
                f,
                "runs do not share a replay lineage (stage '{stage}': baseline effect namespace '{baseline_namespace}', candidate '{candidate_namespace}')"
            ),
        }
    }
}
