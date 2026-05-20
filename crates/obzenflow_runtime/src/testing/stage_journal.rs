// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Error type for [`crate::testing::FlowTestHarness::stage_journal_for_test`]
//! (FLOWIP-114h).

use thiserror::Error;

/// Result of looking up a stage data journal by name through
/// [`crate::testing::FlowTestHarness::stage_journal_for_test`].
#[derive(Debug, Error)]
pub enum StageJournalLookupError {
    /// No stage in the topology has the given name.
    #[error("unknown stage name `{0}` in topology")]
    UnknownStage(String),

    /// More than one stage in the topology has the given name. The handle
    /// rejects ambiguous lookups rather than returning an arbitrary match.
    #[error("ambiguous stage name `{0}`: multiple stages share this name")]
    AmbiguousStage(String),

    /// The stage exists in the topology but the pipeline did not register a
    /// data journal for it (for example, a sink that journals only error
    /// envelopes).
    #[error("stage `{0}` has no registered data journal")]
    MissingStageJournal(String),

    /// The flow handle was constructed without a topology. Without a topology,
    /// stage-name lookups cannot succeed.
    #[error("flow handle has no topology; cannot resolve stage names")]
    MissingTopology,

    /// Duplicate stage-journal entries for the same stage id were supplied when
    /// building a harness.
    #[error("duplicate stage journal for stage id `{0}`")]
    DuplicateStageJournal(obzenflow_core::StageId),
}
