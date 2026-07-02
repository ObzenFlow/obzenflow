// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal factory interface used by the DSL
//!
//! Onion architecture note:
//! - Inner layers define stable traits (`Journal`, `JournalReader`, `ReplayArchive`)
//! - Outer layers (infra) provide concrete implementations and inject them
//! - The DSL (`flow!`) is the composition root that wires factories + journals into runtime services
//!
//! OT-16 formalized the "journal factory" contract with additive, defaulted
//! capability methods. FLOWIP-120u deliberately breaks that stance for one
//! method: `run_state` is required with no default, so a factory cannot compile
//! without declaring its substrate arm.

use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::journal::RunManifest;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::replay::ReplayArchive;

/// Erased substrate fact for consumers outside the build (FLOWIP-120u).
///
/// Durable means located: the locator names the single addressable place
/// holding every artifact the run produces. An ephemeral run has none.
#[derive(Clone, Debug)]
pub enum RunSubstrateState {
    Durable(CurrentRunLocator),
    Ephemeral,
}

impl RunSubstrateState {
    pub fn locator(&self) -> Option<&CurrentRunLocator> {
        match self {
            Self::Durable(locator) => Some(locator),
            Self::Ephemeral => None,
        }
    }
}

/// Opaque locator for the current run's durable archive.
///
/// The `PathBuf` inside is the disk provider's representation, not the concept;
/// presentation renders it via `Display` without interpreting layout.
#[derive(Clone, Debug)]
pub struct CurrentRunLocator(PathBuf);

impl CurrentRunLocator {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl fmt::Display for CurrentRunLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.display().fmt(f)
    }
}

/// Whole-run scale facts the composition root supplies to `resource_preflight`.
pub struct RunResourcePlan {
    pub stage_count: usize,
    pub edge_count: usize,
    pub metrics_enabled: bool,
}

pub trait FlowJournalFactory: Send {
    /// Which run substrate this factory provides.
    ///
    /// Required, no default: a factory cannot compile without answering, and
    /// declaring `Durable` structurally requires producing the locator.
    fn run_state(&self) -> RunSubstrateState;

    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError>;

    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError>;

    /// Provider-owned resource admission, called once at the factory seam with
    /// whole-run scale facts, before any journal is created.
    ///
    /// Advisory guardrail; the default accepts. Only the provider knows its
    /// resource shape (disk estimates file descriptors; a remote backend would
    /// budget connections).
    fn resource_preflight(&self, _plan: &RunResourcePlan) -> Result<(), JournalError> {
        Ok(())
    }

    /// Persist `run_manifest.json` within the run's location.
    ///
    /// Called only on the `Durable` arm; ephemeral runs never receive it.
    /// Durable obligation: a factory declaring `Durable` must persist the
    /// manifest, or its archive is unreadable at replay/resume time.
    fn write_run_manifest(&self, _manifest: &RunManifest) -> Result<(), JournalError> {
        Ok(())
    }

    /// Seed run-output state from the opened replay/resume input archive, once
    /// at build before any journal append (FLOWIP-120n F18 admission sequencing).
    ///
    /// Consume-only: the factory never opens archives (FLOWIP-120u); the host
    /// opens the archive and the composition root passes it here. Default is a
    /// no-op for factories with no sequenced output state.
    fn seed_admission_from_archive(&self, _archive: &dyn ReplayArchive) {}
}
