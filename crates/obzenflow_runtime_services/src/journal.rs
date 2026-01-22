//! Journal factory interface used by the DSL (FLOWIP-095a / OT-16).
//!
//! Onion architecture note:
//! - Inner layers define stable traits (`Journal`, `JournalReader`, `ReplayArchive`)
//! - Outer layers (infra) provide concrete implementations and inject them
//! - The DSL (`flow!`) is the composition root that wires factories + journals into runtime services
//!
//! OT-16 formalizes the "journal factory" contract so adding new capabilities
//! (like `run_manifest.json` writing and replay injection) is additive-only and
//! custom factories get clear trait-bound compiler errors.

use async_trait::async_trait;
use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::journal::RunManifest;
use std::sync::Arc;

use crate::replay::{ReplayArchive, ReplayError};

#[async_trait]
pub trait FlowJournalFactory: Send {
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

    /// Write `run_manifest.json` into the run directory when the backend supports it.
    ///
    /// Default is a no-op so non-disk backends (e.g., memory) can ignore it.
    fn write_run_manifest(&self, _manifest: &RunManifest) -> Result<(), JournalError> {
        Ok(())
    }

    /// Build a replay archive implementation from environment variables, when supported.
    ///
    /// Default is `Ok(None)` so most backends don't need to implement replay for P0.
    async fn replay_archive_from_env(
        &mut self,
    ) -> Result<Option<Arc<dyn ReplayArchive>>, ReplayError> {
        Ok(None)
    }
}
