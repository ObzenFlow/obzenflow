// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sane journal factory that actually creates files when needed
//!
//! No more two-level closures. Just a simple factory that works.

use obzenflow_core::journal::run_manifest::{RunManifest, RUN_MANIFEST_FILENAME};
use obzenflow_core::{
    event::{ChainEvent, SystemEvent},
    journal::{
        journal_error::JournalError, journal_name::JournalName, journal_owner::JournalOwner,
        Journal,
    },
    FlowId,
};
use obzenflow_runtime::journal::FlowJournalFactory;
use obzenflow_runtime::replay::{ReplayArchive, ReplayError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use super::disk::disk_journal::DiskJournal;
use super::disk::replay_archive::DiskReplayArchive;
use super::memory::memory_journal::MemoryJournal;

static LAST_RUN_DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

fn set_last_run_dir(path: PathBuf) {
    let cell = LAST_RUN_DIR.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    *guard = Some(path);
}

pub(crate) fn take_last_run_dir() -> Option<PathBuf> {
    let cell = LAST_RUN_DIR.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    guard.take()
}

/// Simple disk journal factory that creates files immediately
pub struct DiskJournalFactory {
    base_path: PathBuf,
    flow_id: FlowId,
    // Cache journals so all consumers share the same instance (and shared locks)
    chain_journals: HashMap<JournalName, Arc<dyn Journal<ChainEvent>>>,
    system_journals: HashMap<JournalName, Arc<dyn Journal<SystemEvent>>>,
}

impl DiskJournalFactory {
    pub fn new(base_path: PathBuf, flow_id: FlowId) -> Result<Self, JournalError> {
        // Create the flow directory NOW
        let flow_path = base_path.join("flows").join(flow_id.to_string());
        std::fs::create_dir_all(&flow_path).map_err(|e| JournalError::Implementation {
            message: format!("Failed to create flow directory: {}", flow_path.display()),
            source: Box::new(e),
        })?;

        // Stash the run directory so FlowApplication can print a replay hint on completion (OT-17).
        set_last_run_dir(flow_path.clone());

        Ok(Self {
            base_path,
            flow_id,
            chain_journals: HashMap::new(),
            system_journals: HashMap::new(),
        })
    }

    pub fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        if let Some(journal) = self.chain_journals.get(&name) {
            return Ok(journal.clone());
        }
        let flow_path = self.base_path.join("flows").join(self.flow_id.to_string());
        let journal_path = flow_path.join(name.to_filename());

        // Create the file NOW if it doesn't exist
        if !journal_path.exists() {
            std::fs::File::create(&journal_path).map_err(|e| JournalError::Implementation {
                message: format!("Failed to create journal file: {}", journal_path.display()),
                source: Box::new(e),
            })?;
        }

        let journal = Arc::new(DiskJournal::<ChainEvent>::with_owner(journal_path, owner)?);
        self.chain_journals.insert(name, journal.clone());
        Ok(journal)
    }

    pub fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        if let Some(journal) = self.system_journals.get(&name) {
            return Ok(journal.clone());
        }
        let flow_path = self.base_path.join("flows").join(self.flow_id.to_string());
        let journal_path = flow_path.join(name.to_filename());

        // Create the file NOW if it doesn't exist
        if !journal_path.exists() {
            std::fs::File::create(&journal_path).map_err(|e| JournalError::Implementation {
                message: format!("Failed to create journal file: {}", journal_path.display()),
                source: Box::new(e),
            })?;
        }

        let journal = Arc::new(DiskJournal::<SystemEvent>::with_owner(journal_path, owner)?);
        self.system_journals.insert(name, journal.clone());
        Ok(journal)
    }

    /// Path to this flow run directory (e.g., `<base>/flows/<flow_id>/`).
    pub fn run_dir(&self) -> Option<PathBuf> {
        Some(self.base_path.join("flows").join(self.flow_id.to_string()))
    }

    /// Write `run_manifest.json` into the run directory (FLOWIP-095a).
    pub fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        let Some(run_dir) = self.run_dir() else {
            return Ok(());
        };
        let path = run_dir.join(RUN_MANIFEST_FILENAME);
        let body =
            serde_json::to_string_pretty(manifest).map_err(|e| JournalError::Implementation {
                message: format!("Failed to serialize run manifest: {}", path.display()),
                source: Box::new(e),
            })?;
        std::fs::write(&path, body).map_err(|e| JournalError::Implementation {
            message: format!("Failed to write run manifest: {}", path.display()),
            source: Box::new(e),
        })?;

        // Also refresh the run directory hint here in case the factory was reused across runs.
        set_last_run_dir(run_dir);

        Ok(())
    }

    /// Build a replay archive implementation from the runtime bootstrap context (FLOWIP-095a).
    pub async fn replay_archive(&self) -> Result<Option<Arc<dyn ReplayArchive>>, ReplayError> {
        replay_archive().await
    }
}

/// Simple memory journal factory
pub struct MemoryJournalFactory {
    // Keep created journals so we can return the same instance for the same name
    chain_journals: HashMap<JournalName, Arc<dyn Journal<ChainEvent>>>,
    system_journals: HashMap<JournalName, Arc<dyn Journal<SystemEvent>>>,
}

impl MemoryJournalFactory {
    pub fn new(_flow_id: FlowId) -> Self {
        Self {
            chain_journals: HashMap::new(),
            system_journals: HashMap::new(),
        }
    }

    pub fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        Ok(self
            .chain_journals
            .entry(name)
            .or_insert_with(|| Arc::new(MemoryJournal::<ChainEvent>::with_owner(owner)))
            .clone())
    }

    pub fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        Ok(self
            .system_journals
            .entry(name)
            .or_insert_with(|| Arc::new(MemoryJournal::<SystemEvent>::with_owner(owner)))
            .clone())
    }

    /// Memory journals do not have a run directory.
    pub fn run_dir(&self) -> Option<PathBuf> {
        None
    }

    /// Memory journals do not write manifests; this is a no-op.
    pub fn write_run_manifest(&self, _manifest: &RunManifest) -> Result<(), JournalError> {
        Ok(())
    }

    /// Build a replay archive implementation from the runtime bootstrap context (FLOWIP-095a).
    pub async fn replay_archive(&self) -> Result<Option<Arc<dyn ReplayArchive>>, ReplayError> {
        replay_archive().await
    }
}

async fn replay_archive() -> Result<Option<Arc<dyn ReplayArchive>>, ReplayError> {
    let Some(replay) = obzenflow_runtime::bootstrap::replay_bootstrap() else {
        return Ok(None);
    };

    let archive =
        DiskReplayArchive::open(replay.archive_path, replay.allow_incomplete_archive).await?;
    Ok(Some(Arc::new(archive)))
}

/// Simple factory functions for the DSL
pub fn disk_journals(
    base_path: PathBuf,
) -> impl Fn(FlowId) -> Result<DiskJournalFactory, JournalError> {
    move |flow_id| DiskJournalFactory::new(base_path.clone(), flow_id)
}

pub fn memory_journals() -> impl Fn(FlowId) -> Result<MemoryJournalFactory, JournalError> {
    move |flow_id| Ok(MemoryJournalFactory::new(flow_id))
}

#[async_trait::async_trait]
impl FlowJournalFactory for DiskJournalFactory {
    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        DiskJournalFactory::create_chain_journal(self, name, owner)
    }

    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        DiskJournalFactory::create_system_journal(self, name, owner)
    }

    fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        DiskJournalFactory::write_run_manifest(self, manifest)
    }

    async fn replay_archive(&mut self) -> Result<Option<Arc<dyn ReplayArchive>>, ReplayError> {
        DiskJournalFactory::replay_archive(self).await
    }
}

#[async_trait::async_trait]
impl FlowJournalFactory for MemoryJournalFactory {
    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        MemoryJournalFactory::create_chain_journal(self, name, owner)
    }

    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        MemoryJournalFactory::create_system_journal(self, name, owner)
    }

    fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        MemoryJournalFactory::write_run_manifest(self, manifest)
    }

    async fn replay_archive(&mut self) -> Result<Option<Arc<dyn ReplayArchive>>, ReplayError> {
        MemoryJournalFactory::replay_archive(self).await
    }
}
