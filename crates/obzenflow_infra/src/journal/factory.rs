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
use obzenflow_runtime::journal::{
    CurrentRunLocator, FlowJournalFactory, RunResourcePlan, RunSubstrateState,
};
use obzenflow_runtime::replay::ReplayArchive;
use obzenflow_runtime::runtime_resource_limits::{
    env_try_raise_nofile, estimate_disk_journal_fds, preflight_nofile_for_disk_journals,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::disk::DiskJournal;
use super::memory::MemoryJournal;

/// Simple disk journal factory that creates files immediately
pub struct DiskJournalFactory {
    base_path: PathBuf,
    flow_id: FlowId,
    // Cache journals so all consumers share the same instance (and shared locks)
    chain_journals: HashMap<JournalName, Arc<dyn Journal<ChainEvent>>>,
    system_journals: HashMap<JournalName, Arc<dyn Journal<SystemEvent>>>,
    /// Flow-global admission sequencer (FLOWIP-120n F18): one per run, shared
    /// by every data/error journal; system journals skip it (system rows are
    /// never merge inputs). Seeded above the archive maximum on replay/resume.
    admission_sequencer: Arc<AtomicU64>,
}

impl DiskJournalFactory {
    pub fn new(base_path: PathBuf, flow_id: FlowId) -> Result<Self, JournalError> {
        // Create the flow directory NOW
        let flow_path = base_path.join("flows").join(flow_id.to_string());
        std::fs::create_dir_all(&flow_path).map_err(|e| JournalError::Implementation {
            message: format!("Failed to create flow directory: {}", flow_path.display()),
            source: Box::new(e),
        })?;

        Ok(Self {
            base_path,
            flow_id,
            chain_journals: HashMap::new(),
            system_journals: HashMap::new(),
            admission_sequencer: Arc::new(AtomicU64::new(0)),
        })
    }

    /// This flow run's directory (`<base>/flows/<flow_id>/`).
    fn run_path(&self) -> PathBuf {
        self.base_path.join("flows").join(self.flow_id.to_string())
    }

    pub fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        if let Some(journal) = self.chain_journals.get(&name) {
            return Ok(journal.clone());
        }
        let journal_path = self.run_path().join(name.to_filename());

        // Create the file NOW if it doesn't exist
        if !journal_path.exists() {
            std::fs::File::create(&journal_path).map_err(|e| JournalError::Implementation {
                message: format!("Failed to create journal file: {}", journal_path.display()),
                source: Box::new(e),
            })?;
        }

        let journal = Arc::new(
            DiskJournal::<ChainEvent>::with_owner(journal_path, owner)?
                .with_admission_sequencer(self.admission_sequencer.clone()),
        );
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
        let journal_path = self.run_path().join(name.to_filename());

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

    /// Write `run_manifest.json` into the run directory (FLOWIP-095a).
    pub fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        let path = self.run_path().join(RUN_MANIFEST_FILENAME);
        let body =
            serde_json::to_string_pretty(manifest).map_err(|e| JournalError::Implementation {
                message: format!("Failed to serialize run manifest: {}", path.display()),
                source: Box::new(e),
            })?;
        std::fs::write(&path, body).map_err(|e| JournalError::Implementation {
            message: format!("Failed to write run manifest: {}", path.display()),
            source: Box::new(e),
        })?;

        Ok(())
    }
}

/// Simple memory journal factory
pub struct MemoryJournalFactory {
    // Keep created journals so we can return the same instance for the same name
    chain_journals: HashMap<JournalName, Arc<dyn Journal<ChainEvent>>>,
    system_journals: HashMap<JournalName, Arc<dyn Journal<SystemEvent>>>,
    /// Flow-global admission sequencer (FLOWIP-120n F18); see `DiskJournalFactory`.
    admission_sequencer: Arc<AtomicU64>,
}

impl MemoryJournalFactory {
    pub fn new(_flow_id: FlowId) -> Self {
        Self {
            chain_journals: HashMap::new(),
            system_journals: HashMap::new(),
            admission_sequencer: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        let sequencer = self.admission_sequencer.clone();
        Ok(self
            .chain_journals
            .entry(name)
            .or_insert_with(|| {
                Arc::new(
                    MemoryJournal::<ChainEvent>::with_owner(owner)
                        .with_admission_sequencer(sequencer),
                )
            })
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
}

/// Seed the flow sequencer above the archive's recorded maximum (FLOWIP-120n
/// F18) so this run's stamps order after every re-admitted sequence. Runs at
/// flow build, before any stage appends.
fn seed_admission_sequencer(sequencer: &AtomicU64, archive: &dyn ReplayArchive) {
    let seed = archive.max_recorded_admission_seq().0 + 1;
    sequencer.store(seed, Ordering::SeqCst);
    tracing::debug!(
        seed,
        "seeded flow admission sequencer above the archive maximum (FLOWIP-120n F18)"
    );
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

impl FlowJournalFactory for DiskJournalFactory {
    fn run_state(&self) -> RunSubstrateState {
        RunSubstrateState::Durable(CurrentRunLocator::new(self.run_path()))
    }

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

    /// Runtime resource preflight guardrails (FLOWIP-086n; trigger moved here
    /// by FLOWIP-120u). Disk-backed journals scale file descriptors with
    /// topology size; fail fast before any journal file exists.
    fn resource_preflight(&self, plan: &RunResourcePlan) -> Result<(), JournalError> {
        let estimate =
            estimate_disk_journal_fds(plan.stage_count, plan.edge_count, plan.metrics_enabled);

        match preflight_nofile_for_disk_journals(estimate, env_try_raise_nofile()) {
            Ok(Some(limit)) => {
                tracing::info!(
                    target: "flowip-086n",
                    stages = estimate.stages,
                    edges = estimate.edges,
                    metrics_enabled = estimate.metrics_enabled,
                    estimated_fds = estimate.estimated_fds,
                    rlimit_soft = limit.soft,
                    rlimit_hard = limit.hard,
                    breakdown_writer_fds = estimate.breakdown.writer_fds,
                    breakdown_stage_reader_fds = estimate.breakdown.stage_reader_fds,
                    breakdown_metrics_reader_fds = estimate.breakdown.metrics_reader_fds,
                    breakdown_system_reader_fds = estimate.breakdown.system_reader_fds,
                    breakdown_overhead_fds = estimate.breakdown.overhead_fds,
                    "Disk journal FD preflight"
                );

                // Warn when we're close to the current soft limit so operators can tune
                // before hitting a hard failure at startup.
                let warn_threshold = limit.soft.saturating_mul(70) / 100;
                if estimate.estimated_fds >= warn_threshold {
                    tracing::warn!(
                        target: "flowip-086n",
                        estimated_fds = estimate.estimated_fds,
                        rlimit_soft = limit.soft,
                        warn_threshold = warn_threshold,
                        "Disk journal pipeline is near the current RLIMIT_NOFILE soft limit"
                    );
                }
                Ok(())
            }
            Ok(None) => {
                // Platform does not expose RLIMIT_NOFILE; skip preflight.
                Ok(())
            }
            Err(message) => Err(JournalError::Implementation {
                message,
                source: Box::new(std::io::Error::other("RLIMIT_NOFILE preflight refused")),
            }),
        }
    }

    fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        DiskJournalFactory::write_run_manifest(self, manifest)
    }

    fn seed_admission_from_archive(&self, archive: &dyn ReplayArchive) {
        seed_admission_sequencer(&self.admission_sequencer, archive);
    }
}

impl FlowJournalFactory for MemoryJournalFactory {
    fn run_state(&self) -> RunSubstrateState {
        RunSubstrateState::Ephemeral
    }

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

    // resource_preflight and write_run_manifest inherit the accepting defaults:
    // an ephemeral run has no location and no provider-specific resource shape.

    fn seed_admission_from_archive(&self, archive: &dyn ReplayArchive) {
        seed_admission_sequencer(&self.admission_sequencer, archive);
    }
}
