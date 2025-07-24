//! Sane journal factory that actually creates files when needed
//!
//! No more two-level closures. Just a simple factory that works.

use obzenflow_core::{
    FlowId,
    journal::{
        journal::Journal,
        journal_name::JournalName,
        journal_owner::JournalOwner,
        journal_error::JournalError,
    },
    event::{JournalEvent, ChainEvent, SystemEvent},
};
use std::sync::Arc;
use std::path::PathBuf;
use std::collections::HashMap;

use super::disk::disk_journal::DiskJournal;
use super::memory::memory_journal::MemoryJournal;

/// Simple disk journal factory that creates files immediately
pub struct DiskJournalFactory {
    base_path: PathBuf,
    flow_id: FlowId,
}

impl DiskJournalFactory {
    pub fn new(base_path: PathBuf, flow_id: FlowId) -> Result<Self, JournalError> {
        // Create the flow directory NOW
        let flow_path = base_path.join("flows").join(flow_id.to_string());
        std::fs::create_dir_all(&flow_path)
            .map_err(|e| JournalError::Implementation {
                message: format!("Failed to create flow directory: {}", flow_path.display()),
                source: Box::new(e),
            })?;
        
        Ok(Self { base_path, flow_id })
    }
    
    pub fn create_chain_journal(&self, name: JournalName, owner: JournalOwner) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        let flow_path = self.base_path.join("flows").join(self.flow_id.to_string());
        let journal_path = flow_path.join(name.to_filename());
        
        // Create the file NOW if it doesn't exist
        if !journal_path.exists() {
            std::fs::File::create(&journal_path)
                .map_err(|e| JournalError::Implementation {
                    message: format!("Failed to create journal file: {}", journal_path.display()),
                    source: Box::new(e),
                })?;
        }
        
        Ok(Arc::new(DiskJournal::<ChainEvent>::with_owner(journal_path, owner)?))
    }
    
    pub fn create_system_journal(&self, name: JournalName, owner: JournalOwner) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        let flow_path = self.base_path.join("flows").join(self.flow_id.to_string());
        let journal_path = flow_path.join(name.to_filename());
        
        // Create the file NOW if it doesn't exist
        if !journal_path.exists() {
            std::fs::File::create(&journal_path)
                .map_err(|e| JournalError::Implementation {
                    message: format!("Failed to create journal file: {}", journal_path.display()),
                    source: Box::new(e),
                })?;
        }
        
        Ok(Arc::new(DiskJournal::<SystemEvent>::with_owner(journal_path, owner)?))
    }
}

/// Simple memory journal factory
pub struct MemoryJournalFactory {
    flow_id: FlowId,
    // Keep created journals so we can return the same instance for the same name
    chain_journals: HashMap<JournalName, Arc<dyn Journal<ChainEvent>>>,
    system_journals: HashMap<JournalName, Arc<dyn Journal<SystemEvent>>>,
}

impl MemoryJournalFactory {
    pub fn new(flow_id: FlowId) -> Self {
        Self {
            flow_id,
            chain_journals: HashMap::new(),
            system_journals: HashMap::new(),
        }
    }
    
    pub fn create_chain_journal(&mut self, name: JournalName, owner: JournalOwner) -> Arc<dyn Journal<ChainEvent>> {
        self.chain_journals
            .entry(name)
            .or_insert_with(|| Arc::new(MemoryJournal::<ChainEvent>::with_owner(owner)))
            .clone()
    }
    
    pub fn create_system_journal(&mut self, name: JournalName, owner: JournalOwner) -> Arc<dyn Journal<SystemEvent>> {
        self.system_journals
            .entry(name)
            .or_insert_with(|| Arc::new(MemoryJournal::<SystemEvent>::with_owner(owner)))
            .clone()
    }
}


/// Simple factory functions for the DSL
pub fn disk_journals(base_path: PathBuf) -> impl Fn(FlowId) -> Result<DiskJournalFactory, JournalError> {
    move |flow_id| DiskJournalFactory::new(base_path.clone(), flow_id)
}

pub fn memory_journals() -> impl Fn(FlowId) -> MemoryJournalFactory {
    move |flow_id| MemoryJournalFactory::new(flow_id)
}