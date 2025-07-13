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
    }
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
    
    pub fn create_journal(&self, name: JournalName, owner: JournalOwner) -> Result<Arc<dyn Journal>, JournalError> {
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
        
        Ok(Arc::new(DiskJournal::with_owner(journal_path, owner)?))
    }
}

/// Simple memory journal factory
pub struct MemoryJournalFactory {
    flow_id: FlowId,
    // Keep created journals so we can return the same instance for the same name
    journals: HashMap<JournalName, Arc<dyn Journal>>,
}

impl MemoryJournalFactory {
    pub fn new(flow_id: FlowId) -> Self {
        Self {
            flow_id,
            journals: HashMap::new(),
        }
    }
    
    pub fn create_journal(&mut self, name: JournalName, owner: JournalOwner) -> Arc<dyn Journal> {
        self.journals
            .entry(name)
            .or_insert_with(|| Arc::new(MemoryJournal::with_owner(owner)))
            .clone()
    }
}

/// Common factory trait
pub trait JournalFactory {
    fn create_journal(&mut self, name: JournalName, owner: JournalOwner) -> Result<Arc<dyn Journal>, JournalError>;
}

impl JournalFactory for DiskJournalFactory {
    fn create_journal(&mut self, name: JournalName, owner: JournalOwner) -> Result<Arc<dyn Journal>, JournalError> {
        self.create_journal(name, owner)
    }
}

impl JournalFactory for MemoryJournalFactory {
    fn create_journal(&mut self, name: JournalName, owner: JournalOwner) -> Result<Arc<dyn Journal>, JournalError> {
        Ok(self.create_journal(name, owner))
    }
}

/// Simple factory functions for the DSL
pub fn disk_journals(base_path: PathBuf) -> impl Fn(FlowId) -> Result<DiskJournalFactory, JournalError> {
    move |flow_id| DiskJournalFactory::new(base_path.clone(), flow_id)
}

pub fn memory_journals() -> impl Fn(FlowId) -> MemoryJournalFactory {
    move |flow_id| MemoryJournalFactory::new(flow_id)
}