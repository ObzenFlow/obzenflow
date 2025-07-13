//! Journal factory functions for easy configuration
//!
//! These helper functions create journal factories that respect the onion architecture
//! by allowing the outer layer (examples/binaries) to specify concrete journal types
//! that can be passed through to inner layers as trait objects.

use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::id::flow_id::FlowId;
use std::sync::Arc;
use std::path::PathBuf;

use super::memory::memory_journal::MemoryJournal;
use super::disk::disk_journal::DiskJournal;

/// Creates a factory for memory-based journals
///
/// This is the simplest option - all journals are stored in memory.
/// Perfect for testing and development.
///
/// # Example
/// ```rust
/// use obzenflow_infra::journal::memory_journals;
///
/// let flow = flow! {
///     journals: memory_journals(),
///     // ...
/// }.await?;
/// ```
pub fn memory_journals() -> impl Fn(FlowId) -> Box<dyn Fn(&str, &JournalOwner) -> Arc<dyn Journal>> {
    move |_flow_id| {
        Box::new(move |_name, owner| {
            Arc::new(MemoryJournal::with_owner(owner.clone()))
        })
    }
}

/// Creates a factory for disk-based journals
///
/// Each flow gets its own directory under `base_path/flows/{flow_id}/`.
/// This prevents conflicts between different flow executions.
///
/// # Example
/// ```rust
/// use obzenflow_infra::journal::disk_journals;
///
/// let flow = flow! {
///     journals: disk_journals("/tmp/obzenflow"),
///     // ...
/// }.await?;
/// ```
pub fn disk_journals(base_path: PathBuf) -> impl Fn(FlowId) -> Box<dyn Fn(&str, &JournalOwner) -> Arc<dyn Journal>> {
    move |flow_id| {
        let flow_path = base_path.join("flows").join(flow_id.to_string());
        Box::new(move |name, owner| {
            // Create directory if it doesn't exist
            std::fs::create_dir_all(&flow_path).ok();
            
            let journal_path = flow_path.join(format!("{}.log", name));
            
            // Create journal synchronously
            match DiskJournal::with_owner(journal_path, owner.clone()) {
                Ok(j) => Arc::new(j),
                Err(e) => panic!("Failed to create disk journal: {}", e),
            }
        })
    }
}

/// Creates a factory for hybrid journals (memory with disk backup)
///
/// This provides the speed of memory journals with the durability of disk.
/// Events are kept in memory but also persisted to disk.
///
/// # Example
/// ```rust
/// use obzenflow_infra::journal::hybrid_journals;
///
/// let flow = flow! {
///     journals: hybrid_journals("/tmp/obzenflow"),
///     // ...
/// }.await?;
/// ```
pub fn hybrid_journals(base_path: PathBuf) -> impl Fn(FlowId) -> Box<dyn Fn(&str, &JournalOwner) -> Arc<dyn Journal>> {
    // For now, just use disk journals
    // TODO: Implement actual hybrid journal that keeps recent events in memory
    disk_journals(base_path)
}