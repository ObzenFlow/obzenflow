//! Journal implementations
//!
//! This module provides concrete implementations of the core Journal trait

pub mod disk;
pub mod memory;
pub mod factory;

// Re-export implementations
pub use memory::MemoryJournal;
pub use disk::disk_journal::DiskJournal;

// Re-export factory
pub use factory::{DiskJournalFactory, MemoryJournalFactory, JournalFactory};
pub use factory::{disk_journals, memory_journals};