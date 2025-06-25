//! Journal implementations
//!
//! This module provides concrete implementations of the core Journal trait

pub mod disk;
pub mod memory;

// Re-export implementations
pub use memory::MemoryJournal;
pub use disk::disk_journal::DiskJournal;