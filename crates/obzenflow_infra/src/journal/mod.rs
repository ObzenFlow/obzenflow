//! Journal implementations
//!
//! This module provides concrete implementations of the core Journal trait

pub mod disk;
pub mod memory;
pub mod factories;

// Re-export implementations
pub use memory::MemoryJournal;
pub use disk::disk_journal::DiskJournal;

// Re-export factory functions
pub use factories::{memory_journals, disk_journals, hybrid_journals};