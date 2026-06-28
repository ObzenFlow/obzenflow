// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal implementations
//!
//! This module provides concrete implementations of the core Journal trait

pub mod disk;
pub mod factory;
pub mod memory;

// Re-export implementations
pub use disk::DiskJournal;
pub use memory::MemoryJournal;

// Re-export factory
pub use factory::{disk_journals, memory_journals};
pub use factory::{DiskJournalFactory, MemoryJournalFactory};

use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;

/// Shared unowned-journal write guard. A journal with no owner rejects appends
/// with one identical error across both backends.
pub(crate) fn ensure_owned(owner: Option<&JournalOwner>) -> Result<(), JournalError> {
    if owner.is_none() {
        return Err(JournalError::Implementation {
            message: "Cannot write to an unowned journal. Journal must have an owner.".to_string(),
            source: "Unowned journal write attempt".into(),
        });
    }
    Ok(())
}
