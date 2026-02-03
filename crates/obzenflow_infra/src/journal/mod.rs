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
pub use disk::disk_journal::DiskJournal;
pub use memory::MemoryJournal;

// Re-export factory
pub use factory::{disk_journals, memory_journals};
pub use factory::{DiskJournalFactory, MemoryJournalFactory};
