// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disk-based journal implementation

pub mod config;
pub mod inspect;
pub mod journal;
pub mod log_record;
pub mod reader;
pub mod replay_archive;
pub(crate) mod scanner;

pub use journal::DiskJournal;
