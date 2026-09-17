// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core journal abstractions
//!
//! Pure domain types and traits for event journaling.
//! No infrastructure concerns or I/O operations here!

pub mod append;
pub mod archive;
pub mod config;
pub mod factory;
pub mod journal_error;
pub mod journal_name;
pub mod journal_owner;
pub mod journal_trait;
pub mod reader;

pub use append::{AppendOptions, JournalCapture, ObservationCapture};
pub use archive::{
    ArchiveStatus, RunManifest, RunManifestReplayConfig, RunManifestStage, StatusDerivation,
    JOURNAL_FORMAT_VERSION, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION,
};
pub use config::{JournalConfig, ObservabilityPolicy};
pub use journal_error::JournalError;
pub use journal_trait::Journal;
pub use reader::{
    JournalObservationReader, JournalReader, LocatedObservation, ObservationKey, ObservationLookup,
};

// Type aliases for clarity
use crate::event::{ChainEvent, SystemEvent};

/// Journal that accepts ChainEvent (used by stages for data events)
pub type StageJournal = dyn Journal<ChainEvent>;

/// Journal that accepts SystemEvent (used for system orchestration)
pub type SystemJournal = dyn Journal<SystemEvent>;
