//! Core journal abstractions
//!
//! Pure domain types and traits for event journaling.
//! No infrastructure concerns or I/O operations here!

pub mod journal;
pub mod journal_error;
pub mod journal_name;
pub mod journal_owner;
pub mod journal_reader;
pub mod archive;
pub mod run_manifest;

// Re-export commonly used types
pub use journal::Journal;
pub use journal::JournalStorageKind;
pub use journal_error::JournalError;
pub use journal_reader::JournalReader;
pub use archive::{ArchiveStatus, StatusDerivation};
pub use run_manifest::{
    RunManifest, RunManifestReplayConfig, RunManifestStage, RUN_MANIFEST_FILENAME,
    RUN_MANIFEST_VERSION,
};

// Type aliases for clarity
use crate::event::{ChainEvent, SystemEvent};

/// Journal that accepts ChainEvent (used by stages for data events)
pub type StageJournal = dyn Journal<ChainEvent>;

/// Journal that accepts SystemEvent (used for system orchestration)
pub type SystemJournal = dyn Journal<SystemEvent>;
