//! Deterministic streaming sketches (probabilistic analytics) for ObzenFlow.
//!
//! This crate is intentionally runtime-agnostic: it contains pure data structures,
//! deterministic hashing utilities, and small traits to make sketches mergeable
//! and snapshot-friendly for journaling/replay.

pub mod hash;
pub mod snapshot;
pub mod traits;

pub use hash::stable_hash64;
pub use snapshot::{HashSeed, SchemaVersion};
pub use traits::{Merge, Snapshot};
