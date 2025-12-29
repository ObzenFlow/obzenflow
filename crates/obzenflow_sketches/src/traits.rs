//! Shared traits for sketch semantics.

use serde::de::DeserializeOwned;

/// Merge semantics for fan-in and rollups.
///
/// Implementations should be deterministic and side-effect free (beyond mutation
/// of `self`).
pub trait Merge {
    fn merge(&mut self, other: &Self);
}

/// Snapshot semantics for journaling and replay.
///
/// The snapshot is expected to be:
/// - deterministic (encoding does not depend on map iteration order, pointer identity, etc.)
/// - stable for long-term storage (avoid `usize`, prefer explicit versions)
pub trait Snapshot {
    type Output: Clone + std::fmt::Debug + serde::Serialize + DeserializeOwned;

    fn snapshot(&self) -> Self::Output;
}
