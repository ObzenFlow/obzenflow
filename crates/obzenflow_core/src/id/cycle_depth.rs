// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-event cycle depth counter (FLOWIP-051p)

use serde::{Deserialize, Serialize};
use std::fmt;

/// Per-event cycle depth counter. Tracks how many complete round trips
/// an event has made within its current SCC.
///
/// Incremented at the SCC entry point on each round trip.
/// Saturates at `u16::MAX` rather than wrapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CycleDepth(u16);

impl CycleDepth {
    /// Create a cycle depth from a raw value.
    pub fn new(depth: u16) -> Self {
        Self(depth)
    }

    /// The depth assigned to an event entering an SCC for the first time.
    pub fn first() -> Self {
        Self(1)
    }

    /// Increment the depth by one, saturating at `u16::MAX`.
    pub fn increment(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Return the underlying `u16` value.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}

impl fmt::Display for CycleDepth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
