// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configured ceiling for cycle depth (FLOWIP-051p)

use std::fmt;

/// Maximum number of round trips allowed for a single event within an SCC.
///
/// This is a config-only type that never appears on the wire, so it does
/// not implement `Serialize`/`Deserialize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxIterations(u16);

impl MaxIterations {
    /// The default maximum iteration count used when no override is configured.
    pub const DEFAULT: Self = Self(30);

    /// Create a new `MaxIterations` from a `u16`.
    ///
    /// If `value` is 0, returns [`Self::DEFAULT`] instead.
    pub fn new(value: u16) -> Self {
        if value == 0 {
            Self::DEFAULT
        } else {
            Self(value)
        }
    }

    /// Try to create from a `usize`, clamping values above `u16::MAX`.
    ///
    /// Returns `None` for 0 (callers should fall back to [`Self::DEFAULT`]).
    pub fn try_from_usize(value: usize) -> Option<Self> {
        if value == 0 {
            None
        } else {
            Some(Self(value.min(u16::MAX as usize) as u16))
        }
    }

    /// Return the underlying `u16` value.
    pub fn as_u16(self) -> u16 {
        self.0
    }
}

impl fmt::Display for MaxIterations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
