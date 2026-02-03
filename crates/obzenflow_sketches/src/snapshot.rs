// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Snapshot schema helpers.
//!
//! The 087 series uses "versioned snapshots" to make emitted payloads stable
//! and evolvable. Prefer explicit `enum` snapshot types like:
//!
//! ```ignore
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! pub enum HyperLogLogSnapshot {
//!     V1 { precision: u8, registers: Vec<u8> }
//! }
//! ```

use serde::{Deserialize, Serialize};

/// Version for a snapshot schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaVersion(pub u32);

/// Explicit seed used for sketch hashing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HashSeed(pub u64);
