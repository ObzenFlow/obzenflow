// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replayable identity for one fact admitted through a composite input port.

use crate::id::CompositeId;
use crate::EventId;
use serde::{Deserialize, Serialize};

/// The entry side of an exact composite boundary-duration observation.
///
/// This is integration metadata, not a second domain occurrence. Every field
/// is reconstructable from the durable topology binding and admitted entry
/// fact. Derived events carry it so an output boundary fact can be paired with
/// the exact entry fact under fan-out and fan-in.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompositeActivationContext {
    pub composite_id: CompositeId,
    pub activation: EventId,
    pub entry_port: String,
    /// Persisted `processing_info.event_time` of the entry fact, in Unix ms.
    pub entered_at_ms: u64,
}

impl CompositeActivationContext {
    pub fn new(
        composite_id: CompositeId,
        activation: EventId,
        entry_port: impl Into<String>,
        entered_at_ms: u64,
    ) -> Self {
        Self {
            composite_id,
            activation,
            entry_port: entry_port.into(),
            entered_at_ms,
        }
    }
}
