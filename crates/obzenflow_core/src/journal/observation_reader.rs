// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::JournalError;
use crate::event::observation::{CaptureScope, ObservabilityContext};
use crate::event::observation_families::ObservationKind;
use crate::WriterId;
use async_trait::async_trait;

/// Family subjects (effect type, edge endpoints) belong to the existing kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ObservationKey {
    pub capture_scope: CaptureScope,
    pub observer: WriterId,
    pub kind: ObservationKind,
}

#[derive(Debug, Clone)]
pub struct LocatedObservation {
    /// Portable committed-record position, independent of EventId uniqueness.
    pub position: u64,
    pub observation: ObservabilityContext,
}

#[derive(Debug, Clone)]
pub enum ObservationLookup<T = Option<LocatedObservation>> {
    Ready {
        committed_len: u64,
        observation: T,
    },
    Rebuilding {
        examined_through: u64,
        /// Unknown while a read-only archive's unindexed suffix is examined.
        committed_len: Option<u64>,
    },
}

/// Optional measurements have their own lookup status and failure domain.
#[async_trait]
pub trait JournalObservationReader: Send + Sync {
    async fn latest_observation(
        &self,
        key: &ObservationKey,
    ) -> Result<ObservationLookup, JournalError>;

    /// Retained families for one observer, including independent partial packets.
    async fn latest_observations(
        &self,
        observer: WriterId,
    ) -> Result<ObservationLookup<Vec<LocatedObservation>>, JournalError>;
}
