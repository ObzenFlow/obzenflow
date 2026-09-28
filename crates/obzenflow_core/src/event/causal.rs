// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal vector clocks. Inputs contribute componentwise maxima; a successful
//! append advances its destination journal. No per-component witnesses are kept.

use super::payloads::JournalPayload;
use super::vector_clock::{CausalOrderingService, VectorClock, MAX_CAUSAL_COORDINATES};
use super::{EventId, JournalRecord, JournalWriterId};
use crate::FlowId;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "causal_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CausalCoordinate {
    pub journal_writer_id: JournalWriterId,
}

impl CausalCoordinate {
    pub fn new(journal_writer_id: JournalWriterId) -> Self {
        Self { journal_writer_id }
    }
}

/// Identity of one placement in a journal. Used for local continuity and UI
/// freshness, independently of the event's original authorship or clock width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalCommitRef {
    pub run_id: FlowId,
    pub journal_writer_id: JournalWriterId,
    pub sequence: u64,
    pub event_id: EventId,
}

impl JournalCommitRef {
    pub fn coordinate(&self) -> CausalCoordinate {
        CausalCoordinate::new(self.journal_writer_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CausalError {
    #[error("clock propagation requires an unchanged record from a successful journal append or admitted reader")]
    UnadmittedRecord,
    #[error("journal commitment identity or predecessor is inconsistent")]
    ConflictingCommitment,
    #[error("input clock claims a destination sequence beyond its committed prefix")]
    FutureDestination,
    #[error("causal coordinate budget exceeded")]
    CoordinateBudget,
    #[error("causal sequence exhausted")]
    SequenceExhausted,
    #[error("journal clock has no positive local sequence")]
    MissingSequence,
}

/// Accumulated clocks of incorporated inputs and completed publications.
/// Capturing this value freezes the causes of queued work.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CausalFrontier {
    pub(super) clock: VectorClock,
}

impl CausalFrontier {
    pub fn from_record<P: JournalPayload>(record: &JournalRecord<P>) -> Result<Self, CausalError> {
        if !record.is_admitted() {
            return Err(CausalError::UnadmittedRecord);
        }
        Ok(Self {
            clock: record.envelope.provenance.journal.vector_clock.clone(),
        })
    }

    pub fn clock(&self) -> &VectorClock {
        &self.clock
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), CausalError> {
        // Check the finite width before mutation; no reference map, temporary
        // identity index or witness tie-breaking accompanies a clock merge.
        if self
            .clock
            .clocks
            .len()
            .saturating_add(other.clock.clocks.len())
            > MAX_CAUSAL_COORDINATES
        {
            let added = other
                .clock
                .clocks
                .keys()
                .filter(|key| !self.clock.clocks.contains_key(key))
                .count();
            if self.clock.clocks.len() + added > MAX_CAUSAL_COORDINATES {
                return Err(CausalError::CoordinateBudget);
            }
        }
        CausalOrderingService::update_with_parent(&mut self.clock, &other.clock);
        Ok(())
    }
}

/// Journal append/recovery state. Preparing it does not commit storage or admit
/// a record; providers install it only after the physical append succeeds.
#[derive(Debug, Clone)]
pub struct JournalClock {
    pub reference: JournalCommitRef,
    pub clock: VectorClock,
}

impl JournalClock {
    pub fn from_record<P: JournalPayload>(record: &JournalRecord<P>) -> Result<Self, CausalError> {
        Ok(Self {
            reference: Self::validate_record(record)?,
            clock: record.envelope.provenance.journal.vector_clock.clone(),
        })
    }

    pub(crate) fn validate_record<P: JournalPayload>(
        record: &JournalRecord<P>,
    ) -> Result<JournalCommitRef, CausalError> {
        #[cfg(feature = "bench-instrumentation")]
        crate::benchmark::add(crate::benchmark::Counter::StructuralValidations, 1);
        let journal = &record.envelope.provenance.journal;
        if journal.vector_clock.clocks.len() > MAX_CAUSAL_COORDINATES {
            return Err(CausalError::CoordinateBudget);
        }
        if journal
            .vector_clock
            .clocks
            .values()
            .any(|sequence| *sequence == 0)
        {
            return Err(CausalError::MissingSequence);
        }
        let reference = record.commitment();
        if reference.sequence == 0 {
            return Err(CausalError::MissingSequence);
        }
        let previous = journal.previous;
        if previous.is_some_and(|previous| {
            previous.sequence == 0
                || previous.run_id != reference.run_id
                || previous.journal_writer_id != reference.journal_writer_id
        }) || previous.map_or(Some(1), |previous| previous.sequence.checked_add(1))
            != Some(reference.sequence)
        {
            return Err(CausalError::ConflictingCommitment);
        }
        #[cfg(feature = "bench-instrumentation")]
        crate::benchmark::add(
            crate::benchmark::Counter::ValidatedClockEntries,
            journal.vector_clock.clocks.len() as u64,
        );
        Ok(reference)
    }

    pub fn prepare(
        run_id: FlowId,
        coordinate: CausalCoordinate,
        event_id: EventId,
        previous: Option<&Self>,
        input: &CausalFrontier,
    ) -> Result<(Self, Option<JournalCommitRef>), CausalError> {
        let sequence = previous.map_or(0, |previous| previous.reference.sequence);
        if previous.is_some_and(|previous| {
            previous.reference.coordinate() != coordinate || previous.reference.run_id != run_id
        }) {
            return Err(CausalError::ConflictingCommitment);
        }
        if input.clock.get(&coordinate) > sequence {
            return Err(CausalError::FutureDestination);
        }
        let mut frontier = CausalFrontier {
            clock: previous
                .map(|previous| previous.clock.clone())
                .unwrap_or_default(),
        };
        frontier.merge(input)?;
        let sequence = sequence
            .checked_add(1)
            .ok_or(CausalError::SequenceExhausted)?;
        frontier.clock.clocks.insert(coordinate, sequence);
        if frontier.clock.clocks.len() > MAX_CAUSAL_COORDINATES {
            return Err(CausalError::CoordinateBudget);
        }
        Ok((
            Self {
                reference: JournalCommitRef {
                    run_id,
                    journal_writer_id: coordinate.journal_writer_id,
                    sequence,
                    event_id,
                },
                clock: frontier.clock,
            },
            previous.map(|previous| previous.reference),
        ))
    }
}
