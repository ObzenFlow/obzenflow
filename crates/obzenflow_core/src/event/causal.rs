// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Committed causal evidence. Authorship and the authority advancing a clock
//! are separate: forwarding keeps the author and changes the journal coordinate.

use super::payloads::JournalPayload;
use super::vector_clock::{CausalOrderingService, VectorClock};
use super::{EventId, JournalRecord, JournalWriterId};
use crate::FlowId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedCausalRef {
    pub run_id: FlowId,
    pub journal_writer_id: JournalWriterId,
    pub sequence: u64,
    pub event_id: EventId,
}

impl CommittedCausalRef {
    pub fn coordinate(&self) -> CausalCoordinate {
        CausalCoordinate::new(self.journal_writer_id)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CausalWitnesses {
    pub previous: Option<CommittedCausalRef>,
    #[serde(deserialize_with = "super::vector_clock::bounded_entries")]
    pub witnesses: Vec<CommittedCausalRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CausalError {
    #[error("causal evidence requires an unchanged record from a successful journal append or admitted reader")]
    UnadmittedRecord,
    #[error("causal reference does not match committed record")]
    ConflictingCommitment,
    #[error("causal evidence claims a destination sequence beyond its committed prefix")]
    FutureDestination,
    #[error("causal coordinate budget exceeded")]
    CoordinateBudget,
    #[error("causal sequence exhausted")]
    SequenceExhausted,
    #[error("causal clock does not equal witnessed merge plus local increment")]
    UnexplainedClock,
    #[error("causal commitment has no positive local sequence")]
    MissingSequence,
    #[error("causal witness does not strictly precede its child")]
    NonPredecessor,
}

/// A disposable fold of admitted commitments, never an independent clock.
/// There is one vector and one fixed-size witness per coordinate. In particular,
/// a witness does not retain another vector or its own ancestor list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CausalFrontier {
    clock: VectorClock,
    witnesses: BTreeMap<CausalCoordinate, CommittedCausalRef>,
}

impl CausalFrontier {
    /// Admit a successful append receipt or a record from an admitted reader.
    /// Providers remain responsible for commitment; deserialised references by
    /// themselves cannot construct a frontier.
    pub fn from_record<P: JournalPayload>(record: &JournalRecord<P>) -> Result<Self, CausalError> {
        let commitment = CausalCommit::from_record(record)?;
        Ok(commitment.frontier())
    }

    pub fn clock(&self) -> &VectorClock {
        &self.clock
    }

    pub fn witness_count(&self) -> usize {
        self.witnesses.len()
    }

    pub fn references(&self) -> Vec<CommittedCausalRef> {
        self.witnesses
            .values()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Per-coordinate maximum of (counter, witness). The reference order is a
    /// stable tie-break, making the complete fold associative, commutative and
    /// idempotent, including when one reference supports several components.
    pub fn merge(&mut self, other: &Self) -> Result<(), CausalError> {
        let mut identities = BTreeMap::new();
        for reference in self.witnesses.values().chain(other.witnesses.values()) {
            let key = (reference.coordinate(), reference.sequence);
            if identities
                .insert(key, *reference)
                .is_some_and(|previous| previous != *reference)
            {
                return Err(CausalError::ConflictingCommitment);
            }
        }
        for (coordinate, &counter) in &other.clock.clocks {
            let witness = other.witnesses[coordinate];
            let current = self.clock.get(coordinate);
            if counter > current
                || (counter == current
                    && self
                        .witnesses
                        .get(coordinate)
                        .is_none_or(|previous| witness < *previous))
            {
                self.clock.clocks.insert(*coordinate, counter);
                self.witnesses.insert(*coordinate, witness);
            }
        }
        Ok(())
    }
}

/// Evidence from an unchanged, admitted journal record. Preparation and decoding
/// alone cannot construct this type. Its contents are read-only.
///
/// ```compile_fail
/// use obzenflow_core::event::CausalCommit;
/// fn change_evidence(mut committed: CausalCommit) {
///     committed.reference.sequence = 42;
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CausalCommit(PreparedCausalCommit);

impl std::ops::Deref for CausalCommit {
    type Target = PreparedCausalCommit;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CausalCommit {
    pub fn from_record<P: JournalPayload>(record: &JournalRecord<P>) -> Result<Self, CausalError> {
        if !record.is_admitted() {
            return Err(CausalError::UnadmittedRecord);
        }
        Ok(Self(PreparedCausalCommit::from_record(record)?))
    }

    pub fn frontier(&self) -> CausalFrontier {
        self.0.preparation_frontier()
    }

    pub fn into_prepared(self) -> PreparedCausalCommit {
        self.0
    }

    pub fn verify(
        &self,
        provenance: &CausalWitnesses,
        resolved: &[Self],
    ) -> Result<VectorClock, CausalError> {
        self.0
            .verify_resolved(provenance, resolved.iter().map(|commit| &commit.0))
    }
}

/// Provider-owned arithmetic for one coordinate. This is candidate data, not
/// evidence of commitment. Keep it private until the whole append/group commits.
/// It cannot enter an append frontier or a proof cache.
///
/// ```compile_fail
/// use obzenflow_core::event::PreparedCausalCommit;
/// fn publish_candidate(candidate: PreparedCausalCommit) {
///     let _ = candidate.frontier();
/// }
/// ```
///
/// ```compile_fail
/// use obzenflow_core::event::PreparedCausalCommit;
/// use obzenflow_core::journal::causal::CausalProofCache;
/// fn prove_candidate(cache: &mut CausalProofCache, candidate: PreparedCausalCommit) {
///     cache.admit(candidate).unwrap();
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PreparedCausalCommit {
    pub reference: CommittedCausalRef,
    pub clock: VectorClock,
}

impl PreparedCausalCommit {
    /// Validate record structure without asserting that storage committed it.
    pub fn from_record<P: JournalPayload>(record: &JournalRecord<P>) -> Result<Self, CausalError> {
        let journal = &record.envelope.provenance.journal;
        if journal.vector_clock.clocks.len() > super::vector_clock::MAX_CAUSAL_COORDINATES {
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
        let coordinate = CausalCoordinate::new(journal.journal_writer_id);
        let sequence = journal.vector_clock.get(&coordinate);
        if sequence == 0 {
            return Err(CausalError::MissingSequence);
        }
        let previous = journal.causal.previous;
        if previous.is_some_and(|reference| {
            reference.run_id != journal.run_id || reference.coordinate() != coordinate
        }) || previous.map_or(Some(1), |reference| reference.sequence.checked_add(1))
            != Some(sequence)
        {
            return Err(CausalError::ConflictingCommitment);
        }
        if journal.causal.witnesses.len() > journal.vector_clock.clocks.len()
            || journal
                .causal
                .witnesses
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(CausalError::ConflictingCommitment);
        }
        for reference in previous.iter().chain(&journal.causal.witnesses) {
            if reference.sequence == 0
                || reference.sequence > journal.vector_clock.get(&reference.coordinate())
                || (reference.coordinate() == coordinate && reference.sequence >= sequence)
                || journal.causal.witnesses.contains(reference) && Some(*reference) == previous
            {
                return Err(CausalError::ConflictingCommitment);
            }
        }
        Ok(Self {
            reference: CommittedCausalRef {
                run_id: journal.run_id,
                journal_writer_id: journal.journal_writer_id,
                sequence,
                event_id: *record.id(),
            },
            clock: journal.vector_clock.clone(),
        })
    }

    fn preparation_frontier(&self) -> CausalFrontier {
        CausalFrontier {
            clock: self.clock.clone(),
            witnesses: self
                .clock
                .clocks
                .keys()
                .map(|coordinate| (*coordinate, self.reference))
                .collect(),
        }
    }

    pub fn prepare(
        run_id: FlowId,
        coordinate: CausalCoordinate,
        event_id: EventId,
        previous: Option<&Self>,
        input: &CausalFrontier,
    ) -> Result<(Self, CausalWitnesses), CausalError> {
        let sequence = previous.map_or(0, |previous| previous.reference.sequence);
        if previous.is_some_and(|previous| {
            previous.reference.coordinate() != coordinate || previous.reference.run_id != run_id
        }) {
            return Err(CausalError::ConflictingCommitment);
        }
        if input.clock.get(&coordinate) > sequence {
            return Err(CausalError::FutureDestination);
        }
        let mut frontier = previous.map(Self::preparation_frontier).unwrap_or_default();
        frontier.merge(input)?;
        let previous_ref = previous.map(|previous| previous.reference);
        let witnesses = CausalWitnesses {
            previous: previous_ref,
            witnesses: frontier
                .references()
                .into_iter()
                .filter(|reference| Some(*reference) != previous_ref)
                .collect(),
        };
        let sequence = sequence
            .checked_add(1)
            .ok_or(CausalError::SequenceExhausted)?;
        frontier.clock.clocks.insert(coordinate, sequence);
        if frontier.clock.clocks.len() > super::vector_clock::MAX_CAUSAL_COORDINATES {
            return Err(CausalError::CoordinateBudget);
        }
        Ok((
            Self {
                reference: CommittedCausalRef {
                    run_id,
                    journal_writer_id: coordinate.journal_writer_id,
                    sequence,
                    event_id,
                },
                clock: frontier.clock,
            },
            witnesses,
        ))
    }

    /// Exact arithmetic over resolved immediate witnesses. Resolution, admission
    /// budgets and unavailable-archive reporting belong to the reader adapter.
    pub fn verify(
        &self,
        provenance: &CausalWitnesses,
        resolved: &[Self],
    ) -> Result<VectorClock, CausalError> {
        self.verify_resolved(provenance, resolved.iter())
    }

    fn verify_resolved<'a>(
        &self,
        provenance: &CausalWitnesses,
        resolved: impl ExactSizeIterator<Item = &'a Self>,
    ) -> Result<VectorClock, CausalError> {
        let references: Vec<_> = provenance
            .previous
            .iter()
            .chain(&provenance.witnesses)
            .collect();
        if references.len() != resolved.len() {
            return Err(CausalError::ConflictingCommitment);
        }
        let coordinate = self.reference.coordinate();
        let mut merged = VectorClock::new();
        for (reference, evidence) in references.into_iter().zip(resolved) {
            if *reference != evidence.reference {
                return Err(CausalError::ConflictingCommitment);
            }
            if !CausalOrderingService::happened_before(&evidence.clock, &self.clock) {
                return Err(CausalError::NonPredecessor);
            }
            CausalOrderingService::update_with_parent(&mut merged, &evidence.clock);
        }
        let previous_sequence = provenance.previous.map_or(0, |previous| previous.sequence);
        if provenance.previous.is_some_and(|previous| {
            previous.coordinate() != coordinate || previous.run_id != self.reference.run_id
        }) || merged.get(&coordinate) != previous_sequence
        {
            return Err(CausalError::ConflictingCommitment);
        }
        let next = previous_sequence
            .checked_add(1)
            .ok_or(CausalError::SequenceExhausted)?;
        let maximum = merged.clone();
        merged.clocks.insert(coordinate, next);
        if next != self.reference.sequence || merged != self.clock {
            return Err(CausalError::UnexplainedClock);
        }
        Ok(maximum)
    }
}
