// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded, read-only proof over admitted commitments. Missing archive evidence
//! and cache-budget exhaustion are unresolved, never proof of corruption.

use super::read::{RunRecord, RunRecordData};
use crate::event::vector_clock::VectorClock;
use crate::event::{CausalCommit, CausalError, CausalWitnesses, CommittedCausalRef};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CausalProof {
    Valid {
        merged: VectorClock,
        resolved: Vec<ResolvedCausalWitness>,
        commitment: CommittedCausalRef,
    },
    Unresolved {
        missing: Vec<CommittedCausalRef>,
        budget_exhausted: bool,
    },
    Invalid {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedCausalWitness {
    pub reference: CommittedCausalRef,
    pub clock: VectorClock,
}

// A journal incarnation cannot acquire another run namespace. Keep the run in
// the full reference, so a mismatched run is a contradiction, not a cache miss.
type CommitmentKey = (crate::event::CausalCoordinate, u64);
fn identity(reference: &CommittedCausalRef) -> CommitmentKey {
    (reference.coordinate(), reference.sequence)
}

/// Callers supply only records from their admitted cuts. This cache performs no
/// I/O, recursive ancestor expansion, live-state lookup or event-ID fallback.
pub struct CausalProofCache {
    records: BTreeMap<CommittedCausalRef, CausalCommit>,
    order: VecDeque<CommittedCausalRef>,
    identities: BTreeMap<CommitmentKey, CommittedCausalRef>,
    max_records: usize,
    max_components: usize,
    components: usize,
    exhausted: bool,
}

impl CausalProofCache {
    pub fn new(max_records: usize, max_components: usize) -> Self {
        Self {
            records: BTreeMap::new(),
            order: VecDeque::new(),
            identities: BTreeMap::new(),
            max_records,
            max_components,
            components: 0,
            exhausted: false,
        }
    }

    pub fn admit(&mut self, commitment: CausalCommit) -> Result<(), CausalError> {
        let reference = commitment.reference;
        if let Some(known) = self.identities.get(&identity(&reference)) {
            let existing = &self.records[known];
            if existing.reference != reference || existing.clock != commitment.clock {
                return Err(CausalError::ConflictingCommitment);
            }
            return Ok(());
        }
        let size = commitment.clock.clocks.len();
        if self.max_records == 0 || size > self.max_components {
            self.exhausted = true;
            return Ok(());
        }
        while self.records.len() >= self.max_records || self.components + size > self.max_components
        {
            let oldest = self.order.pop_front().expect("nonempty proof cache");
            self.identities.remove(&identity(&oldest));
            self.components -= self
                .records
                .remove(&oldest)
                .expect("cached reference")
                .clock
                .clocks
                .len();
            self.exhausted = true;
        }
        self.components += size;
        self.order.push_back(reference);
        self.identities.insert(identity(&reference), reference);
        self.records.insert(reference, commitment);
        Ok(())
    }

    pub fn admit_run_record(&mut self, record: &RunRecord) -> Result<(), CausalError> {
        self.admit(record.causal_commit()?)
    }

    pub fn verify(&self, commitment: &CausalCommit, witnesses: &CausalWitnesses) -> CausalProof {
        let mut resolved = Vec::new();
        let mut missing = Vec::new();
        for reference in witnesses.previous.iter().chain(&witnesses.witnesses) {
            match self.records.get(reference) {
                Some(evidence) => {
                    if !crate::event::vector_clock::CausalOrderingService::happened_before(
                        &evidence.clock,
                        &commitment.clock,
                    ) {
                        return CausalProof::Invalid {
                            reason: CausalError::NonPredecessor.to_string(),
                        };
                    }
                    resolved.push(evidence.clone());
                }
                None => {
                    if self.identities.contains_key(&identity(reference)) {
                        return CausalProof::Invalid {
                            reason: CausalError::ConflictingCommitment.to_string(),
                        };
                    }
                    missing.push(*reference);
                }
            }
        }
        if !missing.is_empty() {
            return CausalProof::Unresolved {
                missing,
                budget_exhausted: self.exhausted,
            };
        }
        match commitment.verify(witnesses, &resolved) {
            Ok(merged) => CausalProof::Valid {
                merged,
                resolved: resolved
                    .into_iter()
                    .map(|evidence| ResolvedCausalWitness {
                        reference: evidence.reference,
                        clock: evidence.clock,
                    })
                    .collect(),
                commitment: commitment.reference,
            },
            Err(error) => CausalProof::Invalid {
                reason: error.to_string(),
            },
        }
    }

    pub fn verify_run_record(&self, record: &RunRecord) -> CausalProof {
        match record.causal_commit() {
            Ok(commitment) => self.verify(&commitment, record.causal_witnesses()),
            Err(error) => CausalProof::Invalid {
                reason: error.to_string(),
            },
        }
    }
}

impl RunRecord {
    pub fn causal_commit(&self) -> Result<CausalCommit, CausalError> {
        let commitment = match &self.record {
            RunRecordData::Chain(record) => CausalCommit::from_record(record)?,
            RunRecordData::System(record) => CausalCommit::from_record(record)?,
        };
        if commitment.reference.run_id != self.run.flow_id
            || commitment.reference.journal_writer_id.as_journal_id() != &self.journal.id
        {
            return Err(CausalError::ConflictingCommitment);
        }
        Ok(commitment)
    }

    pub fn causal_witnesses(&self) -> &CausalWitnesses {
        match &self.record {
            RunRecordData::Chain(record) => &record.envelope.provenance.journal.causal,
            RunRecordData::System(record) => &record.envelope.provenance.journal.causal,
        }
    }
}
