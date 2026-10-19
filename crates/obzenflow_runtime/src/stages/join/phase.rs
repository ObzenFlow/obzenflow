// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, JoinPhaseFact};
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::{ReaderGeneration, StageId, StageKey};

use super::config::{PhaseWitness, PhaseWitnesses};
use super::fsm::HoldCompletion;
use crate::stages::common::input_facts::EofRead;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HoldOutcome {
    Holding,
    Completed(HoldCompletion),
    LeftOpen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CrossingPhase {
    Hydrating,
    Enriching,
    Live,
}

impl CrossingPhase {
    fn rank(self) -> u8 {
        match self {
            Self::Hydrating => 0,
            Self::Enriching => 1,
            Self::Live => 2,
        }
    }

    fn phase_name(self) -> &'static str {
        match self {
            Self::Hydrating => "hydrating",
            Self::Enriching => "the batch",
            Self::Live => "live",
        }
    }
}

fn fact_rank(fact: &JoinPhaseFact) -> u8 {
    match fact {
        JoinPhaseFact::EnrichingEntered { .. } => 1,
        JoinPhaseFact::LiveEntered { .. } => 2,
    }
}

fn rank_name(rank: u8) -> &'static str {
    match rank {
        0 => "hydrating",
        1 => "the batch",
        _ => "live",
    }
}

pub(crate) fn check_crossing_phase(
    stage_name: &str,
    archived: &[JoinPhaseFact],
    target: ReaderGeneration,
    phase: CrossingPhase,
) -> Result<(), String> {
    let reached = archived
        .iter()
        .filter(|fact| fact.generation() < target.0)
        .map(fact_rank)
        .max()
        .unwrap_or(0);
    if reached > phase.rank() {
        return Err(format!(
            "join '{stage_name}' crossed into generation {} while {}, but its archive had \
             already entered {}; the resumed batch diverged from the recorded one \
             (FLOWIP-084q B1)",
            target.0,
            phase.phase_name(),
            rank_name(reached),
        ));
    }
    Ok(())
}

pub(crate) fn observe_hold(
    witnessed: &Arc<BTreeMap<StageKey, EofKind>>,
    witnesses: Option<&[PhaseWitness]>,
    witness_keys: &HashMap<StageId, StageKey>,
    eof: &EofRead,
) -> (Arc<BTreeMap<StageKey, EofKind>>, HoldOutcome) {
    let side_final = eof
        .ends_reads
        .then(|| eof.side_worst.unwrap_or(EofKind::Natural));
    let completion = |ended_by| HoldCompletion {
        generation: eof.generation.0,
        ended_by,
    };

    let Some(witnesses) = witnesses else {
        let outcome = match side_final {
            None => HoldOutcome::Holding,
            Some(EofKind::Natural) => {
                HoldOutcome::Completed(completion(eof.ended_by.clone().into_iter().collect()))
            }
            Some(_) => HoldOutcome::LeftOpen,
        };
        return (witnessed.clone(), outcome);
    };

    let mut witnessed = witnessed.clone();
    if let Some((author, kind)) = eof.author() {
        if let Some(key) = witness_keys.get(&author) {
            let seen = Arc::make_mut(&mut witnessed)
                .entry(key.clone())
                .or_insert(EofKind::Natural);
            *seen = seen.worst(kind);
        }
    }

    let completed = witnesses
        .iter()
        .all(|witness| witnessed.get(&witness.stage_key) == Some(&EofKind::Natural));
    let outcome = if completed {
        HoldOutcome::Completed(completion(
            witnesses
                .iter()
                .map(|witness| witness.stage_key.clone())
                .collect(),
        ))
    } else if side_final.is_some() {
        HoldOutcome::LeftOpen
    } else {
        HoldOutcome::Holding
    };
    (witnessed, outcome)
}

pub(crate) fn witness_keys(
    witnesses: Option<&PhaseWitnesses>,
    archive: Option<&Arc<dyn ReplayArchive>>,
) -> HashMap<StageId, StageKey> {
    let mut keys = HashMap::new();
    let Some(witnesses) = witnesses else {
        return keys;
    };
    for witness in witnesses.catalog.iter().chain(&witnesses.batch_stream) {
        keys.insert(witness.stage_id, witness.stage_key.clone());
        if let Some(archived) =
            archive.and_then(|archive| archive.archived_stage_id(witness.stage_key.as_str()).ok())
        {
            keys.insert(archived, witness.stage_key.clone());
        }
    }
    keys
}

pub(crate) async fn load_archived_phases(
    archive: &Arc<dyn ReplayArchive>,
    stage_key: &str,
) -> Result<Vec<JoinPhaseFact>, String> {
    let mut phases = read_archived_phases(archive, stage_key).await?;
    for ancestor in archive.ancestors() {
        for fact in read_archived_phases(&ancestor, stage_key).await? {
            if !phases.contains(&fact) {
                phases.push(fact);
            }
        }
    }
    Ok(phases)
}

async fn read_archived_phases(
    archive: &Arc<dyn ReplayArchive>,
    stage_key: &str,
) -> Result<Vec<JoinPhaseFact>, String> {
    let mut reader = match archive.open_effect_history(stage_key).await {
        Ok(reader) => reader,
        Err(ReplayError::StageNotInManifest { .. }) => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "join '{stage_key}': cannot open its archived journal: {error}"
            ))
        }
    };
    let mut phases = Vec::new();
    while let Some(record) = reader
        .next()
        .await
        .map_err(|error| format!("join '{stage_key}': cannot read its archived journal: {error}"))?
    {
        if let ChainPayload::Execution(ExecutionPayload::JoinPhase(fact)) = &record.payload {
            phases.push(fact.clone());
        }
    }
    Ok(phases)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(rank: u8, generation: u64) -> JoinPhaseFact {
        let ended_by = vec![StageKey::from("batch")];
        match rank {
            1 => JoinPhaseFact::EnrichingEntered {
                generation,
                ended_by,
            },
            _ => JoinPhaseFact::LiveEntered {
                generation,
                ended_by,
            },
        }
    }

    #[test]
    fn a_crossing_in_the_phase_the_archive_reached_passes() {
        let archived = [fact(1, 0)];
        assert!(check_crossing_phase(
            "posted",
            &archived,
            ReaderGeneration(1),
            CrossingPhase::Enriching
        )
        .is_ok());
    }

    #[test]
    fn a_crossing_in_an_earlier_phase_than_the_archive_reached_fails_closed() {
        let archived = [fact(1, 0), fact(2, 0)];
        let error = check_crossing_phase(
            "posted",
            &archived,
            ReaderGeneration(1),
            CrossingPhase::Enriching,
        )
        .unwrap_err();
        assert!(error.contains("already entered live"), "{error}");
    }

    #[test]
    fn phases_entered_after_the_boundary_do_not_count() {
        let archived = [fact(1, 0), fact(2, 1)];
        assert!(check_crossing_phase(
            "posted",
            &archived,
            ReaderGeneration(1),
            CrossingPhase::Enriching
        )
        .is_ok());
    }
}
