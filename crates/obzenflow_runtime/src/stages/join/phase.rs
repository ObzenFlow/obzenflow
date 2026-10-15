// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::HashMap;
use std::sync::Arc;

use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, JoinPhaseFact};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::{ReaderGeneration, StageId, StageKey, WriterId};

use crate::messaging::DeliveredRecord;
use crate::stages::common::handlers::UnifiedJoinHandler;

use super::config::{JoinReferenceMode, PhaseWitnesses};
use super::fsm::{HoldCompletion, JoinResources, JoinSubscriptionSide};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HoldOutcome {
    Holding,
    Completed(HoldCompletion),
    LeftOpen,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhaseReads {
    Reference,
    Stream,
    Both,
}

impl PhaseReads {
    pub(crate) fn reads_reference(self) -> bool {
        matches!(self, Self::Reference | Self::Both)
    }

    pub(crate) fn reads_stream(self) -> bool {
        matches!(self, Self::Stream | Self::Both)
    }

    pub(crate) fn held(self) -> Option<JoinSubscriptionSide> {
        match self {
            Self::Reference => Some(JoinSubscriptionSide::Stream),
            Self::Stream => Some(JoinSubscriptionSide::Reference),
            Self::Both => None,
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Reference => 0,
            Self::Stream => 1,
            Self::Both => 2,
        }
    }

    fn phase_name(self) -> &'static str {
        match self {
            Self::Reference => "hydrating",
            Self::Stream => "the batch",
            Self::Both => "live",
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
    reads: PhaseReads,
) -> Result<(), String> {
    let reached = archived
        .iter()
        .filter(|fact| fact.generation() < target.0)
        .map(fact_rank)
        .max()
        .unwrap_or(0);
    if reached > reads.rank() {
        return Err(format!(
            "join '{stage_name}' crossed into generation {} while {}, but its archive had \
             already entered {}; the resumed batch diverged from the recorded one \
             (FLOWIP-084q B1)",
            target.0,
            reads.phase_name(),
            rank_name(reached),
        ));
    }
    Ok(())
}

fn eof_author(envelope: &DeliveredRecord<ChainPayload>) -> Option<(StageId, EofKind)> {
    let event = envelope.authored();
    let ChainPayload::FlowControl(FlowControlPayload::Eof {
        kind, writer_id, ..
    }) = &event.payload
    else {
        return None;
    };
    match writer_id.unwrap_or(event.writer_id) {
        WriterId::Stage(stage) => Some((stage, *kind)),
        _ => None,
    }
}

pub(crate) fn observe_hold_eof<H: UnifiedJoinHandler>(
    ctx: &mut JoinResources<H>,
    side: JoinSubscriptionSide,
    envelope: &DeliveredRecord<ChainPayload>,
    side_final: Option<(EofKind, StageKey)>,
    generation: ReaderGeneration,
) -> HoldOutcome {
    let witnesses = ctx.phase_witnesses.as_ref().map(|witnesses| match side {
        JoinSubscriptionSide::Reference => witnesses.catalog.clone(),
        JoinSubscriptionSide::Stream => witnesses.batch_stream.clone(),
    });

    let Some(witnesses) = witnesses else {
        return match side_final {
            None => HoldOutcome::Holding,
            Some((EofKind::Natural, author)) => HoldOutcome::Completed(HoldCompletion {
                generation: generation.0,
                ended_by: vec![author],
            }),
            Some(_) => HoldOutcome::LeftOpen,
        };
    };

    if let Some((author, kind)) = eof_author(envelope) {
        if let Some(key) = ctx.witness_keys.get(&author) {
            let seen = ctx
                .witnessed_eofs
                .entry(key.clone())
                .or_insert(EofKind::Natural);
            *seen = seen.worst(kind);
        }
    }

    let completed = witnesses
        .iter()
        .all(|witness| ctx.witnessed_eofs.get(&witness.stage_key) == Some(&EofKind::Natural));
    if completed {
        HoldOutcome::Completed(HoldCompletion {
            generation: generation.0,
            ended_by: witnesses
                .into_iter()
                .map(|witness| witness.stage_key)
                .collect(),
        })
    } else if side_final.is_some() {
        HoldOutcome::LeftOpen
    } else {
        HoldOutcome::Holding
    }
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

pub(crate) fn enriching_holds_catalog(mode: JoinReferenceMode) -> bool {
    mode == JoinReferenceMode::FiniteThenLive
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
        assert!(
            check_crossing_phase("posted", &archived, ReaderGeneration(1), PhaseReads::Stream)
                .is_ok()
        );
    }

    #[test]
    fn a_crossing_in_an_earlier_phase_than_the_archive_reached_fails_closed() {
        let archived = [fact(1, 0), fact(2, 0)];
        let error =
            check_crossing_phase("posted", &archived, ReaderGeneration(1), PhaseReads::Stream)
                .unwrap_err();
        assert!(error.contains("already entered live"), "{error}");
    }

    #[test]
    fn phases_entered_after_the_boundary_do_not_count() {
        let archived = [fact(1, 0), fact(2, 1)];
        assert!(
            check_crossing_phase("posted", &archived, ReaderGeneration(1), PhaseReads::Stream)
                .is_ok()
        );
    }
}
