// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Facts a merge or join dispatch reports about the control rows it reads.

use crate::messaging::upstream_subscription::{EofOutcome, ReaderFrontier};
use crate::messaging::DeliveredRecord;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::{ReaderGeneration, StageId, StageKey, WriterId};
use std::sync::Arc;

/// A catch-up watermark a stage read, with the frontier of the inputs it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatermarkRead {
    pub(crate) generation: ReaderGeneration,
    pub(crate) frontier: ReaderFrontier,
}

/// An EOF a stage read, with what its subscription knew once it arrived.
#[derive(Clone, Debug)]
pub struct EofRead {
    pub(crate) envelope: Arc<DeliveredRecord<ChainPayload>>,
    pub(crate) ends_reads: bool,
    pub(crate) side_worst: Option<EofKind>,
    pub(crate) frontier: ReaderFrontier,
    pub(crate) generation: ReaderGeneration,
    pub(crate) ended_by: Option<StageKey>,
}

impl EofRead {
    pub(crate) fn new(
        envelope: DeliveredRecord<ChainPayload>,
        outcome: Option<EofOutcome>,
        ends_reads: bool,
        generation: Option<ReaderGeneration>,
        frontier: ReaderFrontier,
    ) -> Self {
        Self {
            envelope: Arc::new(envelope),
            ends_reads,
            side_worst: outcome.as_ref().and_then(|outcome| outcome.worst_kind),
            frontier,
            generation: generation.unwrap_or_default(),
            ended_by: outcome.map(|outcome| StageKey::from(outcome.stage_name)),
        }
    }

    pub(crate) fn clock(&self) -> Option<VectorClock> {
        match &self.envelope.payload {
            ChainPayload::FlowControl(FlowControlPayload::Eof { vector_clock, .. }) => {
                vector_clock.clone()
            }
            _ => None,
        }
    }

    pub(crate) fn author(&self) -> Option<(StageId, EofKind)> {
        let event = self.envelope.authored();
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
}

pub(crate) fn fold_worst(worst: Option<EofKind>, kind: Option<EofKind>) -> Option<EofKind> {
    match (worst, kind) {
        (Some(current), Some(kind)) => Some(current.worst(kind)),
        (current, kind) => current.or(kind),
    }
}
