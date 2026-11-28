// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{open_disk_run, JournalReadError};
use obzenflow_core::event::payloads::chain_payload::EventKind;
use obzenflow_core::event::payloads::effect_payload::EffectOutcomePayload;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::read::{RunOutcome, RunRecordData};
use obzenflow_core::EventId;
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RebuildTally {
    pub(crate) reached_last_settled_action: bool,
    pub(crate) sinks: Vec<SinkTally>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SinkTally {
    pub(crate) stage: String,
    pub(crate) made: u64,
    pub(crate) unsettled: u64,
}

#[derive(Default)]
struct Observed {
    made: HashSet<String>,
    settled: HashSet<EventId>,
    delivered: Vec<(EventId, String)>,
}

pub(crate) async fn read_rebuild_tally(
    run_dir: &Path,
    stages: &[String],
) -> Result<RebuildTally, JournalReadError> {
    let mut snapshot = open_disk_run(run_dir).await?;
    let upstream: HashMap<String, Vec<String>> = stages
        .iter()
        .map(|stage| {
            let inbound = snapshot
                .manifest()
                .stages
                .get(stage)
                .map(|recorded| recorded.inbound.clone())
                .unwrap_or_default();
            (stage.clone(), inbound)
        })
        .collect();
    let mut observed: HashMap<String, Observed> = stages
        .iter()
        .map(|stage| (stage.clone(), Observed::default()))
        .collect();
    while let Some(record) = snapshot.next().await? {
        let RunRecordData::Chain(row) = &record.record else {
            continue;
        };
        let Some(stage) = record
            .journal
            .stage
            .as_ref()
            .map(|stage| stage.key.as_str())
        else {
            continue;
        };
        let event = &row.envelope.provenance.event;
        if let Some(sink) = observed.get_mut(stage) {
            if let ChainPayload::Execution(ExecutionPayload::EffectRecord(effect)) = &row.payload {
                sink.settled
                    .extend(event.causality.parent_ids.iter().copied());
                let succeeded = matches!(
                    effect.outcome,
                    EffectOutcomePayload::Succeeded { .. }
                        | EffectOutcomePayload::SucceededFact { .. }
                );
                if succeeded && event.replay_context.is_none() {
                    sink.made.insert(format!(
                        "{}/{}",
                        effect.cursor.input_seq, effect.cursor.effect_ordinal
                    ));
                }
            }
        }
        if !matches!(event.event_kind, EventKind::Fact | EventKind::CompositeData) {
            continue;
        }
        for (sink, inbound) in &upstream {
            if inbound.iter().any(|key| key == stage) {
                if let Some(observed) = observed.get_mut(sink) {
                    observed
                        .delivered
                        .push((event.id, event.event_type.clone()));
                }
            }
        }
    }
    let reached_last_settled_action =
        match &snapshot.into_tail().progress().outcome {
            Some(recorded) => match &recorded.outcome {
                RunOutcome::Completed => true,
                RunOutcome::Cancelled { reason } => reason
                    == obzenflow_runtime::stages::common::stage_handle::STOP_REASON_RECORDING_ENDED,
                RunOutcome::Failed { .. } | RunOutcome::NotStarted => false,
            },
            None => false,
        };
    let sinks = stages
        .iter()
        .map(|stage| {
            let observed = observed.remove(stage).unwrap_or_default();
            let accepted: HashSet<&str> = observed
                .delivered
                .iter()
                .filter(|(id, _)| observed.settled.contains(id))
                .map(|(_, event_type)| event_type.as_str())
                .collect();
            let unsettled = observed
                .delivered
                .iter()
                .filter(|(id, event_type)| {
                    !observed.settled.contains(id)
                        && (accepted.is_empty() || accepted.contains(event_type.as_str()))
                })
                .count();
            SinkTally {
                stage: stage.clone(),
                made: observed.made.len() as u64,
                unsettled: unsettled as u64,
            }
        })
        .collect();
    Ok(RebuildTally {
        reached_last_settled_action,
        sinks,
    })
}
