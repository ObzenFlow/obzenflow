// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::execution::DeliverySettlement;
use crate::stages::SinkResumeEdges;
use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
use obzenflow_core::event::{ChainPayload, JournalCommitRef};
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::{ChainEvent, JournalWriterId, StageId};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
struct EdgeReceipts {
    inputs: Vec<(JournalCommitRef, bool)>,
}

impl EdgeReceipts {
    fn observe(&mut self, input: JournalCommitRef, terminal: bool) {
        match self.inputs.iter_mut().find(|(known, _)| *known == input) {
            Some((_, settled)) => *settled |= terminal,
            None => self.inputs.push((input, terminal)),
        }
    }

    fn settled_prefix(&self) -> u64 {
        self.inputs
            .iter()
            .take_while(|(_, settled)| *settled)
            .count() as u64
    }
}

pub(crate) fn settled_prefixes<'a>(
    records: impl IntoIterator<Item = &'a ChainEvent>,
    journal_stages: &HashMap<JournalWriterId, String>,
) -> HashMap<String, u64> {
    let mut edges: HashMap<String, EdgeReceipts> = HashMap::new();
    for record in records {
        let ChainPayload::Delivery(receipt) = &record.payload else {
            continue;
        };
        let Some(upstream) = journal_stages.get(&receipt.subject.input.journal_writer_id) else {
            continue;
        };
        let terminal = !matches!(receipt.outcome.result, DeliveryResult::Buffered { .. });
        edges
            .entry(upstream.clone())
            .or_default()
            .observe(receipt.subject.input, terminal);
    }
    edges
        .into_iter()
        .map(|(upstream, receipts)| (upstream, receipts.settled_prefix()))
        .collect()
}

pub(crate) async fn load_settled_prefixes(
    archive: &Arc<dyn ReplayArchive>,
    stage_key: &str,
) -> Result<HashMap<String, u64>, ReplayError> {
    let mut settled: HashMap<String, u64> = HashMap::new();
    let members = std::iter::once(archive.clone()).chain(archive.ancestors());
    for member in members {
        if matches!(
            member.archived_stage_id(stage_key),
            Err(ReplayError::StageNotInManifest { .. })
        ) {
            continue;
        }
        let journal_stages = member.journal_stages();
        let mut reader = member.open_effect_history(stage_key).await?;
        let mut records = Vec::new();
        while let Some(record) = reader.next().await.map_err(|error| ReplayError::Parse {
            message: format!("Failed to read sink receipts for '{stage_key}': {error}"),
        })? {
            records.push(record.authored());
        }
        for (edge, prefix) in settled_prefixes(&records, &journal_stages) {
            let longest = settled.entry(edge).or_default();
            *longest = (*longest).max(prefix);
        }
    }
    Ok(settled)
}

#[derive(Debug, Default)]
pub(crate) struct SinkReceiptClassification {
    edges: SinkResumeEdges,
    settled: HashMap<String, u64>,
    delivered: HashMap<StageId, u64>,
}

impl SinkReceiptClassification {
    pub(crate) fn new(edges: SinkResumeEdges) -> Self {
        Self {
            edges,
            ..Self::default()
        }
    }

    pub(crate) fn certified_edges(&self) -> &SinkResumeEdges {
        &self.edges
    }

    pub(crate) fn with_settled(mut self, settled: HashMap<String, u64>) -> Self {
        self.settled = settled;
        self
    }

    fn settled_on(&self, upstream: StageId) -> Option<u64> {
        self.edges
            .certified
            .get(&upstream)
            .map(|key| self.settled.get(key).copied().unwrap_or(0))
    }

    pub(crate) fn classify(&mut self, upstream: StageId) -> DeliverySettlement {
        let ordinal = self.delivered.entry(upstream).or_default();
        let position = *ordinal;
        *ordinal += 1;
        match self.settled_on(upstream) {
            Some(settled) if position >= settled => DeliverySettlement::FirstDelivery,
            Some(_) => DeliverySettlement::Settled,
            None => DeliverySettlement::Unclassified,
        }
    }

    pub(crate) fn edges_redelivering(&self) -> usize {
        self.edges
            .certified
            .iter()
            .filter(|&(upstream, key)| {
                self.delivered.get(upstream).copied().unwrap_or(0)
                    < self.settled.get(key).copied().unwrap_or(0)
            })
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryOutcome};
    use obzenflow_core::event::{ChainEventFactory, ChainPayload};
    use obzenflow_core::{EventId, FlowId, WriterId};

    fn input(writer: JournalWriterId, sequence: u64) -> JournalCommitRef {
        JournalCommitRef {
            run_id: FlowId::new(),
            journal_writer_id: writer,
            sequence,
            event_id: EventId::new(),
        }
    }

    fn receipt(input: &JournalCommitRef, outcome: DeliveryOutcome) -> ChainEvent {
        let parent = crate::testing::causal_fixture::committed_input(
            input.journal_writer_id,
            ChainEventFactory::data_event(
                StageId::new().into(),
                "test.row",
                std::num::NonZeroU32::MIN,
                serde_json::json!({}),
            ),
        );
        let mut event = super::super::journalled_delivery_event(
            WriterId::from(StageId::new()),
            "test.sink",
            &parent.into(),
            outcome,
        );
        let ChainPayload::Delivery(payload) = &mut event.payload else {
            unreachable!("a journalled delivery event carries a delivery payload");
        };
        payload.subject.input = *input;
        event
    }

    fn delivered() -> DeliveryOutcome {
        DeliveryOutcome::success(DeliveryMethod::Noop, None)
    }

    fn buffered() -> DeliveryOutcome {
        let mut outcome = delivered();
        outcome.result = DeliveryResult::Buffered {};
        outcome
    }

    #[test]
    fn each_edge_keeps_its_settled_prefix_and_a_buffered_tail_is_unsettled() {
        let (left, right) = (JournalWriterId::new(), JournalWriterId::new());
        let stages = HashMap::from([(left, "left".to_string()), (right, "right".to_string())]);
        let (l0, l1, l2) = (input(left, 1), input(left, 2), input(left, 3));
        let r0 = input(right, 1);
        let records = vec![
            receipt(&l0, delivered()),
            receipt(&r0, buffered()),
            receipt(&l1, buffered()),
            receipt(&l2, delivered()),
            receipt(&l1, delivered()),
        ];
        let settled = settled_prefixes(&records, &stages);
        assert_eq!(
            settled["left"], 3,
            "a later commit receipt settles a buffered input"
        );
        assert_eq!(settled["right"], 0, "a buffered tail is unsettled");
    }

    #[test]
    fn a_delivery_past_the_settled_prefix_is_a_first_delivery() {
        let upstream = StageId::new();
        let uncertified = StageId::new();
        let mut classification = SinkReceiptClassification::new(SinkResumeEdges {
            certified: HashMap::from([(upstream, "orders".to_string())]),
        })
        .with_settled(HashMap::from([("orders".to_string(), 2)]));
        assert_eq!(
            classification.classify(upstream),
            DeliverySettlement::Settled
        );
        assert_eq!(
            classification.classify(upstream),
            DeliverySettlement::Settled
        );
        assert_eq!(
            classification.classify(upstream),
            DeliverySettlement::FirstDelivery
        );
        assert_eq!(
            classification.classify(uncertified),
            DeliverySettlement::Unclassified
        );
        assert_eq!(classification.edges_redelivering(), 0);
    }

    #[test]
    fn a_sink_with_two_edges_counts_each_edge_still_redelivering() {
        let (left, right) = (StageId::new(), StageId::new());
        let mut classification = SinkReceiptClassification::new(SinkResumeEdges {
            certified: HashMap::from([(left, "left".to_string()), (right, "right".to_string())]),
        })
        .with_settled(HashMap::from([
            ("left".to_string(), 0),
            ("right".to_string(), 1),
        ]));
        assert_eq!(
            classification.classify(left),
            DeliverySettlement::FirstDelivery
        );
        assert_eq!(
            classification.edges_redelivering(),
            1,
            "the right edge has not re-delivered its settled input"
        );
        assert_eq!(classification.classify(right), DeliverySettlement::Settled);
        assert_eq!(
            classification.classify(left),
            DeliverySettlement::FirstDelivery
        );
        assert_eq!(classification.edges_redelivering(), 0);
    }
}
