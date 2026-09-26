// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A dispatch view paired with the unchanged journal evidence it came from.

use obzenflow_core::event::payloads::JournalPayload;
use obzenflow_core::event::provenance::CompositeActivationContext;
use obzenflow_core::event::{ChainPayload, JournalRecord};
use std::ops::Deref;

/// Runtime enrichment belongs to the dispatched event, never to the committed
/// record. Cloning or deferring this value retains both, without admitting new
/// evidence. Journal operations always use `record()` as their causal parent.
#[derive(Clone, Debug)]
pub struct DeliveredRecord<P: JournalPayload> {
    record: JournalRecord<P>,
    // Ordinary deliveries retain only the original inline record. Allocate a
    // separate view only for inputs that need runtime enrichment.
    dispatch: Option<Box<P::Event>>,
}

impl<P: JournalPayload> From<JournalRecord<P>> for DeliveredRecord<P> {
    fn from(record: JournalRecord<P>) -> Self {
        Self {
            record,
            dispatch: None,
        }
    }
}

impl<P: JournalPayload> Deref for DeliveredRecord<P> {
    type Target = JournalRecord<P>;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl<P: JournalPayload> DeliveredRecord<P> {
    pub fn record(&self) -> &JournalRecord<P> {
        &self.record
    }

    pub fn authored(&self) -> P::Event {
        self.dispatch
            .as_deref()
            .cloned()
            .unwrap_or_else(|| self.record.authored())
    }

    pub(crate) fn authored_mut(&mut self) -> &mut P::Event {
        self.dispatch
            .get_or_insert_with(|| Box::new(self.record.authored()))
    }
}

impl DeliveredRecord<ChainPayload> {
    pub fn composite_activations(&self) -> &[CompositeActivationContext] {
        match &self.dispatch {
            Some(event) => event.composite_activations(),
            None => self.record.composite_activations(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::causal_fixture::committed_input;
    use obzenflow_core::event::{CausalCommit, ChainEventFactory};
    use obzenflow_core::id::CompositeId;
    use obzenflow_core::{JournalWriterId, StageId};
    use serde_json::json;

    #[test]
    fn dispatch_enrichment_preserves_original_committed_evidence() {
        let event = ChainEventFactory::data_event(StageId::new().into(), "input", json!({}));
        let record = committed_input(JournalWriterId::new(), event);
        let original = serde_json::to_value(&record).unwrap();
        let commitment = CausalCommit::from_record(&record).unwrap();
        let mut delivered = DeliveredRecord::from(record);
        let activation = CompositeActivationContext::new(
            CompositeId::new("composite"),
            *delivered.id(),
            "in",
            7,
        );
        delivered
            .authored_mut()
            .try_extend_composite_activations(std::slice::from_ref(&activation))
            .unwrap();
        assert_eq!(delivered.authored().composite_activations(), &[activation]);

        let deferred = delivered.clone();
        assert_eq!(serde_json::to_value(deferred.record()).unwrap(), original);
        let deferred_commitment = CausalCommit::from_record(deferred.record()).unwrap();
        assert_eq!(deferred_commitment.reference, commitment.reference);
        assert_eq!(deferred_commitment.clock, commitment.clock);
        assert!(deferred.record().composite_activations().is_empty());
    }
}
