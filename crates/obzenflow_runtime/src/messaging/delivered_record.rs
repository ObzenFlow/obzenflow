// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A dispatch view paired with the unchanged journal evidence it came from.

use obzenflow_core::event::payloads::JournalPayload;
use obzenflow_core::event::provenance::CompositeActivationContext;
use obzenflow_core::event::{ChainPayload, JournalRecord};
use obzenflow_core::EventDescriptor;
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

pub(crate) fn record_descriptor(record: &JournalRecord<ChainPayload>) -> EventDescriptor {
    let provenance = &record.envelope.provenance.event;
    EventDescriptor {
        event_kind: provenance.event_kind,
        event_type: provenance.event_type.clone().into(),
        payload_schema_version: provenance.payload_schema_version,
    }
}

/// Runtime enrichment belongs to the dispatched event, never to the committed
/// record. Clones share the record and its view; enrichment copies the view
/// first when another clone shares it. Journal operations always use
/// `record()` as their causal parent.
#[derive(Clone, Debug)]
pub struct DeliveredRecord<P: JournalPayload> {
    record: Arc<JournalRecord<P>>,
    view: OnceLock<Arc<P::Event>>,
}

impl<P: JournalPayload> From<JournalRecord<P>> for DeliveredRecord<P> {
    fn from(record: JournalRecord<P>) -> Self {
        Self::from(Arc::new(record))
    }
}

impl<P: JournalPayload> From<Arc<JournalRecord<P>>> for DeliveredRecord<P> {
    fn from(record: Arc<JournalRecord<P>>) -> Self {
        Self {
            record,
            view: OnceLock::new(),
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

    /// The dispatched event every reader of this record shares, built on first use.
    pub fn view(&self) -> &Arc<P::Event> {
        self.view.get_or_init(|| Arc::new(self.record.authored()))
    }

    /// An owned copy of the dispatched event.
    pub fn authored(&self) -> P::Event {
        match self.view.get() {
            Some(view) => P::Event::clone(view),
            None => self.record.authored(),
        }
    }

    pub(crate) fn authored_mut(&mut self) -> &mut P::Event {
        if self.view.get().is_none() {
            self.view = OnceLock::from(Arc::new(self.record.authored()));
        }
        Arc::make_mut(self.view.get_mut().expect("the view is set above"))
    }
}

impl DeliveredRecord<ChainPayload> {
    pub fn composite_activations(&self) -> &[CompositeActivationContext] {
        match self.view.get() {
            Some(event) => event.composite_activations(),
            None => self.record.composite_activations(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::causal_fixture::committed_input;
    use obzenflow_core::event::{ChainEventFactory, JournalClock};
    use obzenflow_core::id::CompositeId;
    use obzenflow_core::{JournalWriterId, StageId};
    use serde_json::json;
    use std::slice;

    #[test]
    fn dispatch_enrichment_preserves_original_committed_evidence() {
        let event = ChainEventFactory::data_event(
            StageId::new().into(),
            "input",
            std::num::NonZeroU32::MIN,
            json!({}),
        );
        let record = committed_input(JournalWriterId::new(), event);
        let original = serde_json::to_value(&record).unwrap();
        let commitment = JournalClock::from_record(&record).unwrap();
        let mut delivered = DeliveredRecord::from(record);
        let activation = CompositeActivationContext::new(
            CompositeId::new("composite"),
            *delivered.id(),
            "in",
            7,
        );
        delivered
            .authored_mut()
            .try_extend_composite_activations(slice::from_ref(&activation))
            .unwrap();
        assert_eq!(
            delivered.authored().composite_activations(),
            slice::from_ref(&activation)
        );

        let deferred = delivered.clone();
        assert!(Arc::ptr_eq(&deferred.record, &delivered.record));
        assert!(Arc::ptr_eq(deferred.view(), delivered.view()));
        assert_eq!(serde_json::to_value(deferred.record()).unwrap(), original);
        let deferred_commitment = JournalClock::from_record(deferred.record()).unwrap();
        assert_eq!(deferred_commitment.reference, commitment.reference);
        assert_eq!(deferred_commitment.clock, commitment.clock);
        assert!(deferred.record().composite_activations().is_empty());

        let mut rerouted = deferred.clone();
        let second =
            CompositeActivationContext::new(CompositeId::new("second"), *rerouted.id(), "in", 8);
        rerouted
            .authored_mut()
            .try_extend_composite_activations(slice::from_ref(&second))
            .unwrap();
        assert!(!Arc::ptr_eq(rerouted.view(), deferred.view()));
        assert!(Arc::ptr_eq(&rerouted.record, &deferred.record));
        assert_eq!(
            deferred.view().composite_activations(),
            slice::from_ref(&activation)
        );
        assert_eq!(
            delivered.composite_activations(),
            slice::from_ref(&activation)
        );
        assert_eq!(rerouted.composite_activations().len(), 2);
        assert!(rerouted.record().composite_activations().is_empty());
    }
}
