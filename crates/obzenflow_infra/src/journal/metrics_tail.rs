// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One disposable locator per live reporting key. No prefix, history, rebuild,
//! or checkpoint. A newly opened archive has no live values until new commits.

use obzenflow_core::event::observability::families::any_observation_family;
use obzenflow_core::event::observability::CaptureSeq;
use obzenflow_core::event::payloads::JournalPayload;
use obzenflow_core::journal::metrics_tail::MetricsTailKey;
use obzenflow_core::journal::ObservationKey;
use obzenflow_core::JournalRecord;
use std::collections::{BTreeSet, HashMap};

const MAX_OBSERVATION_KEYS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Carrier {
    pub offset: u64,
    pub member: usize,
}

#[derive(Default)]
pub(super) struct MetricsTailIndex {
    reporting_carriers: HashMap<MetricsTailKey, Carrier>,
    observation_carriers: HashMap<ObservationKey, (CaptureSeq, Carrier)>,
}

impl MetricsTailIndex {
    pub fn observe<P: JournalPayload>(&mut self, record: &JournalRecord<P>, carrier: Carrier) {
        record
            .payload
            .visit_metrics_keys(&record.envelope.provenance.event, &mut |key| {
                // Reporting identities follow the flow's stages and contract edges.
                // In particular, optional-family capacity cannot hide its terminal.
                self.reporting_carriers.insert(key, carrier);
            });
        if let Some(packet) = &record.envelope.observability {
            any_observation_family(packet, |kind, stamp| {
                let key = ObservationKey {
                    capture_scope: stamp.capture_scope,
                    observer: stamp.observer,
                    kind,
                };
                if let Some((sequence, previous)) = self.observation_carriers.get_mut(&key) {
                    if stamp.capture_seq > *sequence {
                        *sequence = stamp.capture_seq;
                        *previous = carrier;
                    }
                } else if self.observation_carriers.len() < MAX_OBSERVATION_KEYS {
                    self.observation_carriers
                        .insert(key, (stamp.capture_seq, carrier));
                }
                false
            });
        }
    }

    pub fn carriers(&self) -> Vec<Carrier> {
        self.reporting_carriers
            .values()
            .copied()
            .chain(
                self.observation_carriers
                    .values()
                    .map(|(_, carrier)| *carrier),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}
