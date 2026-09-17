// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::observability::families::observation_families;
use obzenflow_core::event::observability::{CaptureSeq, ObservabilityContext};
use obzenflow_core::journal::{JournalError, LocatedObservation, ObservationKey};
use std::collections::{HashMap, VecDeque};

pub(super) const MAX_KEYS: usize = 4096;
pub(super) const HISTORY_PER_KEY: usize = 4;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(super) struct Locator {
    pub position: u64,
    pub capture_seq: CaptureSeq,
    pub frame_offset: u64,
    pub member: usize,
}

/// Disposable locators only. Packets remain in their committed carriers.
#[derive(Default, Clone)]
pub(super) struct ObservationIndex {
    pub examined_through: u64,
    pub entries: HashMap<ObservationKey, VecDeque<Locator>>,
}

pub(super) fn unavailable(message: impl Into<String>) -> JournalError {
    JournalError::Implementation {
        message: format!("Optional observation lookup: {}", message.into()),
        source: "observation index unavailable".into(),
    }
}

impl ObservationIndex {
    pub fn observe(
        &mut self,
        observation: Option<&ObservabilityContext>,
        frame_offset: u64,
        member: usize,
    ) -> Result<(), JournalError> {
        if let Some(packet) = observation {
            for (kind, packet) in observation_families(packet.clone())
                .ok_or_else(|| unavailable("too many attachment families"))?
            {
                let key = ObservationKey {
                    capture_scope: packet.capture.capture_scope,
                    observer: packet.capture.observer,
                    kind,
                };
                if self.entries.len() >= MAX_KEYS && !self.entries.contains_key(&key) {
                    return Err(unavailable("attachment key capacity reached"));
                }
                let history = self.entries.entry(key).or_default();
                if history
                    .back()
                    .is_none_or(|previous| previous.capture_seq < packet.capture.capture_seq)
                {
                    history.push_back(Locator {
                        position: self.examined_through,
                        capture_seq: packet.capture.capture_seq,
                        frame_offset,
                        member,
                    });
                    while history.len() > HISTORY_PER_KEY {
                        history.pop_front();
                    }
                }
            }
        }
        self.examined_through += 1;
        Ok(())
    }
}

pub(super) fn locate(
    key: &ObservationKey,
    locator: &Locator,
    packet: ObservabilityContext,
) -> Result<LocatedObservation, JournalError> {
    let observation = observation_families(packet)
        .into_iter()
        .flatten()
        .find_map(|(kind, packet)| {
            (kind == key.kind
                && packet.capture.capture_scope == key.capture_scope
                && packet.capture.observer == key.observer
                && packet.capture.capture_seq == locator.capture_seq)
                .then_some(packet)
        })
        .ok_or_else(|| unavailable("attachment locator no longer matches its carrier"))?;
    Ok(LocatedObservation {
        position: locator.position,
        observation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::observability::tests::event;
    use obzenflow_core::event::observability::families::ObservationKind;
    use obzenflow_core::StageId;

    #[test]
    fn cleanup_retains_the_latest_capture_and_ignores_stale_copies() {
        let mut packet = event(StageId::new(), 1).envelope.observability.unwrap();
        let key = ObservationKey {
            capture_scope: packet.capture.capture_scope,
            observer: packet.capture.observer,
            kind: ObservationKind::InFlight,
        };
        let mut index = ObservationIndex::default();
        for seq in 1..=20 {
            packet.capture.capture_seq = CaptureSeq(seq);
            index.observe(Some(&packet), seq * 100, 0).unwrap();
            assert!(index.entries[&key].len() <= HISTORY_PER_KEY);
        }
        packet.capture.capture_seq = CaptureSeq(1);
        index.observe(Some(&packet), 2100, 0).unwrap();
        index.observe(None, 2200, 0).unwrap();
        let latest = index.entries[&key].back().unwrap();
        assert_eq!(latest.capture_seq, CaptureSeq(20));
        assert_eq!(latest.position, 19);
        assert_eq!(index.examined_through, 22);
    }
}
