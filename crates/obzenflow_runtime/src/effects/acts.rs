// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::ReaderGeneration;
use std::collections::HashSet;

pub(crate) use obzenflow_core::event::payloads::effect_payload::SETTLEMENT_EFFECT_TYPE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArchivedAct {
    Effect {
        cursor: EffectCursor,
        effect_type: String,
        in_doubt: bool,
    },
    Fact {
        id: EventId,
        event_type: String,
    },
    Settled {
        cursor: EffectCursor,
    },
}

impl ArchivedAct {
    pub(crate) fn recorded(cursor: EffectCursor, effect_type: &str) -> Self {
        if effect_type == SETTLEMENT_EFFECT_TYPE {
            Self::Settled { cursor }
        } else {
            Self::Effect {
                cursor,
                effect_type: effect_type.to_string(),
                in_doubt: false,
            }
        }
    }

    pub(crate) fn key(&self) -> ActKey {
        match self {
            Self::Effect { cursor, .. } => ActKey::Effect(cursor.clone()),
            Self::Fact { id, .. } => ActKey::Fact(*id),
            Self::Settled { cursor } => ActKey::Settled(cursor.clone()),
        }
    }

    pub(crate) fn cursor(&self) -> Option<&EffectCursor> {
        match self {
            Self::Effect { cursor, .. } | Self::Settled { cursor } => Some(cursor),
            Self::Fact { .. } => None,
        }
    }

    pub(crate) fn is_fact(&self) -> bool {
        matches!(self, Self::Fact { .. })
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Effect { effect_type, .. } => {
                format!("a {} effect", display_name(effect_type))
            }
            Self::Fact { event_type, .. } => format!("a {} fact", display_name(event_type)),
            Self::Settled { cursor } => {
                format!("input {}, settled without an act", cursor.input_seq.get())
            }
        }
    }

    pub(crate) fn skipped_detail(&self, stage_key: &str) -> String {
        match self {
            Self::Settled { cursor } => format!(
                "stage '{stage_key}' skipped input {}, which its recording settled without an act",
                cursor.input_seq.get()
            ),
            act => format!(
                "stage '{stage_key}' skipped {} its recording holds",
                act.describe()
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ActKey {
    Effect(EffectCursor),
    Fact(EventId),
    Settled(EffectCursor),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ActRow {
    Effect {
        cursor: EffectCursor,
        attempt: Option<EffectAttemptOrdinal>,
    },
    Fact(EventId),
}

pub(crate) fn display_name(type_name: &str) -> String {
    type_name
        .rsplit('.')
        .next()
        .unwrap_or(type_name)
        .split('_')
        .filter(|word| !word.is_empty())
        .map(|word| {
            let mut chars = word.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_default()
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActCheck {
    Reproduce,
    Live,
    Missing { next: ArchivedAct },
    Skipped { skipped: ArchivedAct },
}

#[derive(Debug, Default)]
pub(crate) struct StageActs {
    acts: Vec<ArchivedAct>,
    facts: HashSet<EventId>,
    anchors: HashMap<ActRow, Vec<ReaderGeneration>>,
    next: Mutex<usize>,
}

impl StageActs {
    pub(crate) fn new(
        acts: Vec<ArchivedAct>,
        anchors: HashMap<ActRow, Vec<ReaderGeneration>>,
    ) -> Self {
        let facts = acts
            .iter()
            .filter_map(|act| match act {
                ArchivedAct::Fact { id, .. } => Some(*id),
                ArchivedAct::Effect { .. } | ArchivedAct::Settled { .. } => None,
            })
            .collect();
        Self {
            acts,
            facts,
            anchors,
            next: Mutex::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn acts(&self) -> &[ArchivedAct] {
        &self.acts
    }

    pub(crate) fn holds_fact(&self, id: &EventId) -> bool {
        self.facts.contains(id)
    }

    pub(crate) fn crossings_before(&self, row: &ActRow) -> Vec<ReaderGeneration> {
        let mut generations = self.anchors.get(row).cloned().unwrap_or_default();
        generations.sort();
        generations
    }

    fn position(&self) -> usize {
        *self
            .next
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn remaining(&self) -> Vec<ArchivedAct> {
        self.acts[self.position().min(self.acts.len())..].to_vec()
    }

    pub(crate) fn reproducing(&self, next_effect: &EffectCursor) -> bool {
        match self.remaining().as_slice() {
            [] => false,
            [ArchivedAct::Effect {
                cursor,
                in_doubt: true,
                ..
            }] => cursor != next_effect,
            _ => true,
        }
    }

    pub(crate) fn take(&self, offered: &ActKey) -> ActCheck {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(expected) = self.acts.get(*next) else {
            return ActCheck::Live;
        };
        if &expected.key() == offered {
            *next += 1;
            return ActCheck::Reproduce;
        }
        if self.acts[*next..].iter().any(|act| &act.key() == offered) {
            ActCheck::Skipped {
                skipped: expected.clone(),
            }
        } else {
            ActCheck::Missing {
                next: expected.clone(),
            }
        }
    }

    pub(crate) fn joined_with(&self, newer: &Self, stage_key: &str) -> Result<Self, EffectError> {
        let (shorter, longer) = if self.acts.len() <= newer.acts.len() {
            (&self.acts, &newer.acts)
        } else {
            (&newer.acts, &self.acts)
        };
        if let Some(position) = shorter
            .iter()
            .zip(longer.iter())
            .position(|(left, right)| left.key() != right.key())
        {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "the archives this resume merges disagree on stage '{stage_key}''s act {position}"
            )));
        }
        let mut anchors = self.anchors.clone();
        for (row, generations) in &newer.anchors {
            let entry = anchors.entry(row.clone()).or_default();
            for generation in generations {
                if !entry.contains(generation) {
                    entry.push(*generation);
                }
            }
        }
        Ok(Self::new(longer.clone(), anchors))
    }

    pub(crate) fn with_in_doubt(self, in_doubt: impl Fn(&EffectCursor) -> bool) -> Self {
        let acts = self
            .acts
            .into_iter()
            .map(|act| match act {
                ArchivedAct::Effect {
                    cursor,
                    effect_type,
                    ..
                } => ArchivedAct::Effect {
                    in_doubt: in_doubt(&cursor),
                    cursor,
                    effect_type,
                },
                fact => fact,
            })
            .collect();
        Self::new(acts, self.anchors)
    }
}

#[derive(Debug, Default)]
pub(crate) struct ActScan {
    acts: Vec<ArchivedAct>,
    seen: HashSet<EffectCursor>,
    pending: Vec<ReaderGeneration>,
    anchors: HashMap<ActRow, Vec<ReaderGeneration>>,
}

impl ActScan {
    pub(crate) fn observe(&mut self, event: &ChainEvent) -> Result<(), EffectError> {
        use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
        if let ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
            generation, ..
        }) = &event.payload
        {
            self.pending.push(*generation);
            return Ok(());
        }
        let row = if EffectAttemptStarted::matches_event_type(&event.event_type()) {
            let started = EffectAttemptStarted::try_from_event(event)
                .map_err(|error| EffectError::Serialization(error.to_string()))?;
            self.effect(started.cursor.clone(), started.effect_type.as_str());
            Some(ActRow::Effect {
                cursor: started.cursor,
                attempt: Some(started.attempt),
            })
        } else if let Some(provenance) = &event.effect_provenance {
            self.effect(
                provenance.cursor.clone(),
                provenance.descriptor.effect_type.as_str(),
            );
            Some(ActRow::Effect {
                cursor: provenance.cursor.clone(),
                attempt: provenance.attempt,
            })
        } else if event.consumes_data_credit() {
            self.acts.push(ArchivedAct::Fact {
                id: event.id,
                event_type: event.event_type(),
            });
            Some(ActRow::Fact(event.id))
        } else {
            None
        };
        if let Some(row) = row {
            if !self.pending.is_empty() {
                self.anchors
                    .entry(row)
                    .or_default()
                    .append(&mut self.pending);
            }
        }
        Ok(())
    }

    fn effect(&mut self, cursor: EffectCursor, effect_type: &str) {
        if self.seen.insert(cursor.clone()) {
            self.acts.push(ArchivedAct::recorded(cursor, effect_type));
        }
    }

    pub(crate) fn finish(
        self,
        holds: impl Fn(&EffectCursor) -> bool,
        in_doubt: impl Fn(&EffectCursor) -> bool,
    ) -> StageActs {
        let acts = self
            .acts
            .into_iter()
            .filter(|act| act.cursor().is_none_or(&holds))
            .collect();
        StageActs::new(acts, self.anchors).with_in_doubt(in_doubt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(input: u64, ordinal: u32) -> EffectCursor {
        EffectCursor::new(
            "flow_k",
            "allocate_stock",
            input,
            EffectOrdinal::new(ordinal),
        )
    }

    fn effect(input: u64, in_doubt: bool) -> ArchivedAct {
        ArchivedAct::Effect {
            cursor: cursor(input, 0),
            effect_type: "allocation.reserve_stock".to_string(),
            in_doubt,
        }
    }

    fn fact(id: EventId) -> ArchivedAct {
        ArchivedAct::Fact {
            id,
            event_type: "allocation.sold_out".to_string(),
        }
    }

    #[test]
    fn display_names_read_as_types() {
        assert_eq!(display_name("allocation.sold_out"), "SoldOut");
        assert_eq!(display_name("allocation.reserve_stock"), "ReserveStock");
        assert_eq!(display_name("obzenflow.capture"), "Capture");
        assert_eq!(display_name("plain"), "Plain");
    }

    #[test]
    fn acts_are_taken_in_order_and_a_miss_runs_live_once_they_are_exhausted() {
        let sold_out = EventId::new();
        let acts = StageActs::new(vec![effect(1, false), fact(sold_out)], HashMap::new());
        assert!(acts.reproducing(&cursor(1, 0)));
        assert_eq!(
            acts.take(&ActKey::Effect(cursor(1, 0))),
            ActCheck::Reproduce
        );
        assert!(acts.reproducing(&cursor(2, 0)));
        assert_eq!(acts.take(&ActKey::Fact(sold_out)), ActCheck::Reproduce);
        assert!(!acts.reproducing(&cursor(3, 0)));
        assert_eq!(acts.take(&ActKey::Effect(cursor(3, 0))), ActCheck::Live);
    }

    #[test]
    fn an_act_the_archive_lacks_and_a_skipped_act_are_departures() {
        let sold_out = EventId::new();
        let acts = StageActs::new(vec![fact(sold_out), effect(2, false)], HashMap::new());
        assert_eq!(
            acts.take(&ActKey::Effect(cursor(7, 0))),
            ActCheck::Missing {
                next: fact(sold_out)
            }
        );
        assert_eq!(
            acts.take(&ActKey::Effect(cursor(2, 0))),
            ActCheck::Skipped {
                skipped: fact(sold_out)
            }
        );
        assert_eq!(fact(sold_out).describe(), "a SoldOut fact");
        assert_eq!(acts.take(&ActKey::Fact(sold_out)), ActCheck::Reproduce);
    }

    #[test]
    fn only_the_next_effects_in_doubt_attempt_left_is_live() {
        let acts = StageActs::new(vec![effect(4, true)], HashMap::new());
        assert!(!acts.reproducing(&cursor(4, 0)));
        assert!(acts.reproducing(&cursor(5, 0)));
        let settled = StageActs::new(vec![effect(4, false)], HashMap::new());
        assert!(settled.reproducing(&cursor(4, 0)));
    }

    #[test]
    fn a_merged_lineage_joins_by_identity_prefix_and_refuses_disagreement() {
        let sold_out = EventId::new();
        let older = StageActs::new(vec![effect(1, true)], HashMap::new());
        let newer = StageActs::new(vec![effect(1, false), fact(sold_out)], HashMap::new());
        let joined = older.joined_with(&newer, "allocate_stock").unwrap();
        assert_eq!(joined.acts().len(), 2);

        let other = StageActs::new(vec![fact(EventId::new())], HashMap::new());
        assert!(older.joined_with(&other, "allocate_stock").is_err());
    }

    #[test]
    fn a_settlement_is_an_act_apart_from_any_effect_at_its_cursor() {
        let settled = ArchivedAct::recorded(cursor(5, 0), SETTLEMENT_EFFECT_TYPE);
        assert_eq!(
            settled,
            ArchivedAct::Settled {
                cursor: cursor(5, 0)
            }
        );
        assert!(matches!(
            ArchivedAct::recorded(cursor(5, 0), "allocation.reserve_stock"),
            ArchivedAct::Effect { .. }
        ));

        let acts = StageActs::new(vec![effect(4, false), settled.clone()], HashMap::new());
        assert_eq!(
            acts.take(&ActKey::Effect(cursor(4, 0))),
            ActCheck::Reproduce
        );
        assert!(
            acts.reproducing(&cursor(5, 0)),
            "a settled input reproduces"
        );
        assert_eq!(
            acts.take(&ActKey::Effect(cursor(5, 0))),
            ActCheck::Missing {
                next: settled.clone()
            },
            "an effect where the archive settled the input departs"
        );
        assert_eq!(
            acts.take(&ActKey::Settled(cursor(5, 0))),
            ActCheck::Reproduce
        );
        assert_eq!(acts.take(&ActKey::Settled(cursor(6, 0))), ActCheck::Live);

        assert_eq!(
            settled.skipped_detail("allocate_stock"),
            "stage 'allocate_stock' skipped input 5, which its recording settled without an act"
        );
        assert_eq!(
            fact(EventId::new()).skipped_detail("allocate_stock"),
            "stage 'allocate_stock' skipped a SoldOut fact its recording holds"
        );
    }

    #[test]
    fn a_torn_settlement_is_not_an_archived_act() {
        let mut scan = ActScan::default();
        scan.effect(cursor(5, 0), SETTLEMENT_EFFECT_TYPE);
        let acts = scan.finish(|_| false, |_| false);
        assert!(acts.acts().is_empty());
    }
}
