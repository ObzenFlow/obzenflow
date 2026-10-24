// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::stages::common::handler_error::StageFatal;
use obzenflow_core::event::{Departure, DepartureAction, StageFatalCode, StageFatalReason};
use obzenflow_core::{EventDescriptor, EventType, ReaderGeneration, StageKey};
use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordedAction {
    Effect {
        cursor: EffectCursor,
        effect_type: EffectType,
        in_doubt: bool,
    },
    Fact {
        id: EventId,
        event_type: EventType,
    },
    Settlement {
        cursor: EffectCursor,
    },
}

impl RecordedAction {
    pub(crate) fn recorded(cursor: EffectCursor, effect_type: &EffectType) -> Self {
        if effect_type.is_settlement() {
            Self::Settlement { cursor }
        } else {
            Self::effect(cursor, effect_type.clone())
        }
    }

    pub(crate) fn effect(cursor: EffectCursor, effect_type: EffectType) -> Self {
        Self::Effect {
            cursor,
            effect_type,
            in_doubt: false,
        }
    }

    pub(crate) fn key(&self) -> ActionKey {
        match self {
            Self::Effect { cursor, .. } => ActionKey::Effect(cursor.clone()),
            Self::Fact { id, .. } => ActionKey::Fact(*id),
            Self::Settlement { cursor } => ActionKey::Settlement(cursor.clone()),
        }
    }

    pub(crate) fn cursor(&self) -> Option<&EffectCursor> {
        match self {
            Self::Effect { cursor, .. } | Self::Settlement { cursor } => Some(cursor),
            Self::Fact { .. } => None,
        }
    }

    pub(crate) fn is_fact(&self) -> bool {
        matches!(self, Self::Fact { .. })
    }

    pub(crate) fn record(&self) -> DepartureAction {
        match self {
            Self::Effect {
                cursor,
                effect_type,
                ..
            } => DepartureAction::Effect {
                effect_type: effect_type.clone(),
                input_position: cursor.input_seq,
                effect_ordinal: cursor.effect_ordinal,
            },
            Self::Fact { id, event_type } => DepartureAction::Fact {
                event_type: event_type.clone(),
                event_id: *id,
            },
            Self::Settlement { cursor } => DepartureAction::Settlement {
                input_position: cursor.input_seq,
            },
        }
    }
}

impl fmt::Display for RecordedAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Effect { effect_type, .. } => {
                write!(f, "a {} effect", display_name(effect_type.as_str()))
            }
            Self::Fact { event_type, .. } => {
                write!(f, "a {} fact", display_name(event_type.as_str()))
            }
            Self::Settlement { cursor } => {
                write!(f, "input {}, settled without an action", cursor.input_seq)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ActionKey {
    Effect(EffectCursor),
    Fact(EventId),
    Settlement(EffectCursor),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ActionRow {
    Effect {
        cursor: EffectCursor,
        attempt: Option<EffectAttemptOrdinal>,
    },
    Fact(EventId),
}

fn display_name(type_name: &str) -> String {
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
pub(crate) enum ActionCheck {
    Reproduce,
    Live,
    Unrecorded { expected: RecordedAction },
    Skipped { expected: RecordedAction },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DepartureKind {
    Unrecorded {
        input: EffectInputPosition,
        offered: RecordedAction,
        expected: RecordedAction,
    },
    Skipped {
        offered: Option<RecordedAction>,
        expected: RecordedAction,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActionDeparture {
    stage: StageKey,
    kind: DepartureKind,
}

impl ActionDeparture {
    pub(crate) fn unrecorded(
        stage: StageKey,
        input: EffectInputPosition,
        offered: RecordedAction,
        expected: RecordedAction,
    ) -> Self {
        if matches!(offered, RecordedAction::Settlement { .. }) {
            return Self::skipped(stage, Some(offered), expected);
        }
        Self {
            stage,
            kind: DepartureKind::Unrecorded {
                input,
                offered,
                expected,
            },
        }
    }

    pub(crate) fn skipped(
        stage: StageKey,
        offered: Option<RecordedAction>,
        expected: RecordedAction,
    ) -> Self {
        Self {
            stage,
            kind: DepartureKind::Skipped { offered, expected },
        }
    }

    pub(crate) fn reason(&self) -> StageFatalReason {
        match &self.kind {
            DepartureKind::Unrecorded { offered, .. } if offered.is_fact() => {
                StageFatalReason::MissingRecordedFact
            }
            DepartureKind::Unrecorded { .. } => StageFatalReason::MissingRecordedEffect,
            DepartureKind::Skipped { expected, .. } if expected.is_fact() => {
                StageFatalReason::UnusedFactHistory
            }
            DepartureKind::Skipped { .. } => StageFatalReason::UnusedEffectHistory,
        }
    }

    pub(crate) fn record(&self) -> Departure {
        match &self.kind {
            DepartureKind::Unrecorded {
                offered, expected, ..
            } => Departure {
                offered: Some(offered.record()),
                expected: expected.record(),
            },
            DepartureKind::Skipped { offered, expected } => Departure {
                offered: offered.as_ref().map(RecordedAction::record),
                expected: expected.record(),
            },
        }
    }

    pub(crate) fn stage_fatal(&self) -> StageFatal {
        StageFatal::new(StageFatalCode::Replay, self.reason(), self.to_string())
            .with_departure(self.record())
    }
}

fn write_offered(
    f: &mut fmt::Formatter<'_>,
    input: EffectInputPosition,
    offered: &RecordedAction,
) -> fmt::Result {
    match offered {
        RecordedAction::Effect { effect_type, .. } => write!(
            f,
            "would perform {} at input {input}",
            display_name(effect_type.as_str())
        ),
        RecordedAction::Fact { event_type, .. } => write!(
            f,
            "emitted {} at input {input}",
            display_name(event_type.as_str())
        ),
        RecordedAction::Settlement { .. } => {
            write!(f, "settled input {input} without an action")
        }
    }
}

impl fmt::Display for ActionDeparture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stage '{}' ", self.stage.as_str())?;
        match &self.kind {
            DepartureKind::Unrecorded {
                input,
                offered,
                expected,
            } => {
                write_offered(f, *input, offered)?;
                match expected {
                    RecordedAction::Settlement { cursor } if cursor.input_seq == *input => {
                        f.write_str(", but its recording settled that input without an action")
                    }
                    expected => write!(f, ", but its recording's next action is {expected}"),
                }
            }
            DepartureKind::Skipped {
                expected: RecordedAction::Settlement { cursor },
                ..
            } => write!(
                f,
                "skipped input {}, which its recording settled without an action",
                cursor.input_seq
            ),
            DepartureKind::Skipped { expected, .. } => {
                write!(f, "skipped {expected} its recording holds")
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RecordedActions {
    actions: Vec<RecordedAction>,
    facts: HashSet<EventId>,
    anchors: HashMap<ActionRow, ReaderGeneration>,
    next: Mutex<usize>,
}

impl RecordedActions {
    pub(crate) fn new(
        actions: Vec<RecordedAction>,
        anchors: HashMap<ActionRow, ReaderGeneration>,
    ) -> Self {
        let facts = actions
            .iter()
            .filter_map(|action| match action {
                RecordedAction::Fact { id, .. } => Some(*id),
                RecordedAction::Effect { .. } | RecordedAction::Settlement { .. } => None,
            })
            .collect();
        Self {
            actions,
            facts,
            anchors,
            next: Mutex::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn actions(&self) -> &[RecordedAction] {
        &self.actions
    }

    pub(crate) fn holds_fact(&self, id: &EventId) -> bool {
        self.facts.contains(id)
    }

    pub(crate) fn crossing_before(&self, row: &ActionRow) -> Option<ReaderGeneration> {
        self.anchors.get(row).copied()
    }

    fn position(&self) -> usize {
        *self
            .next
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn next_unreproduced(&self) -> Option<RecordedAction> {
        self.actions.get(self.position()).cloned()
    }

    pub(crate) fn reproducing(&self, next_effect: &EffectCursor) -> bool {
        match self.actions.get(self.position().min(self.actions.len())..) {
            None | Some([]) => false,
            Some(
                [RecordedAction::Effect {
                    cursor,
                    in_doubt: true,
                    ..
                }],
            ) => cursor != next_effect,
            Some(_) => true,
        }
    }

    pub(crate) fn take(&self, offered: &ActionKey) -> ActionCheck {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(expected) = self.actions.get(*next) else {
            return ActionCheck::Live;
        };
        if &expected.key() == offered {
            *next += 1;
            return ActionCheck::Reproduce;
        }
        let expected = expected.clone();
        if self.actions[*next..]
            .iter()
            .any(|action| &action.key() == offered)
        {
            ActionCheck::Skipped { expected }
        } else {
            ActionCheck::Unrecorded { expected }
        }
    }

    pub(crate) fn joined_with(&self, newer: &Self, stage: &StageKey) -> Result<Self, EffectError> {
        let (shorter, longer) = if self.actions.len() <= newer.actions.len() {
            (&self.actions, &newer.actions)
        } else {
            (&newer.actions, &self.actions)
        };
        if let Some(position) = shorter
            .iter()
            .zip(longer.iter())
            .position(|(left, right)| left.key() != right.key())
        {
            return Err(EffectError::EffectProvenanceMismatch(format!(
                "the archives this resume merges disagree on stage '{}''s action {position}",
                stage.as_str()
            )));
        }
        let mut anchors = self.anchors.clone();
        for (row, generation) in &newer.anchors {
            let entry = anchors.entry(row.clone()).or_insert(*generation);
            *entry = (*entry).max(*generation);
        }
        Ok(Self::new(longer.clone(), anchors))
    }

    pub(crate) fn with_in_doubt(self, in_doubt: impl Fn(&EffectCursor) -> bool) -> Self {
        let actions = self
            .actions
            .into_iter()
            .map(|action| match action {
                RecordedAction::Effect {
                    cursor,
                    effect_type,
                    ..
                } => RecordedAction::Effect {
                    in_doubt: in_doubt(&cursor),
                    cursor,
                    effect_type,
                },
                other => other,
            })
            .collect();
        Self::new(actions, self.anchors)
    }
}

#[derive(Debug, Default)]
pub(crate) struct ActionScan {
    actions: Vec<RecordedAction>,
    seen: HashSet<EffectCursor>,
    pending: Option<ReaderGeneration>,
    anchors: HashMap<ActionRow, ReaderGeneration>,
    pass_through: Option<EventDescriptor>,
}

impl ActionScan {
    pub(crate) fn passing_through(pass_through: Option<EventDescriptor>) -> Self {
        Self {
            pass_through,
            ..Self::default()
        }
    }

    pub(crate) fn observe(&mut self, event: &ChainEvent) -> Result<(), EffectError> {
        use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
        if let ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
            generation, ..
        }) = &event.payload
        {
            self.pending = Some(
                self.pending
                    .map_or(*generation, |held| held.max(*generation)),
            );
            return Ok(());
        }
        if self
            .pass_through
            .as_ref()
            .is_some_and(|descriptor| *descriptor == event.descriptor())
        {
            return Ok(());
        }
        let row = if EffectAttemptStarted::matches_event_type(&event.event_type()) {
            let started = EffectAttemptStarted::try_from_event(event)
                .map_err(|error| EffectError::Serialization(error.to_string()))?;
            self.effect(started.cursor.clone(), &started.effect_type);
            Some(ActionRow::Effect {
                cursor: started.cursor,
                attempt: Some(started.attempt),
            })
        } else if let Some(provenance) = &event.effect_provenance {
            self.effect(
                provenance.cursor.clone(),
                &provenance.descriptor.effect_type,
            );
            Some(ActionRow::Effect {
                cursor: provenance.cursor.clone(),
                attempt: provenance.attempt,
            })
        } else if event.consumes_data_credit() {
            self.actions.push(RecordedAction::Fact {
                id: event.id,
                event_type: EventType::from(event.event_type()),
            });
            Some(ActionRow::Fact(event.id))
        } else {
            None
        };
        if let (Some(row), Some(generation)) = (row, self.pending.take()) {
            let entry = self.anchors.entry(row).or_insert(generation);
            *entry = (*entry).max(generation);
        }
        Ok(())
    }

    fn effect(&mut self, cursor: EffectCursor, effect_type: &EffectType) {
        if self.seen.insert(cursor.clone()) {
            self.actions
                .push(RecordedAction::recorded(cursor, effect_type));
        }
    }

    pub(crate) fn finish(
        self,
        holds: impl Fn(&EffectCursor) -> bool,
        in_doubt: impl Fn(&EffectCursor) -> bool,
    ) -> RecordedActions {
        let actions = self
            .actions
            .into_iter()
            .filter(|action| action.cursor().is_none_or(&holds))
            .collect();
        RecordedActions::new(actions, self.anchors).with_in_doubt(in_doubt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::effect_payload::SETTLEMENT_EFFECT_TYPE;

    fn cursor(input: u64, ordinal: u32) -> EffectCursor {
        EffectCursor::new(
            "flow_k",
            "allocate_stock",
            input,
            EffectOrdinal::new(ordinal),
        )
    }

    fn effect(input: u64, in_doubt: bool) -> RecordedAction {
        RecordedAction::Effect {
            cursor: cursor(input, 0),
            effect_type: EffectType::from("allocation.reserve_stock"),
            in_doubt,
        }
    }

    fn fact(id: EventId) -> RecordedAction {
        RecordedAction::Fact {
            id,
            event_type: EventType::from("allocation.sold_out"),
        }
    }

    fn stage() -> StageKey {
        StageKey::from("allocate_stock")
    }

    fn departure(input: u64, offered: RecordedAction, check: ActionCheck) -> ActionDeparture {
        match check {
            ActionCheck::Unrecorded { expected } => ActionDeparture::unrecorded(
                stage(),
                EffectInputPosition::new(input),
                offered,
                expected,
            ),
            ActionCheck::Skipped { expected } => {
                ActionDeparture::skipped(stage(), Some(offered), expected)
            }
            ActionCheck::Reproduce | ActionCheck::Live => panic!("not a departure"),
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
    fn actions_are_taken_in_order_and_a_miss_runs_live_once_they_are_exhausted() {
        let sold_out = EventId::new();
        let actions = RecordedActions::new(vec![effect(1, false), fact(sold_out)], HashMap::new());
        assert!(actions.reproducing(&cursor(1, 0)));
        assert_eq!(
            actions.take(&ActionKey::Effect(cursor(1, 0))),
            ActionCheck::Reproduce
        );
        assert!(actions.reproducing(&cursor(2, 0)));
        assert_eq!(
            actions.take(&ActionKey::Fact(sold_out)),
            ActionCheck::Reproduce
        );
        assert!(!actions.reproducing(&cursor(3, 0)));
        assert_eq!(actions.next_unreproduced(), None);
        assert_eq!(
            actions.take(&ActionKey::Effect(cursor(3, 0))),
            ActionCheck::Live
        );
    }

    #[test]
    fn an_action_the_archive_lacks_and_a_skipped_action_are_departures() {
        let sold_out = EventId::new();
        let actions = RecordedActions::new(vec![fact(sold_out), effect(2, false)], HashMap::new());
        assert_eq!(
            actions.take(&ActionKey::Effect(cursor(7, 0))),
            ActionCheck::Unrecorded {
                expected: fact(sold_out)
            }
        );
        assert_eq!(
            actions.take(&ActionKey::Effect(cursor(2, 0))),
            ActionCheck::Skipped {
                expected: fact(sold_out)
            }
        );
        assert_eq!(fact(sold_out).to_string(), "a SoldOut fact");
        assert_eq!(
            actions.take(&ActionKey::Fact(sold_out)),
            ActionCheck::Reproduce
        );
    }

    #[test]
    fn only_the_next_effects_in_doubt_attempt_left_is_live() {
        let actions = RecordedActions::new(vec![effect(4, true)], HashMap::new());
        assert!(!actions.reproducing(&cursor(4, 0)));
        assert!(actions.reproducing(&cursor(5, 0)));
        let settled = RecordedActions::new(vec![effect(4, false)], HashMap::new());
        assert!(settled.reproducing(&cursor(4, 0)));
    }

    #[test]
    fn a_merged_lineage_joins_by_identity_prefix_and_refuses_disagreement() {
        let sold_out = EventId::new();
        let older = RecordedActions::new(vec![effect(1, true)], HashMap::new());
        let newer = RecordedActions::new(vec![effect(1, false), fact(sold_out)], HashMap::new());
        let joined = older.joined_with(&newer, &stage()).unwrap();
        assert_eq!(joined.actions().len(), 2);

        let other = RecordedActions::new(vec![fact(EventId::new())], HashMap::new());
        let refused = older.joined_with(&other, &stage()).unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("disagree on stage 'allocate_stock''s action 0"),
            "{refused}"
        );
    }

    #[test]
    fn a_settlement_is_an_action_apart_from_any_effect_at_its_cursor() {
        let settled =
            RecordedAction::recorded(cursor(5, 0), &EffectType::from(SETTLEMENT_EFFECT_TYPE));
        assert_eq!(
            settled,
            RecordedAction::Settlement {
                cursor: cursor(5, 0)
            }
        );
        assert!(matches!(
            RecordedAction::recorded(cursor(5, 0), &EffectType::from("allocation.reserve_stock")),
            RecordedAction::Effect { .. }
        ));

        let actions = RecordedActions::new(vec![effect(4, false), settled.clone()], HashMap::new());
        assert_eq!(
            actions.take(&ActionKey::Effect(cursor(4, 0))),
            ActionCheck::Reproduce
        );
        assert!(
            actions.reproducing(&cursor(5, 0)),
            "a settled input reproduces"
        );
        assert_eq!(
            actions.take(&ActionKey::Effect(cursor(5, 0))),
            ActionCheck::Unrecorded {
                expected: settled.clone()
            },
            "an effect where the archive settled the input departs"
        );
        assert_eq!(
            actions.take(&ActionKey::Settlement(cursor(5, 0))),
            ActionCheck::Reproduce
        );
        assert_eq!(
            actions.take(&ActionKey::Settlement(cursor(6, 0))),
            ActionCheck::Live
        );
    }

    #[test]
    fn each_departure_reads_as_its_approved_line_and_records_its_reason() {
        let sold_out = EventId::new();
        let settled = RecordedAction::Settlement {
            cursor: cursor(5, 0),
        };
        let cancel = RecordedAction::Fact {
            id: EventId::new(),
            event_type: EventType::from("allocation.cancel_ignored"),
        };

        let on_settled = departure(
            5,
            cancel.clone(),
            ActionCheck::Unrecorded {
                expected: settled.clone(),
            },
        );
        assert_eq!(
            on_settled.to_string(),
            "stage 'allocate_stock' emitted CancelIgnored at input 5, but its recording \
             settled that input without an action"
        );
        assert_eq!(on_settled.reason(), StageFatalReason::MissingRecordedFact);

        let before_fact = departure(
            7,
            RecordedAction::effect(cursor(7, 0), EffectType::from("allocation.reserve_stock")),
            ActionCheck::Unrecorded {
                expected: fact(sold_out),
            },
        );
        assert_eq!(
            before_fact.to_string(),
            "stage 'allocate_stock' would perform ReserveStock at input 7, but its recording's \
             next action is a SoldOut fact"
        );
        assert_eq!(
            before_fact.reason(),
            StageFatalReason::MissingRecordedEffect
        );

        let skipped_fact = departure(
            5,
            RecordedAction::effect(cursor(5, 0), EffectType::from("allocation.reserve_stock")),
            ActionCheck::Skipped {
                expected: fact(sold_out),
            },
        );
        assert_eq!(
            skipped_fact.to_string(),
            "stage 'allocate_stock' skipped a SoldOut fact its recording holds"
        );
        assert_eq!(skipped_fact.reason(), StageFatalReason::UnusedFactHistory);

        let settled_instead = departure(
            5,
            settled.clone(),
            ActionCheck::Unrecorded {
                expected: effect(5, false),
            },
        );
        assert_eq!(
            settled_instead.to_string(),
            "stage 'allocate_stock' skipped a ReserveStock effect its recording holds"
        );
        assert_eq!(
            settled_instead.reason(),
            StageFatalReason::UnusedEffectHistory
        );

        let ended = ActionDeparture::skipped(stage(), None, settled);
        assert_eq!(
            ended.to_string(),
            "stage 'allocate_stock' skipped input 5, which its recording settled without an action"
        );
        assert_eq!(ended.record().offered, None);
    }

    #[test]
    fn a_departure_records_the_offered_and_expected_actions_by_kind() {
        let cancel_id = EventId::new();
        let recorded = departure(
            5,
            RecordedAction::Fact {
                id: cancel_id,
                event_type: EventType::from("allocation.cancel_ignored"),
            },
            ActionCheck::Unrecorded {
                expected: RecordedAction::Settlement {
                    cursor: cursor(5, 0),
                },
            },
        )
        .record();
        assert_eq!(
            recorded.offered,
            Some(DepartureAction::Fact {
                event_type: EventType::from("allocation.cancel_ignored"),
                event_id: cancel_id,
            })
        );
        assert_eq!(
            recorded.expected,
            DepartureAction::Settlement {
                input_position: EffectInputPosition::new(5),
            }
        );

        let fatal = departure(
            7,
            RecordedAction::effect(cursor(7, 1), EffectType::from("allocation.reserve_stock")),
            ActionCheck::Unrecorded {
                expected: fact(EventId::new()),
            },
        )
        .stage_fatal();
        assert_eq!(fatal.reason, StageFatalReason::MissingRecordedEffect);
        assert_eq!(
            fatal.departure.and_then(|departure| departure.offered),
            Some(DepartureAction::Effect {
                effect_type: EffectType::from("allocation.reserve_stock"),
                input_position: EffectInputPosition::new(7),
                effect_ordinal: EffectOrdinal::new(1),
            })
        );
    }

    #[test]
    fn a_torn_settlement_is_not_a_recorded_action() {
        let mut scan = ActionScan::default();
        scan.effect(cursor(5, 0), &EffectType::from(SETTLEMENT_EFFECT_TYPE));
        let actions = scan.finish(|_| false, |_| false);
        assert!(actions.actions().is_empty());
    }

    #[test]
    fn a_row_of_the_pass_through_type_names_no_action_and_takes_no_anchor() {
        use obzenflow_core::event::ChainEventFactory;
        let writer = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
        let row = |event_type: &str| {
            ChainEventFactory::data_event(
                writer,
                event_type,
                std::num::NonZeroU32::MIN,
                serde_json::json!({}),
            )
        };
        let manifest = row("map_reduce.grouping_ended");
        let partial = row("map_reduce.partial");
        let marker =
            ChainEventFactory::catch_up_complete_event(writer, ReaderGeneration(1), stage());

        let mut scan = ActionScan::passing_through(Some(manifest.descriptor()));
        scan.observe(&marker).unwrap();
        scan.observe(&manifest).unwrap();
        scan.observe(&partial).unwrap();
        let actions = scan.finish(|_| true, |_| false);

        assert_eq!(
            actions.actions(),
            [RecordedAction::Fact {
                id: partial.id,
                event_type: EventType::from("map_reduce.partial"),
            }]
        );
        assert_eq!(
            actions.crossing_before(&ActionRow::Fact(partial.id)),
            Some(ReaderGeneration(1))
        );
        assert_eq!(actions.crossing_before(&ActionRow::Fact(manifest.id)), None);
    }
}
