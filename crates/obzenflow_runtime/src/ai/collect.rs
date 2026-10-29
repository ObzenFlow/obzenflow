// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::stateful::traits::StatefulHandler;
use crate::stages::common::handlers::{StatefulTerminationKind, TerminalValidation};
use crate::stages::stateful::strategies::accumulators::trace::TraceState;
use async_trait::async_trait;
use obzenflow_core::ai::{
    canonical_json_bytes_v2, AiMapReduceChunkFailed, AiMapReduceGroupFailure,
    AiMapReduceGroupingEnded, AiMapReduceReduceInput, AiMapReduceRoleFailure,
    AiMapReduceTaggedPartial, AiMapReduceWindowFailed, GroupOrdinal, ReduceTrigger, WindowOrdinal,
};
use obzenflow_core::event::schema::TypedFact;
use obzenflow_core::event::{ChainPayload, StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU64;

const MAX_FACT_BYTES: usize = 4 * 1024 * 1024;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const MAX_SLOTS_PER_WINDOW: u64 = 4096;

type KeyId = String;

#[doc(hidden)]
#[derive(Clone)]
pub struct AiCollector {
    stage: String,
    trigger: ReduceTrigger,
    max_open_windows: NonZeroU64,
    lineage: obzenflow_core::config::LineagePolicy,
    writer_id: Option<WriterId>,
}

impl AiCollector {
    pub fn new(
        stage: impl Into<String>,
        trigger: ReduceTrigger,
        max_open_windows: NonZeroU64,
    ) -> Self {
        Self {
            stage: stage.into(),
            trigger,
            max_open_windows,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            writer_id: None,
        }
    }
}

impl fmt::Debug for AiCollector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiCollector")
            .field("stage", &self.stage)
            .field("trigger", &self.trigger)
            .field("max_open_windows", &self.max_open_windows)
            .finish()
    }
}

#[derive(Debug, Clone)]
enum Slot {
    Partial { items: Vec<Value>, partial: Value },
    Failed(AiMapReduceRoleFailure),
}

#[derive(Debug, Clone)]
struct StoredSlot {
    canonical: Vec<u8>,
    slot: Slot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowPhase {
    Open,
    Handed,
}

#[derive(Clone)]
struct Window {
    key: Option<Value>,
    slots: BTreeMap<GroupOrdinal, StoredSlot>,
    expected: Option<u64>,
    trace: TraceState,
    retained_bytes: usize,
    phase: WindowPhase,
}

#[derive(Clone, Default)]
struct KeyProgress {
    group_count: Option<u64>,
    closed: BTreeSet<WindowOrdinal>,
}

#[doc(hidden)]
#[derive(Clone, Default)]
pub struct AiCollectorState {
    windows: BTreeMap<(KeyId, WindowOrdinal), Window>,
    keys: BTreeMap<KeyId, KeyProgress>,
    ready: Vec<(KeyId, WindowOrdinal)>,
    handed: Vec<(KeyId, WindowOrdinal)>,
    retained_bytes: usize,
    failure: Option<StageFatal>,
}

fn protocol_fatal(detail: impl Into<String>) -> StageFatal {
    StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    )
}

fn resource_fatal(detail: impl Into<String>) -> StageFatal {
    StageFatal::new(
        StageFatalCode::Resource,
        StageFatalReason::ResourceExhaustion,
        detail,
    )
}

fn key_id(key: &Option<Value>) -> KeyId {
    key.as_ref().map(Value::to_string).unwrap_or_default()
}

fn shown_key(key: &Option<Value>) -> String {
    key.as_ref()
        .map(|key| format!(" key {key}"))
        .unwrap_or_default()
}

impl AiCollector {
    fn writer_id(&self) -> Result<WriterId, StageFatal> {
        self.writer_id.ok_or_else(|| {
            protocol_fatal("ai collector ran before its writer identity was installed")
        })
    }

    fn expected_slots(&self, window: WindowOrdinal, group_count: Option<u64>) -> Option<u64> {
        let first = self.trigger.first_group(window).get();
        match (self.trigger.slots(), group_count) {
            (Some(slots), Some(count)) if count < first.saturating_add(slots.get()) => {
                Some(count.saturating_sub(first))
            }
            (Some(slots), _) => Some(slots.get()),
            (None, count) => count,
        }
    }

    fn open_window_count(state: &AiCollectorState) -> u64 {
        state
            .windows
            .values()
            .filter(|window| window.phase == WindowPhase::Open)
            .count() as u64
    }

    fn window_mut<'a>(
        &self,
        state: &'a mut AiCollectorState,
        key: &Option<Value>,
        window: WindowOrdinal,
    ) -> Result<&'a mut Window, StageFatal> {
        let id = (key_id(key), window);
        if !state.windows.contains_key(&id) {
            if Self::open_window_count(state) >= self.max_open_windows.get() {
                return Err(resource_fatal(format!(
                    "stage `{}`: opening window {window}{} would exceed the limit of {} open windows",
                    self.stage,
                    shown_key(key),
                    self.max_open_windows
                )));
            }
            let group_count = state
                .keys
                .get(&id.0)
                .and_then(|progress| progress.group_count);
            state.windows.insert(
                id.clone(),
                Window {
                    key: key.clone(),
                    slots: BTreeMap::new(),
                    expected: self.expected_slots(window, group_count),
                    trace: TraceState::default(),
                    retained_bytes: 0,
                    phase: WindowPhase::Open,
                },
            );
        }
        state
            .windows
            .get_mut(&id)
            .ok_or_else(|| protocol_fatal("ai collector lost a window it opened"))
    }

    fn maybe_close(state: &mut AiCollectorState, id: (KeyId, WindowOrdinal)) {
        let Some(window) = state.windows.get_mut(&id) else {
            return;
        };
        if window.phase == WindowPhase::Open
            && window
                .expected
                .is_some_and(|expected| window.slots.len() as u64 == expected)
        {
            window.phase = WindowPhase::Handed;
            state.ready.push(id);
        }
    }

    fn record_disposition(
        &self,
        state: &mut AiCollectorState,
        event: &ChainEvent,
        key: Option<Value>,
        group: GroupOrdinal,
        canonical: Vec<u8>,
        slot: Slot,
    ) -> Result<(), StageFatal> {
        let (window, slot_index) = self.trigger.place(group);
        if slot_index >= MAX_SLOTS_PER_WINDOW {
            return Err(resource_fatal(format!(
                "stage `{}`: group {group}{} falls beyond the limit of {MAX_SLOTS_PER_WINDOW} groups per window",
                self.stage,
                shown_key(&key)
            )));
        }
        let id = key_id(&key);
        let progress = state.keys.entry(id.clone()).or_default();
        if progress.closed.contains(&window) {
            return Err(protocol_fatal(format!(
                "stage `{}`: group {group}{} arrived after window {window} closed",
                self.stage,
                shown_key(&key)
            )));
        }
        if let Some(count) = progress.group_count {
            if group.get() >= count {
                return Err(protocol_fatal(format!(
                    "stage `{}`: group {group}{} is past the key's end of input at {count} groups",
                    self.stage,
                    shown_key(&key)
                )));
            }
        }
        if let Some(existing) = state
            .windows
            .get(&(id.clone(), window))
            .and_then(|stored| stored.slots.get(&group))
        {
            return if existing.canonical == canonical {
                Ok(())
            } else {
                Err(protocol_fatal(format!(
                    "stage `{}`: conflicting dispositions for group {group}{}",
                    self.stage,
                    shown_key(&key)
                )))
            };
        }
        let charged = canonical.len();
        let retained = state
            .retained_bytes
            .checked_add(charged)
            .filter(|retained| *retained <= MAX_RETAINED_BYTES)
            .ok_or_else(|| {
                resource_fatal(format!(
                    "stage `{}`: holding group {group}{} would retain more than {MAX_RETAINED_BYTES} bytes",
                    self.stage,
                    shown_key(&key)
                ))
            })?;
        let lineage = self.lineage;
        let stored = self.window_mut(state, &key, window)?;
        stored.trace.record_event(event, lineage);
        stored.retained_bytes += charged;
        stored.slots.insert(group, StoredSlot { canonical, slot });
        state.retained_bytes = retained;
        Self::maybe_close(state, (id, window));
        Ok(())
    }

    fn record_end(
        &self,
        state: &mut AiCollectorState,
        ended: AiMapReduceGroupingEnded,
    ) -> Result<(), StageFatal> {
        let id = key_id(&ended.key);
        let progress = state.keys.entry(id.clone()).or_default();
        match progress.group_count {
            Some(count) if count == ended.group_count => return Ok(()),
            Some(count) => {
                return Err(protocol_fatal(format!(
                    "stage `{}`: conflicting ends of input{} at {count} and {} groups",
                    self.stage,
                    shown_key(&ended.key),
                    ended.group_count
                )))
            }
            None => progress.group_count = Some(ended.group_count),
        }
        let open: Vec<_> = state
            .windows
            .iter()
            .filter(|((key, _), window)| key == &id && window.phase == WindowPhase::Open)
            .map(|((_, window), _)| *window)
            .collect();
        for window in open {
            let expected = self.expected_slots(window, Some(ended.group_count));
            if let Some(stored) = state.windows.get_mut(&(id.clone(), window)) {
                if stored
                    .slots
                    .keys()
                    .any(|group| group.get() >= ended.group_count)
                {
                    return Err(protocol_fatal(format!(
                        "stage `{}`: a group{} lies past the key's end of input at {} groups",
                        self.stage,
                        shown_key(&ended.key),
                        ended.group_count
                    )));
                }
                stored.expected = expected;
            }
            Self::maybe_close(state, (id.clone(), window));
        }
        Ok(())
    }

    fn admit(&self, state: &mut AiCollectorState, event: ChainEvent) -> Result<(), StageFatal> {
        if !matches!(event.payload, ChainPayload::CompositeData(_)) {
            return Err(protocol_fatal(
                "ai collector requires composite protocol data",
            ));
        }
        let event_type = event.envelope.provenance.event.event_type.clone();
        let canonical = canonical_json_bytes_v2(&event_type, event.payload()).map_err(|error| {
            protocol_fatal(format!("ai collector canonicalization failed: {error}"))
        })?;
        if canonical.len() > MAX_FACT_BYTES {
            return Err(resource_fatal(format!(
                "stage `{}`: a collected fact is {} bytes; the limit is {MAX_FACT_BYTES}",
                self.stage,
                canonical.len()
            )));
        }

        if AiMapReduceGroupingEnded::matches_event_type(&event_type) {
            let ended = AiMapReduceGroupingEnded::try_from_event(&event).map_err(|error| {
                protocol_fatal(format!("ai end-of-input fact decode failed: {error}"))
            })?;
            return self.record_end(state, ended);
        }
        if AiMapReduceTaggedPartial::<Value, Value>::matches_event_type(&event_type) {
            let tagged = AiMapReduceTaggedPartial::<Value, Value>::try_from_event(&event).map_err(
                |error| protocol_fatal(format!("ai tagged partial decode failed: {error}")),
            )?;
            return self.record_disposition(
                state,
                &event,
                tagged.key,
                tagged.group,
                canonical,
                Slot::Partial {
                    items: tagged.items,
                    partial: tagged.partial,
                },
            );
        }
        if AiMapReduceChunkFailed::matches_event_type(&event_type) {
            let failed = AiMapReduceChunkFailed::try_from_event(&event).map_err(|error| {
                protocol_fatal(format!("ai chunk failure decode failed: {error}"))
            })?;
            return self.record_disposition(
                state,
                &event,
                failed.key,
                failed.group,
                canonical,
                Slot::Failed(failed.cause),
            );
        }
        Err(protocol_fatal(format!(
            "ai collector received undeclared event type '{event_type}'"
        )))
    }

    fn hand_off(
        &self,
        id: &(KeyId, WindowOrdinal),
        window: &Window,
    ) -> Result<ChainEvent, StageFatal> {
        let failures: Vec<_> = window
            .slots
            .iter()
            .filter_map(|(group, stored)| match &stored.slot {
                Slot::Failed(cause) => Some(AiMapReduceGroupFailure {
                    group: *group,
                    cause: cause.clone(),
                }),
                Slot::Partial { .. } => None,
            })
            .collect();
        let fact = if failures.is_empty() {
            let mut items = Vec::new();
            let mut partials = Vec::with_capacity(window.slots.len());
            for stored in window.slots.values() {
                if let Slot::Partial {
                    items: group_items,
                    partial,
                } = &stored.slot
                {
                    items.extend(group_items.iter().cloned());
                    partials.push(partial.clone());
                }
            }
            TypedFact::from_payload(AiMapReduceReduceInput {
                key: window.key.clone(),
                window: id.1,
                trigger: self.trigger,
                items,
                partials,
            })
        } else {
            TypedFact::from_payload(AiMapReduceWindowFailed {
                key: window.key.clone(),
                window: id.1,
                failures,
            })
        }
        .map_err(|error| protocol_fatal(error.to_string()))?;
        let mut event = fact.into_event(self.writer_id()?);
        window.trace.apply_to_event(&mut event);
        Ok(event)
    }
}

#[async_trait]
impl StatefulHandler for AiCollector {
    type State = AiCollectorState;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if state.failure.is_none() {
            state.failure = self.admit(state, event).err();
        }
    }

    fn try_accumulate(
        &mut self,
        state: &mut Self::State,
        event: ChainEvent,
    ) -> Result<(), HandlerError> {
        if let Some(failure) = state.failure.as_ref() {
            return Err(HandlerError::Fatal(failure.clone()));
        }
        self.admit(state, event).map_err(HandlerError::Fatal)
    }

    fn initial_state(&self) -> Self::State {
        AiCollectorState::default()
    }

    fn create_events(&self, _: &Self::State) -> Result<Vec<ChainEvent>, StageFatal> {
        Ok(Vec::new())
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        state.failure.is_some() || (state.handed.is_empty() && !state.ready.is_empty())
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, StageFatal> {
        if let Some(failure) = state.failure.take() {
            return Err(failure);
        }
        if !state.handed.is_empty() {
            return Err(protocol_fatal(
                "ai collector attempted a second hand-off before the first committed",
            ));
        }
        let ready = std::mem::take(&mut state.ready);
        let mut events = Vec::with_capacity(ready.len());
        for id in &ready {
            let window = state
                .windows
                .get(id)
                .ok_or_else(|| protocol_fatal("ai collector lost a ready window"))?;
            events.push(self.hand_off(id, window)?);
        }
        state.handed = ready;
        Ok(events)
    }

    fn outputs_committed(&self, state: &mut Self::State) {
        for id in std::mem::take(&mut state.handed) {
            if let Some(window) = state.windows.remove(&id) {
                state.retained_bytes = state.retained_bytes.saturating_sub(window.retained_bytes);
            }
            state.keys.entry(id.0).or_default().closed.insert(id.1);
        }
    }

    fn validate_terminal(
        &self,
        state: &Self::State,
        kind: StatefulTerminationKind,
    ) -> TerminalValidation {
        if kind != StatefulTerminationKind::NaturalEof {
            return TerminalValidation::Clean;
        }
        let open: Vec<_> = state
            .windows
            .iter()
            .filter(|(_, window)| window.phase == WindowPhase::Open)
            .map(|((_, ordinal), window)| format!("window {ordinal}{}", shown_key(&window.key)))
            .collect();
        if open.is_empty() {
            return TerminalValidation::Clean;
        }
        TerminalValidation::Primary(StageFatal::new(
            StageFatalCode::Termination,
            StageFatalReason::IncompleteTermination,
            format!(
                "stage `{}` reached a natural end of input with {} open window(s): {}",
                self.stage,
                open.len(),
                open.join(", ")
            ),
        ))
    }

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, StageFatal> {
        if let Some(failure) = state.failure.as_ref() {
            return Err(failure.clone());
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::StageId;
    use serde_json::json;

    fn collector(trigger: ReduceTrigger) -> AiCollector {
        let mut collector = AiCollector::new(
            "digest",
            trigger,
            NonZeroU64::new(8).expect("a nonzero window limit"),
        );
        collector.install_writer_id(WriterId::from(StageId::new()));
        collector
    }

    fn every(chunks: u64) -> ReduceTrigger {
        ReduceTrigger::EveryNChunks {
            chunks: NonZeroU64::new(chunks).expect("a nonzero trigger"),
        }
    }

    fn fact<P: TypedPayload>(payload: P) -> ChainEvent {
        TypedFact::from_payload(payload)
            .expect("the protocol fact encodes")
            .into_event(WriterId::from(StageId::new()))
    }

    fn partial(group: u64, value: u64) -> ChainEvent {
        fact(AiMapReduceTaggedPartial::<Value, Value> {
            key: None,
            group: GroupOrdinal::new(group),
            items: vec![json!(value)],
            partial: json!(value * 10),
        })
    }

    fn ended(group_count: u64) -> ChainEvent {
        fact(AiMapReduceGroupingEnded {
            key: None,
            group_count,
        })
    }

    fn admit(collector: &mut AiCollector, state: &mut AiCollectorState, events: Vec<ChainEvent>) {
        for event in events {
            collector
                .try_accumulate(state, event)
                .expect("the collector admits the fact");
        }
    }

    fn handed(
        collector: &AiCollector,
        state: &mut AiCollectorState,
    ) -> Vec<(u64, Vec<Value>, Vec<Value>)> {
        if !collector.should_emit(state) {
            return Vec::new();
        }
        let events = collector.emit(state).expect("the ready windows hand off");
        collector.outputs_committed(state);
        events
            .iter()
            .map(|event| {
                let input = AiMapReduceReduceInput::<Value, Value>::try_from_event(event)
                    .expect("a reduce input");
                (input.window.get(), input.items, input.partials)
            })
            .collect()
    }

    #[test]
    fn a_window_closes_when_its_slots_fill_and_hands_partials_in_group_order() {
        let mut collector = collector(every(2));
        let mut state = collector.initial_state();
        admit(&mut collector, &mut state, vec![partial(1, 2)]);
        assert!(handed(&collector, &mut state).is_empty());

        admit(&mut collector, &mut state, vec![partial(0, 1)]);
        assert_eq!(
            handed(&collector, &mut state),
            [(0, vec![json!(1), json!(2)], vec![json!(10), json!(20)])]
        );
    }

    #[test]
    fn the_end_of_input_fact_closes_an_on_eof_window() {
        let mut collector = collector(ReduceTrigger::OnEof);
        let mut state = collector.initial_state();
        admit(
            &mut collector,
            &mut state,
            vec![partial(0, 1), partial(2, 3), partial(1, 2)],
        );
        assert!(handed(&collector, &mut state).is_empty());

        admit(&mut collector, &mut state, vec![ended(3)]);
        assert_eq!(
            handed(&collector, &mut state),
            [(
                0,
                vec![json!(1), json!(2), json!(3)],
                vec![json!(10), json!(20), json!(30)]
            )]
        );
    }

    #[test]
    fn the_end_of_input_fact_closes_the_remainder_window() {
        let mut collector = collector(every(2));
        let mut state = collector.initial_state();
        admit(
            &mut collector,
            &mut state,
            vec![partial(0, 1), partial(1, 2), partial(2, 3)],
        );
        assert_eq!(handed(&collector, &mut state).len(), 1);

        admit(&mut collector, &mut state, vec![ended(3)]);
        assert_eq!(
            handed(&collector, &mut state),
            [(1, vec![json!(3)], vec![json!(30)])]
        );
    }

    #[test]
    fn a_repeated_disposition_is_ignored() {
        let mut collector = collector(every(2));
        let mut state = collector.initial_state();
        admit(
            &mut collector,
            &mut state,
            vec![partial(0, 1), partial(0, 1), partial(1, 2)],
        );
        assert_eq!(
            handed(&collector, &mut state),
            [(0, vec![json!(1), json!(2)], vec![json!(10), json!(20)])]
        );
    }

    #[test]
    fn conflicting_dispositions_for_one_group_fail_the_stage() {
        let mut collector = collector(every(2));
        let mut state = collector.initial_state();
        admit(&mut collector, &mut state, vec![partial(0, 1)]);
        let Err(HandlerError::Fatal(fatal)) = collector.try_accumulate(&mut state, partial(0, 5))
        else {
            panic!("a second disposition for group 0 is a protocol failure");
        };
        assert_eq!(fatal.code, StageFatalCode::Protocol);
        assert_eq!(
            fatal.detail,
            "stage `digest`: conflicting dispositions for group 0"
        );
    }

    #[test]
    fn only_a_natural_end_of_input_requires_every_window_closed() {
        let mut collector = collector(ReduceTrigger::OnEof);
        let mut state = collector.initial_state();
        admit(&mut collector, &mut state, vec![partial(0, 1)]);

        for kind in [
            StatefulTerminationKind::TruncatedEof,
            StatefulTerminationKind::PoisonedEof,
        ] {
            assert_eq!(
                collector.validate_terminal(&state, kind),
                TerminalValidation::Clean
            );
        }
        let TerminalValidation::Primary(fatal) =
            collector.validate_terminal(&state, StatefulTerminationKind::NaturalEof)
        else {
            panic!("an open window at a natural end of input fails the stage");
        };
        assert_eq!(
            fatal.detail,
            "stage `digest` reached a natural end of input with 1 open window(s): window 0"
        );
    }
}
